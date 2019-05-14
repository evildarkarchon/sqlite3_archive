from __future__ import annotations

import sys
import argparse
import sqlite3
import pathlib
import glob
import atexit
import hashlib
import json

from typing import Any, List, Tuple, Dict
from time import wait

parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Imports or Exports files from an sqlite3 database.")
parser.add_argument("--db", "-d", dest="db", type=str, required=True, help="SQLite DB filename.")
parser.add_argument("--table", "-t", dest="table", type=str, help="Name of table to use.")
parser.add_argument("--extract", "-x", dest="extract", action="store_true", help="Extract files from a table instead of adding them.")
parser.add_argument("--output-dir", "-o", dest="out", type=str, help="Directory to output files to, if in extraction mode (defaults to current directory).", default=str(pathlib.Path.cwd()))
parser.add_argument("--replace", "-r", action="store_true", help="Replace any existing file entry's data instead of skipping.")
parser.add_argument("--debug", dest="debug", action="store_true", help="Supress any exception skipping and some debug info.")
parser.add_argument("--dups-file", type=str, dest="dups", help="Location of the file to store the list of duplicate files to. Defaults to duplicates.json in current directory.", default="{}/duplicates.json".format(pathlib.Path.cwd()))
parser.add_argument("--no-dups", action="store_true", dest="nodups", help="Disables saving the duplicate list as a json file or reading an existing one from an existing file.")
parser.add_argument("--hide-dups", dest="hidedups", action="store_true", help="Hides the list of duplicate files.")
parser.add_argument("--full-dup-path", dest="fulldups", action="store_true", help="Use the full path of the duplicate file as the key for the duplicates list.")
parser.add_argument("--dups-current-db", dest="dupscurrent", action="store_true", help="Only show the duplicates from the current database.")
parser.add_argument("--compact", action="store_true", help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while, use sparingly)")
parser.add_argument("files", nargs="*", help="Files to be archived in the SQLite Database.")

args: argparse.Namespace = parser.parse_args()

def cleantablename(instring: str):
    return instring.replace(".", "_").replace(' ', '_').replace("'", '_').replace(",", "").replace("/", '_').replace('\\', '_')

def infertableadd():
    base = pathlib.Path(args.files[0])
    if not base.exists():
        return None
    f: str = None
    if base.is_file():
       f = cleantablename(base.stem.name)
    elif base.is_dir():
        f = cleantablename(base.name)
        
    if f:
        return f
    else:
        return None
def infertableextract():
    if args[0]:
            f = cleantablename(args.files[0])
            args.files.pop(0)
            return f
    else:
        return None
def calculatehash(file: bytes):
    filehash = hashlib.sha512()
    filehash.update(file)
    return filehash.hexdigest()

if not args.table and not args.compact:
    if args.files and not args.extract:
        args.table = infertableadd()
        if not args.table:
            raise RuntimeError("File or Directory specified not found and --table was not specified.")
    elif args.files and args.extract:
        args.table = infertableextract()
        if not args.table:
            raise RuntimeError("File or Directory specified not found and --table was not specified.")
    elif not args.files:
        raise RuntimeError("--table must be specified if compact mode is not active.")

def globlist(listglob: list):
    outlist: list = []
    for a in listglob:
        if type(a) is str and "*" in a:
            globs: list = glob.glob(a, recursive=True)
            for x in globs:
                outlist.append(pathlib.Path(x))
        elif type(a) is pathlib.Path and a.is_file() or type(a) is str and pathlib.Path(a).is_file():
            outlist.append(a)
        elif type(a) is str and "*" not in listglob and pathlib.Path(a).is_file():
            outlist.append(pathlib.Path(a))
        elif type(a) is str and "*" not in listglob and pathlib.Path(a).is_dir() or type(a) is pathlib.Path and a.is_dir():
            for y in pathlib.Path(a).rglob("*"):
                outlist.append(y)
        else:
            globs: list = glob.glob(a, recursive=True)
            for x in globs:
                outlist.append(pathlib.Path(x))
    outlist.sort()
    return outlist

def duplist(dups: dict, dbname: str):
    if len(dups) > 0:
        if len(dups[dbname]) is 0:
            dups.pop(dbname)
        keylist = list(dups.keys())
        dupsexist = False
        for i in keylist:
            if len(dups[i]) >= 1:
                dupsexist = True
        if not args.hidedups and dupsexist:
            if args.dupscurrent:
                print("Duplicate Files:\n {}".format(json.dumps(dups[dbname], indent=4)))
            else:
                print("Duplicate files:\n {}".format(json.dumps(dups, indent=4)))
        if not args.nodups and dupsexist:
            with open(args.dups, 'w') as dupsjson:
                json.dump(dups, dupsjson, indent=4)

class SQLiteArchive:
    def __init__(self):
        self.db: pathlib.Path = pathlib.Path(args.db).resolve()
        self.files: list = []
        
        if self.db.is_file():
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            if not (self.dbcon.execute("PRAGMA auto_vacuum;").fetchone()[0]) == 1:
                self.dbcon.execute("PRAGMA auto_vacuum = 1;")
                self.dbcon.execute("VACUUM;")
        else:
            self.db.touch()
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            self.dbcon.execute("PRAGMA auto_vacuum = 1;")
            self.dbcon.execute("VACUUM;")
        
        if args.extract:
            self.dbcon.text_factory = bytes
        atexit.register(self.dbcon.close)
        if not args.compact:
            atexit.register(self.dbcon.execute, "PRAGMA optimize;")
        
        if not args.compact or len(args.files) > 0:
            listglob: list = globlist(args.files)
            for i in listglob:
                if pathlib.Path(i).is_file():
                    self.files.append(i)
        if len(self.files) == 0 and not args.extract and not args.compact:
            raise RuntimeError("No files were found.")

    def execquerynocommit(self, query: str, values: Union[tuple, list] = None, one: bool = False, raw: bool = False):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")
        output: Any = None
        if "select" in query or "SELECT" in query or "Select" in query:
            if values:
                output = self.dbcon.execute(query, values)
            else:
                output = self.dbcon.execute(query)
            
            if one:
                return output.fetchone()[0]
            elif raw:
                return output
            else:
                return output.fetchall()
        else:
            if values:
                self.dbcon.execute(query, values)
            else:
                self.dbcon.execute(query)
            return None
    def execquerycommit(self, query: str, values: Union[tuple, list] = None):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")
        if values:
            try:
                self.dbcon.execute(query, values)
            except Exception:
                raise
            else:
                self.dbcon.commit()
        else:
            try:
                self.dbcon.execute(query)
            except Exception:
                raise
            else:
                self.dbcon.commit()
    
    def add(self):
        self.execquerycommit("""CREATE TABLE IF NOT EXISTS {} ( "pk" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE );""".format(args.table))
        dups: dict = {}
        if pathlib.Path(args.dups).is_file() and not args.nodups:
            with open(args.dups) as dupsjson:
                dups = json.load(dupsjson)
        
        try:
            dbname: str = str(self.db.relative_to(pathlib.Path(args.dups).resolve().parent))
        except ValueError:
            dbname: str = str(self.db.relative_to(self.db.parent))
        if dbname not in list(dups.keys()):
            dups[dbname] = {}
        for i in self.files:
            fullpath: pathlib.Path = i.resolve()
            name: str = str(fullpath.relative_to(fullpath.parent))
            relparent: str = str(fullpath.relative_to(fullpath.parent.parent))
            try:                
                if i.is_file():
                    exists: int = None
                    if args.replace:
                        exists = int(self.execquerynocommit("select count(distinct filename) from {} where filename = ?".format(args.table), values = (name,), one = True))
                    if args.debug:
                        print(exists)
                    data: bytes = i.read_bytes()
                    digest: str = calculatehash(data)
                    if args.replace and exists and exists > 0:
                        print("* Replacing {}'s data in {} with specified file...".format(name, args.table), end=' ')
                        self.execquerycommit("replace into {} (filename, data, hash) values (?, ?, ?)".format(args.table), (name, data, digest))
                    else:
                        print("* Adding {} to {}...".format(name, args.table), end=' ')
                        self.execquerycommit("insert into {} (filename, data, hash) values (?, ?, ?)".format(args.table), (name, data, digest))
            except sqlite3.IntegrityError:
                if args.debug:
                    raise

                query = self.execquerynocommit("select filename from {} where hash == ?".format(args.table), (digest,))
                if query and query[0][0] and len(query[0][0]) >= 1:
                    print("duplicate")
                else:
                    raise
                
                if args.fulldups and type(query) is list and len(query) >= 1 or str(pathlib.Path.cwd()) not in str(fullpath) and type(query) is list and len(query) >= 1:
                    if query[0] is not None:
                        dups[dbname][str(fullpath)] = query[0]
                elif not args.fulldups and type(query) is list and len(query) >= 1:
                    if query[0] is not None:
                        dups[dbname][relparent] = query[0]

                for z in list(dups[dbname].keys()):
                    if query[0][0] in z:
                        try:
                            dups[dbname].pop(z)
                        except KeyError:
                            pass
                if not args.debug:
                    continue
            else:
                print("done")

        duplist(dups, dbname)
    
    def extract(self):
        outputdir: pathlib.Path = pathlib.Path(args.out).resolve()
        if outputdir.is_file():
            raise RuntimeError("The output directory specified points to a file.")
        
        if not outputdir.exists():
            print("Creating output directory...")
            outputdir.mkdir(parents=True)
        if args.debug:
            print(len(self.files))
            print(repr(tuple(self.files)))
        if args.files and len(args.files) > 0:
            if len(self.files) > 1:
                questionmarks: Any = '?' * len(args.files)
                query_files: str = "select rowid, data from {0} where filename in ({1}) order by filename asc".format(args.table, ','.join(questionmarks))
                query_files2: str = "select rowid, image_data from {0} where filename in ({1}) order by filename asc (".format(args.table, ','.join(questionmarks))
            elif args.files and len(args.files) == 1:
                query_files: str = "select rowid, data from {} where filename == ? order by filename asc".format(args.table)
                query_files2: str = "select rowid, image_data from {} where filename == ? order by filename asc".format(args.table)     
        else:
            query: str = "select rowid, data from {} order by filename asc".format(args.table)
            query2: str = "select rowid, image_data from {} order by filename asc".format(args.table)
        if args.debug:
            print(query_files)
            print(query_files2)
        cursor: sqlite3.Cursor = None
        
        try:
            if args.files and len(self.files) > 0:
                cursor = self.execquerynocommit(query_files, tuple(self.files), raw = True)
            else:
                cursor = self.execquerynocommit(query, raw = True)
        except sqlite3.OperationalError:
            if args.files and len(self.files) > 0:
                cursor = self.execquerynocommit(query_files2, tuple(self.files), raw = True)
            else:
                cursor = self.execquerynocommit(query2, raw = True)

        row: Any = cursor.fetchone()
        while row:
            try:
                data: bytes = bytes(row[1])
                name: Any = self.execquerynocommit("select filename from {} where rowid == ?".format(args.table), values = (str(row[0]),), one = True)
                name = name.decode(sys.stdout.encoding) if sys.stdout.encoding else name.decode("utf-8")

                outpath: pathlib.Path = outputdir.joinpath(name)
                if not pathlib.Path(outpath.parent).exists():
                    pathlib.Path(outpath.parent).mkdir(parents=True)
                
                print("Extracting {}...".format(str(outpath)), end =' ')
                outpath.write_bytes(data)
                print("done")
            except sqlite3.DatabaseError:
                print("failed")
                                
                if args.debug:
                    raise
                else:
                    row = cursor.fetchone()
                    continue
        
            row = cursor.fetchone()  # Normal end of loop
    def compact(self):
        try:
            print("Compacting the database, this might take a while...", end = ' ')
            wait(1)
            self.dbcon.execute("VACUUM;")
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")

sqlitearchive: SQLiteArchive = SQLiteArchive()

if args.compact:
    sqlitearchive.compact()
elif args.extract:
    sqlitearchive.extract()
else:
    sqlitearchive.add()