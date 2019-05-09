from __future__ import annotations

import sys
import argparse
import sqlite3
import pathlib
import glob
import atexit
import hashlib
import json

from typing import Any

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
parser.add_argument("--compact", action="store_true", help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while, use sparingly)")
parser.add_argument("files", nargs="*", help="Files to be archived in the SQLite Database.")

args: argparse.Namespace = parser.parse_args()

if not args.table and not args.compact:
    if args.files and not args.extract:
        argpath = pathlib.Path(args.files[0])
        f = None
        if argpath.is_file():
            f = argpath.stem.name
        elif argpath.is_dir():
            f = argpath.name
        if f:
            args.table = f.replace(".", "_").replace(' ', '_').replace("'", '_').replace(",", "")
        else:
            raise RuntimeError("--table must be specified if compact mode is not active.")
    elif args.files:
        raise RuntimeError("--table must be specified if compact mode is not active.")
    elif args.extract:
        raise RuntimeError("--table must be specified in extract mode.")

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

class SQLiteArchive:
    def __init__(self):
        self.db: pathlib.Path = pathlib.Path(args.db).resolve()
        self.files: list = []
        
        if self.db.is_file():
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
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
        
        listglob: list = globlist(args.files)
        for i in listglob:
            if pathlib.Path(i).is_file():
                self.files.append(i)
        if len(self.files) == 0 and not args.extract:
            raise RuntimeError("No files were found.")
    
    def add(self):
        self.dbcon.execute("""CREATE TABLE IF NOT EXISTS {} ( "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE );""".format(args.table))
        self.dbcon.commit()
        dups: dict = {}
        if pathlib.Path(args.dups).is_file() and not args.nodups:
            with open(args.dups) as dupsjson:
                dups = json.load(dupsjson)

        for i in self.files:
            filehash = hashlib.sha256()
            fullpath: pathlib.Path = i.resolve()
            name: str = str(fullpath.relative_to(fullpath.parent))
            relparent: str = str(fullpath.relative_to(fullpath.parent.parent))
            try:                
                if i.is_file():
                    exists = len(self.dbcon.execute("select filename from {} where filename = ?".format(args.table), (name,)).fetchall())
                    data: bytes = i.read_bytes()
                    filehash.update(data)
                    digest: str = filehash.hexdigest()
                    if args.replace and exists > 0:
                        print("* Replacing {}'s data in {} with specified file...".format(name, args.table), end=' ')
                        self.dbcon.execute("insert or replace into {} (filename, data, hash) values (?, ?, ?)".format(args.table), (name, data, digest))
                    else:
                        print("* Adding {} to {}...".format(name, args.table), end=' ')
                        self.dbcon.execute("insert into {} (filename, data, hash) values (?, ?, ?)".format(args.table), (name, data, digest))
            except sqlite3.IntegrityError:
                if args.debug:
                    raise
                print("duplicate")
                
                query = self.dbcon.execute("select filename from {} where hash == ?".format(args.table), (digest,)).fetchall()
                if args.fulldups and type(query) is list and len(query) >= 1 or str(pathlib.Path.cwd()) not in str(fullpath) and type(query) is list and len(query) >= 1:
                    if query[0] is not None:
                        dups[str(fullpath)] = query[0]
                elif not args.fulldups and type(query) is list and len(query) >= 1:
                    if query[0] is not None:
                        dups[relparent] = query[0]

                for z in list(dups.keys()):
                    if query[0][0] in z:
                        try:
                            dups.pop(z)
                        except KeyError:
                            pass
                
                #if args.debug:
                #    exctype, value = sys.exc_info()[:2]
                #    print("Exception type = {}, value = {}".format(exctype, value))
                if not args.debug:
                    continue
            else:
                self.dbcon.commit()
                print("done")
            
        if len(dups) > 0:
            if not args.hidedups:
                print("Duplicate files:\n {}".format(json.dumps(dups, indent=4)))
            if not args.nodups:
                with open(args.dups, 'w') as dupsjson:
                    json.dump(dups, dupsjson, indent=4)
    
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
                cursor = self.dbcon.execute(query_files, tuple(self.files))
            else:
                cursor = self.dbcon.execute(query)
        except sqlite3.OperationalError:
            if args.files and len(self.files) > 0:
                cursor = self.dbcon.execute(query_files2, tuple(self.files))
            else:
                cursor = self.dbcon.execute(query2)

        row: Any = cursor.fetchone()
        while row:
            try:
                data: bytes = bytes(row[1])
                name: Any = self.dbcon.execute("select filename from {} where rowid == {}".format(args.table, str(row[0]))).fetchone()[0]
                name = name.decode(sys.stdout.encoding) if sys.stdout.encoding else name.decode("utf-8")

                outpath: pathlib.Path = outputdir.joinpath(name)
                if not pathlib.Path(outpath.parent).exists():
                    pathlib.Path(outpath.parent).mkdir(parents=True)
                
                print("Extracting {}...".format(str(outpath)), end =' ')
                outpath.write_bytes(data)
                print("done")
            except sqlite3.OperationalError:
                print("failed")
                
                # if args.debug:
                #     exctype, value = sys.exc_info()[:2]
                #     print("Exception type = {}, value = {}".format(exctype, value))
                                
                if args.debug:
                    raise
                else:
                    row = cursor.fetchone()
                    continue
        
            row = cursor.fetchone()  # Normal end of loop
    def compact(self):
        print("Compacting the database, this might take a while...", end = ' ')
        try:
            self.dbcon.execute("VACUUM;")
        except sqlite3.OperationalError:
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