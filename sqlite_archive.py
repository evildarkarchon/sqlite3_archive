#!/usr/bin/env python3
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
from collections import OrderedDict

parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Imports or Exports files from an sqlite3 database.")
parser.add_argument("--db", "-d", dest="db", type=str, required=True, help="SQLite DB filename.")
parser.add_argument("--table", "-t", dest="table", type=str, help="Name of table to use.")
parser.add_argument("--drop-table", action="store_true", dest="drop", help="Drop the table specified in the --table argument. NOTE: this will run VACUUM when done, by default.")
parser.add_argument("--no-drop-vacuum", action="store_false", dest="drop_vacuum", help="Do not execute VACUUM when dropping a table")
parser.add_argument("--extract", "-x", dest="extract", action="store_true", help="Extract files from a table instead of adding them.")
parser.add_argument("--output-dir", "-o", dest="out", type=str, help="Directory to output files to, if in extraction mode (defaults to current directory).", default=str(pathlib.Path.cwd()))
parser.add_argument("--replace", "-r", action="store_true", help="Replace any existing file entry's data instead of skipping. By default, the VACUUM command will be run to prevent database fragmentation.")
parser.add_argument("--no-replace-vacuum", action="store_false", dest="replace_vacuum", help="Do not run the VACUUM command after replacing data.")
parser.add_argument("--debug", dest="debug", action="store_true", help="Supress any exception skipping and some debug info.")
parser.add_argument("--dups-file", type=str, dest="dups_file", help="Location of the file to store the list of duplicate files to. Defaults to duplicates.json in current directory.", default="{}/duplicates.json".format(pathlib.Path.cwd()))
parser.add_argument("--no-dups", action="store_false", dest="dups", help="Disables saving the duplicate list as a json file or reading an existing one from an existing file.")
parser.add_argument("--hide-dups", dest="hidedups", action="store_true", help="Hides the list of duplicate files.")
parser.add_argument("--dups-current-db", dest="dupscurrent", action="store_true", help="Only show the duplicates from the current database.")
parser.add_argument("--compact", action="store_true", help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while)")
parser.add_argument("--no-lowercase-table", action="store_false", dest="lower", help="Don't modify the inferred table name to be lowercase (doesn't do anything if --table is specified)")
parser.add_argument("--update-schema", "--update", "-u", dest="update", action="store_true", help="Run the schema creation queries and exit")
parser.add_argument("--no-atomic", action="store_false", dest="atomic", help="Run commit on every insert instead of at the end of the loop.")
parser.add_argument("--verbose", "-v", action="store_true", help="Print some more information without changing the exception raising policy.")
parser.add_argument("files", nargs="*", help="Files to be archived in the SQLite Database.")

args: argparse.Namespace = parser.parse_args()


def cleantablename(instring: str):
    out = instring.replace(".", "_").replace(' ', '_').replace("'", '_').replace(",", "").replace("/", '_').replace('\\', '_')
    if args.lower:
        return out.lower()
    else:
        return out


def infertableadd():
    base: pathlib.Path = pathlib.Path(args.files[0]).resolve()
    if not base.exists():
        return None
    f: str = None
    if base.is_file():
        f = cleantablename(base.parent.name)
    elif base.is_dir():
        f = cleantablename(base.name)

    if f:
        return f
    else:
        return None


def infertableextract():
    if args.files[0]:
        f = cleantablename(args.files[0])
        args.files.pop(0)
        return f
    elif args.out:
        f = cleantablename(pathlib.Path(args.out).name)
        return f
    else:
        return None


def calculatehash(file: bytes):
    filehash = hashlib.sha512()
    filehash.update(file)
    return filehash.hexdigest()


if not args.table and not args.compact and not args.drop:
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

    for i in listglob:
        if pathlib.Path(i).resolve() == pathlib.Path(args.db).resolve():
            if args.verbose or args.debug:
                print("Removing database file from file list.")
            try:
                listglob.remove(i)
            except ValueError:
                if args.debug:
                    raise
                else:
                    break
    
    def runglobs():
        return list(map(pathlib.Path, glob.glob(a, recursive=True)))
        
    for a in listglob:
        if type(a) is str and "*" in a:
            outlist.extend(runglobs())
        elif type(a) is pathlib.Path and a.is_file() or type(a) is str and pathlib.Path(a).is_file():
            if type(a) is str:
                outlist.append(pathlib.Path(a))
            elif type(a) is pathlib.Path:
                outlist.append(a)
            else:
                continue
        elif type(a) is str and "*" not in a and pathlib.Path(a).is_file():
            outlist.append(pathlib.Path(a))
        elif type(a) is str and "*" not in a and pathlib.Path(a).is_dir() or type(a) is pathlib.Path and a.is_dir():
            outlist.extend([x for x in pathlib.Path(a).rglob("*")])
        else:
            outlist.extend(runglobs())
    
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
            if args.dupscurrent and dbname in keylist:
                print("Duplicate Files:\n {}".format(json.dumps(dups[dbname], indent=4)))
            elif not args.dupscurrent:
                print("Duplicate files:\n {}".format(json.dumps(dups, indent=4)))
        if args.dups_file and dupsexist:
            with open(args.dups_file, 'w') as dupsjson:
                json.dump(dups, dupsjson, indent=4)


class SQLiteArchive:
    def __init__(self):
        self.db: pathlib.Path = pathlib.Path(args.db).resolve()
        self.files: list = []

        if self.db.is_file():
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            if not (self.dbcon.execute("PRAGMA auto_vacuum;").fetchone()[0]) == 1 and not args.extract and not args.compact:
                self.dbcon.execute("PRAGMA auto_vacuum = 1;")
                self.dbcon.execute("VACUUM;")
        elif not self.db.is_file() and not args.extract and not args.compact:
            self.db.touch()
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            self.dbcon.execute("PRAGMA auto_vacuum = 1;")
            self.dbcon.execute("VACUUM;")
        else:
            raise RuntimeError("Extract mode and Compact mode require an existing database.")

        if args.extract:
            self.dbcon.text_factory = bytes
        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

        if not args.compact or len(args.files) > 0:
            listglob: list = globlist(args.files)
            if args.debug or args.verbose:
                print(listglob, end="\n\n")
            self.files = [i for i in listglob if i.is_file()]
            if args.debug or args.verbose:
                print(self.files)
        if len(self.files) == 0 and not args.extract and not args.compact and not args.update and not args.drop:
            raise RuntimeError("No files were found.")

    def execquerynocommit(self, query: str, values: Union[tuple, list] = None, one: bool = False, raw: bool = False, returndata = False):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")
        output: Any = None
        returnlist = ("select", "SELECT", "Select")
        if (any(i in query for i in returnlist) and returndata is False) or returndata is True:
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
        if values and type(values) == list or type(values) == tuple:
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
    def execmanycommit(self, query: str, values: Union[tuple, list]):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")

        try:
            self.dbcon.executemany(query, values)
        except Exception:
            raise
        else:
            self.dbcon.commit()
    
    def drop(self):
        print("* Deleting table {}...".format(args.table), end=' ', flush=True)
        try:
            self.execquerycommit("DROP TABLE {}".format(args.table))
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")
            if args.drop_vacuum:
                self.compact()
    
    def rename(self, name1: str, name2: str):
            print("* Renaming {0} to {1}...".format(name1, name2), end=' ', flush=True)
            try:
                self.execquerycommit("update {} set filename = ? where filename = ?".format(args.table), (name1, name2))
            except sqlite3.DatabaseError:
                print("failed")
                raise
            else:
                print("done")

    def schema(self):
        createtable = 'CREATE TABLE IF NOT EXISTS {} ( "pk" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE );'.format(args.table)
        self.execquerycommit(createtable)
        createindex = 'CREATE UNIQUE INDEX IF NOT EXISTS {0}_index ON {0} ( "filename", "hash" );'.format(args.table)
        self.execquerycommit(createindex)
    
    def calcname(self, inpath: pathlib.Path):
        parents = sorted(inpath.parents)
        if args.verbose or args.debug:
            print(parents)
        def oldbehavior():
            if args.verbose or args.debug:
                print("Using old name calculation behavior")
            if len(parents) > 2:
                return str(inpath.relative_to(inpath.parent.parent))
            else:
                return str(inpath.relative_to(inpath.parent))
        try:
            if inpath.is_absolute() and str(pathlib.Path.cwd()) in str(inpath):
                return str(inpath.resolve().relative_to(pathlib.Path.cwd()))
            elif not inpath.is_absolute():
                return str(inpath.relative_to(parents[1]))
            else:
                return str(inpath.relative_to(self.db.parent))
        except (ValueError, IndexError):
            return oldbehavior()
    def add(self):
        def insert():
            print("* Adding {} to {}...".format(name, args.table), end=' ', flush=True)
            query="insert into {} (filename, data, hash) values (?, ?, ?)".format(args.table)
            values=(name, data, digest)
            if args.atomic:
                self.execquerynocommit(query, values)
            else:
                self.execquerycommit(query, values)
        def replace():
            print("* Replacing {}'s data in {} with specified file...".format(name, args.table), end=' ', flush=True)
            query="replace into {} (filename, data, hash) values (?, ?, ?)".format(args.table)
            values=(name, data, digest)
            if args.atomic:
                self.execquerynocommit(query, values)
            else:
                self.execquerycommit(query, values)
        
        self.schema()
        dups: dict = {}
        if pathlib.Path(args.dups_file).is_file() and args.dups:
            with open(args.dups_file) as dupsjson:
                dups = json.load(dupsjson)
        replaced: int = 0
        
        def calcdbname():
            try:
                return str(self.db.relative_to(pathlib.Path(args.dups_file).resolve().parent))
            except ValueError:
                return str(self.db.relative_to(self.db.parent))
        
        dbname: str = calcdbname()
        if dbname not in list(dups.keys()):
            dups[dbname] = {}
        
        for i in self.files:
            if not type(i) == pathlib.Path:
                i = pathlib.Path(i)
            fullpath: pathlib.Path = i.resolve()

            name: str = self.calcname(i)
            try:
                if i.is_file():
                    exists: int = None
                    if args.replace:
                        exists = int(self.execquerynocommit("select count(distinct filename) from {} where filename = ?".format(args.table), values=(name,), one=True))
                        if args.debug or args.verbose:
                            print(exists)
                    data: bytes = bytes(i.read_bytes())
                    digest: str = calculatehash(data)
                    if args.replace and exists and exists > 0:
                        replace()
                        replaced += 1
                    else:
                        insert()
            except sqlite3.IntegrityError:

                query = self.execquerynocommit("select filename from {} where hash == ?".format(args.table), (digest,))
                if query and query[0][0] and len(query[0][0]) >= 1:
                    print("duplicate")

                if type(query) is list and len(query) >= 1:
                    if query[0] is not None:
                        dups[dbname][str(fullpath)] = query[0]

                for z in list(dups[dbname].keys()):
                    if query[0][0] in z:
                        try:
                            dups[dbname].pop(z)
                        except KeyError:
                            pass
                
                if args.debug:
                    raise
                else:
                    continue
            except sqlite3.InterfaceError:
                if i.stat().st_size > 1000000000:
                    print("too big, skipping.")
                else:
                    print("failed")
                if args.debug:
                    raise
                else:
                    continue
            else:
                print("done")
        if args.atomic:
            if args.verbose or args.debug:
                print("Running commit function.")
            self.dbcon.commit()

        if args.replace and args.replace_vacuum and replaced > 0:
            self.compact()
        if args.dups:
            duplist(dups, dbname)

    def extract(self):
        def calcextractquery():
            out: list = []
            if args.files and len(args.files) > 0:
                if len(self.files) > 1:
                    questionmarks: Any = '?' * len(args.files)
                    out.insert(0, "select rowid, data from {0} where filename in ({1}) order by filename asc".format(args.table, ','.join(questionmarks)))
                    out.insert(1, "select rowid, image_data from {0} where filename in ({1}) order by filename asc (".format(args.table, ','.join(questionmarks)))
                elif args.files and len(args.files) == 1:
                    out.insert(0, "select rowid, data from {} where filename == ? order by filename asc".format(args.table))
                    out.insert(1, "select rowid, image_data from {} where filename == ? order by filename asc".format(args.table))
            else:
                out.insert(0, "select rowid, data from {} order by filename asc".format(args.table))
                out.insert(1, "select rowid, image_data from {} order by filename asc".format(args.table))
        
            return out
        
        outputdir: pathlib.Path = pathlib.Path(args.out).resolve()
        if outputdir.is_file():
            raise RuntimeError("The output directory specified points to a file.")

        if not outputdir.exists():
            print("Creating output directory...")
            outputdir.mkdir(parents=True)
        if args.debug or args.verbose:
            print(len(self.files))
            print(repr(tuple(self.files)))
        query: list = calcextractquery()
        if args.debug or args.verbose:
            print(query[0])
            print(query[1])
        cursor: sqlite3.Cursor = None

        try:
            if args.files and len(self.files) > 0:
                cursor = self.execquerynocommit(query[0], tuple(self.files), raw=True)
            else:
                cursor = self.execquerynocommit(query[0], raw=True)
        except sqlite3.OperationalError:
            if args.files and len(self.files) > 0:
                cursor = self.execquerynocommit(query[1], tuple(self.files), raw=True)
            else:
                cursor = self.execquerynocommit(query[1], raw=True)

        row: Any = cursor.fetchone()
        while row:
            try:
                data: bytes = bytes(row[1])
                name: Any = self.execquerynocommit("select filename from {} where rowid == ?".format(args.table), values=(str(row[0]),), one=True)
                name = name.decode(sys.stdout.encoding) if sys.stdout.encoding else name.decode("utf-8")

                outpath: pathlib.Path = outputdir.joinpath(name)
                if not pathlib.Path(outpath.parent).exists():
                    pathlib.Path(outpath.parent).mkdir(parents=True)

                print("Extracting {}...".format(str(outpath)), end=' ', flush=True)
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
        print("Compacting the database, this might take a while...", end=' ', flush=True)

        try:
            self.dbcon.execute("VACUUM;")
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")


sqlitearchive: SQLiteArchive = SQLiteArchive()

if args.update:
    sqlitearchive.schema()
elif args.drop:
    sqlitearchive.drop()
elif args.compact and not args.files:
    sqlitearchive.compact()
elif args.compact and args.files and not args.update:
    sqlitearchive.add()
    atexit.register(sqlitearchive.compact)
elif args.extract and not args.update:
    sqlitearchive.extract()
else:
    sqlitearchive.add()
