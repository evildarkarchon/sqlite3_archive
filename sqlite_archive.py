from __future__ import annotations

import sys
import argparse
import sqlite3
import pathlib
import glob
import atexit

from typing import Any

parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Imports or Exports files from an sqlite3 database.")
parser.add_argument("--db", "-d", dest="db", type=str, help="SQLite DB filename.")
parser.add_argument("--table", "-t", dest="table", type=str, help="Name of table to use.")
parser.add_argument("--extract", "-x", dest="extract", action="store_true", help="Extract all files from a table instead of adding files.")
parser.add_argument("--output-dir", "-o", dest="out", type=str, help="Directory to output files to, if in extraction mode (defaults to current directory).", default=str(pathlib.Path.cwd()))
parser.add_argument("--debug", dest="debug", action="store_true", help="Prints additional information.")
parser.add_argument("files", nargs="*", help="Files to be archived in the SQLite Database.")

args: argparse.Namespace = parser.parse_args()

def globlist(listglob: list):
    outlist = []
    for a in listglob:
        if type(a) is str and "*" in a:
            globs = glob.glob(a)
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
            globs = glob.glob(a)
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
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")
        
        listglob: list = globlist(args.files)
        for i in listglob:
            if i.is_file():
                self.files.append(i)
        if len(self.files) is 0 and not args.extract:
            raise RuntimeError("The file list is empty.")
    
    def add(self):
        self.dbcon.execute("""create table if not exists {} \
    (pk integer not null primary key autoincrement unique, filename text not null unique, data blob not null unique);""".format(args.table))
        self.dbcon.execute("""create unique index if not exists {0}_index on {0} ("filename" asc);""".format(args.table))
        self.dbcon.commit()

        for i in self.files:
            try:
                parents = sorted(pathlib.Path(i).parents)
                if parents[0] == "." and len(parents) == 2:
                    name = str(i.relative_to(i.parent))
                else:    
                    name = str(i.relative_to(parents[1]))
            except IndexError:
                name = str(i.relative_to(i.parent))
    
            try:
                if i.is_file():
                    print("* Adding {} to {}...".format(name, args.table), end=' ')    
                    self.dbcon.execute("insert into {} (filename, data) values (?, ?)".format(args.table), (name, i.read_bytes()))
            except sqlite3.IntegrityError:
                print("duplicate")

                if args.debug:
                    exctype, value = sys.exc_info()[:2]
                    print("Exception type = {}, value = {}".format(exctype, value))

                continue
            else:
                self.dbcon.commit()
                print("done")
    
    def extract(self):
        outputdir: pathlib.Path = pathlib.Path(args.out).resolve()
        if outputdir.is_file():
            raise RuntimeError("The output directory specified points to a file.")
        
        if not outputdir.exists():
            print("Creating outputdir directory...")
            outputdir.mkdir(parents=True)
        
        query: str = "select pk, data from {} order by pk".format(args.table)
        query2: str = "select pk, image_data from {} order by pk".format(args.table)
        cursor: sqlite3.Cursor = None
        try:
            cursor = self.dbcon.execute(query)
        except sqlite3.OperationalError:
            cursor = self.dbcon.execute(query2)

        row: Any = cursor.fetchone()
        while row:
            try:
                data: bytes = bytes(row[1])
                name: Any = self.dbcon.execute("select filename from {} where pk == {}".format(args.table, str(row[0]))).fetchone()[0]
                name = name.decode(sys.stdout.encoding) if sys.stdout.encoding else name.decode("utf-8")

                outpath: pathlib.Path = outputdir.joinpath(name)
                if not pathlib.Path(outpath.parent).exists():
                    pathlib.Path(outpath.parent).mkdir(parents=True)
                
                print("Extracting {}...".format(str(outpath)), end =' ')
                outpath.write_bytes(data)
                print("done")
            except sqlite3.OperationalError:
                print("failed")
                
                if args.debug:
                    exctype, value = sys.exc_info()[:2]
                    print("Exception type = {}, value = {}".format(exctype, value))
                
                row = cursor.fetchone()
                
                continue  # Skip to next loop if error here
        
            row = cursor.fetchone()  # Normal end of loop

sqlitearchive = SQLiteArchive()

if args.extract:
    sqlitearchive.extract()
else:
    sqlitearchive.add()