from __future__ import annotations

import sys
import argparse
import sqlite3
import pathlib
import glob
import atexit

from typing import Any

parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Imports or Exports files from an sqlite3 database.")
parser.add_argument("--db", "-d", name="db", type=str, help="SQLite DB filename.")
parser.add_argument("--table", "-t", name="table", type=str, help="Name of table to use.")
parser.add_argument("--extract", "-x", name="extract", type="store_true", help="Activate extraction mode (creation mode is default).")
parser.add_argument("--output-dir", "-o", name="out", type=str, help="Directory to output files to, if in extraction mode.")
parser.add_argument("files", nargs="+", help="Files to be archived in the SQLite Database.")

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
        
        if args.extract:
            self.dbcon.text_factory(bytes)
        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")
        
        listglob: list = globlist(args.files)
        for i in listglob:
            if i.is_file():
                out = str(i)
                self.files.append(out)
    
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
                continue
            else:
                self.dbcon.commit()
                print("done")