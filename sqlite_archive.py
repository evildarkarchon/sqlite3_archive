#!/usr/bin/env python3

from __future__ import annotations

import glob
import sqlite3
import argparse
import pathlib

parser: argparse.ArgumentParser = argparse.ArgumentParser(description='Puts files into an sqlite database as BLOB(s)')
parser.add_argument("db", type=str, help='SQLite DB filename')
parser.add_argument("table", type=str, help='SQLite DB table name containing BLOB(s)')
parser.add_argument("files", nargs="*", help='Files to be archived in the SQLite Database')

args: argparse.Namespace = parser.parse_args()

if len(args.files) == 0:
    print("No files specified, exiting.")
    exit()

db: pathlib.Path = pathlib.Path(args.db).resolve()
files: list = []

def globlist(listglob: list):
    outlist = []
    for i in listglob:
        if type(i) is str and "*" in i:
            globs = glob.glob(i)
            for x in globs:
                outlist.append(pathlib.Path(x))
        elif type(i) is pathlib.Path and i.is_file() or type(i) is str and pathlib.Path(i).is_file():
            outlist.append(i)
        elif type(i) is str and "*" not in listglob and pathlib.Path(i).is_file():
            outlist.append(pathlib.Path(i))
        elif type(i) is str and "*" not in listglob and pathlib.Path(i).is_dir() or type(i) is pathlib.Path and i.is_dir():
            for y in pathlib.Path(i).rglob("*"):
                outlist.append(y)
        else:
            globs = glob.glob(i)
            for x in globs:
                outlist.append(pathlib.Path(x))
    # intermediatelist = [item for sublist in outlist for item in sublist]
    # outlist = intermediatelist
    outlist.sort()
    return outlist

for i in args.files:
    listglob = globlist(i)
    for x in listglob:
        if x.is_file():
            out = str(x)
            files.append(out)
if len(files) == 0:
    print("The file list is empty.")

if db.is_file():
    dbcon: sqlite3.Connection = sqlite3.connect(str(db))
else:
    db.touch()
    dbcon: sqlite3.Connection = sqlite3.connect(str(db))
data: list = []
for i in files:
    data.append(pathlib.Path(i))
dbcon.execute("""create table if not exists {} \
    (pk integer not null primary key autoincrement unique, filename text not null unique, data blob not null unique);""".format(args.table))
dbcon.execute("""create unique index if not exists {0}_index on {0} ("filename" asc);""".format(args.table))
dbcon.commit()

for i in data:
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
            dbcon.execute("insert into {} (filename, data) values (?, ?)".format(args.table), (name, i.read_bytes()))
    except sqlite3.IntegrityError:
        print("duplicate")
        continue
    else:
        dbcon.commit()
        print("done")
dbcon.execute('PRAGMA optimize;')
dbcon.close()