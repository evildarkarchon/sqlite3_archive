from __future__ import annotations

import sys
import argparse
import sqlite3
import pathlib

from typing import Any

parser: argparse.ArgumentParser = argparse.ArgumentParser(description='Extracts BLOB fields from a given SQLite Database')
parser.add_argument("--debug", action="store_true", name="debug", help="Print out debug information in the case an extraction fails.")
parser.add_argument("db", type=str, help='SQLite DB filename')
parser.add_argument("table", type=str, help='SQLite DB table name containing BLOB(s)')
parser.add_argument("outputdir", type=str, help='Output directory for storing extracted BLOBs')

args: argparse.Namespace = parser.parse_args()
db: pathlib.Path = pathlib.Path(args.db).resolve()
outputdir: pathlib.Path = pathlib.Path(args.outputdir).resolve()
# Check DB file exists before trying to connect
if db.is_file():
    dbcon: sqlite3.Connection = sqlite3.connect(str(db))
else:
    print("{} file does not exist!".format(str(db)))
    exit(-1)
dbcon.text_factory = bytes
if outputdir.is_file():
    raise(FileExistsError, "Location selected for the output directory points to a file.")

if not outputdir.is_dir():
    print("Creating outputdir directory...")
    outputdir.mkdir(parents=True)


query = "select pk, data from {} order by pk".format(args.table)
cursor: sqlite3.Cursor = dbcon.execute(query)

# Row order will be pk value then BLOB column(s)
row: Any = cursor.fetchone()
while row:
    try:
        data: bytes = bytes(row[1])
        name: Any = dbcon.execute("select filename from {} where pk == {}".format(args.table, str(row[0]))).fetchone()[0]
        name = name.decode(sys.stdout.encoding) if sys.stdout.encoding else name.decode("utf-8")

        outpath: pathlib.Path = outputdir.joinpath(name)
        if not pathlib.Path(outpath.parent).is_dir():
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
        # raise

    row = cursor.fetchone()  # Normal end of loop

cursor.close()
dbcon.close()