#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import glob
import hashlib
import json
import pathlib
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

files_args: Tuple = ("files", "*")
lowercase_table_args: Tuple = (
    "--lowercase-table", "store_true", "lower",
    "Modify the inferred table name to be lowercase (has no effect if table name is specified)."
)
table_arguments: Tuple = ("--table", "-t", "Name of table to use.", "table")

parser: argparse.ArgumentParser = argparse.ArgumentParser(
    description="Imports or Exports files from an sqlite3 database.")
subparsers: argparse.ArgumentParser = parser.add_subparsers(dest="mode")
parser.add_argument("--db",
                    "-d",
                    dest="db",
                    type=str,
                    required=True,
                    help="SQLite DB filename.")
parser.add_argument("--debug",
                    dest="debug",
                    action="store_true",
                    help="Supress any exception skipping and print some additional info.")
parser.add_argument(
    "--verbose",
    "-v",
    action="store_true",
    help="Print some more information without changing the exception raising policy."
)

drop: argparse.ArgumentParser = subparsers.add_parser(
    'drop',
    aliases=['drop-table', 'drop_table'],
    help="Drop the specified table. NOTE: this will run VACUUM when done, by default."
)
drop.add_argument("--no-drop-vacuum",
                  action="store_false",
                  dest="drop_vacuum",
                  help="Do not execute VACUUM when dropping a table")
drop.add_argument("table", help="Name of table to use")

add = subparsers.add_parser("add", help="Add files to the database.")
add.add_argument(table_arguments[0],
                 table_arguments[1],
                 dest=table_arguments[3],
                 type=str,
                 help=table_arguments[2])
add.add_argument(
    "--replace",
    "-r",
    action="store_true",
    help="Replace any existing file entry's data instead of skipping. By default, the VACUUM command will be run to prevent database fragmentation."
)
add.add_argument("--no-replace-vacuum",
                 action="store_false",
                 dest="replace_vacuum",
                 help="Do not run the VACUUM command after replacing data.")
add.add_argument(
    "--dups-file",
    type=str,
    dest="dups_file",
    help="Location of the file to store the list of duplicate files to. Defaults to duplicates.json in current directory.",
    default=f"{pathlib.Path.cwd()}/duplicates.json")
add.add_argument(
    "--no-dups",
    action="store_false",
    dest="dups",
    help="Disables saving the duplicate list as a json file or reading an existing one from an existing file."
)
add.add_argument("--hide-dups",
                 dest="hidedups",
                 action="store_true",
                 help="Hides the list of duplicate files.")
add.add_argument("--dups-current-db",
                 dest="dupscurrent",
                 action="store_true",
                 help="Only show the duplicates from the current database.")
add.add_argument(lowercase_table_args[0],
                 action=lowercase_table_args[1],
                 dest=lowercase_table_args[2],
                 help=lowercase_table_args[3])
add.add_argument(
    "--no-atomic",
    action="store_false",
    dest="atomic",
    help="Run commit on every insert instead of at the end of the loop.")
add.add_argument(files_args[0],
                 nargs=files_args[1],
                 help="Files to be archived in the SQLite Database.")

compact = subparsers.add_parser(
    "compact",
    help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while)"
)

create = subparsers.add_parser(
    "create",
    aliases=['create-table', 'create_table'],
    help="Runs the table creation queries and exits.")
create.add_argument("table", help=table_arguments[2])

extract: argparse.ArgumentParser = subparsers.add_parser(
    'extract', help="Extract files from a table instead of adding them.")
extract.add_argument(table_arguments[0],
                     table_arguments[1],
                     dest=table_arguments[3],
                     type=str,
                     help=table_arguments[2])
extract.add_argument(
    "--output-dir",
    "-o",
    dest="out",
    type=str,
    help="Directory to output files to. Defaults to a directory named after the table in the current directory."
)
extract.add_argument(lowercase_table_args[0],
                     action=lowercase_table_args[1],
                     dest=lowercase_table_args[2],
                     help=lowercase_table_args[3])
extract.add_argument(
    "--force",
    "-f",
    dest="force",
    action="store_true",
    help="Forces extraction of a file from the database, even if the digest of the data does not match the one recorded in the database."
)
extract.add_argument(
    "--infer-pop-file",
    action="store_true",
    dest="pop",
    help="Removes the first entry in the file list when inferring the table name (has no effect when table name is specified)."
)
extract.add_argument(files_args[0],
                     nargs=files_args[1],
                     help="Files to be extracted from the SQLite Database.")

args: argparse.Namespace = parser.parse_args()
if args.verbose or args.debug:
    print("* Parsed Command Line Arguments: ", end=' ', flush=True)
    print(args)


def cleantablename(instring: str, lower: bool = False):
    out = instring.replace(".", "_").replace(
        ' ', '_').replace("'", '_').replace(",", "").replace("/", '_').replace(
            '\\', '_').replace('-', '_').replace('#', '')
    if lower:
        return out.lower()
    else:
        return out


if "table" in args and args.table:
    args.table = cleantablename(args.table)


def infertable():
    base: pathlib.Path = pathlib.Path(args.files[0]).resolve()

    if not base.exists():
        return None

    f: str = None
    if args.mode == "add" and base.is_file():
        f = cleantablename(base.parent.name, args.lower)
    elif args.mode == "add" and base.is_dir():
        f = cleantablename(base.name, args.lower)

    if args.mode == "extract":
        if args.files[0]:
            if args.files[0] and not args.out:
                f = cleantablename(pathlib.Path(args.files[0]).stem)
                if args.pop:
                    args.files.pop(0)
            elif args.out and not args.files[0]:
                f = cleantablename(pathlib.Path(args.out).name)

    if f:
        return f
    else:
        return None


@dataclass
class FileInfo:
    name: str = None
    data: bytes = None
    digest: str = None

    def __post_init__(self):
        name = None
        if self.name:
            name = pathlib.Path(self.name)
        if name and name.resolve().is_file() and not self.data:
            self.data = name.resolve().read_bytes()
        if self.data and not self.digest:
            self.digest = self.calculatehash()

    def calculatehash(self):
        if self.data:
            filehash = hashlib.sha512()
            filehash.update(self.data)
            return filehash.hexdigest()
        else:
            return None

    def verify(self, refhash: str):
        calchash = self.calculatehash()
        if args.debug or args.verbose:
            print(f"* Verifying digest for {self.name}...",
                  end=' ',
                  flush=True)
        if calchash == refhash:
            if args.debug or args.verbose:
                print("pass", flush=True)
            return True
        elif calchash != refhash:
            if args.debug or args.verbose:
                print("failed", flush=True)
            return False


if "table" in args and not args.table:
    if args.mode in ('add', 'extract') and args.files:
        args.table = infertable()
        if not args.table:
            raise RuntimeError(
                "File or Directory specified not found and table was not specified."
            )


def globlist(listglob: List):
    for a in listglob:
        objtype = type(a)
        if args.mode == "extract":
            yield from listglob
            break

        if objtype is str and "*" in a:
            yield from map(pathlib.Path, glob.glob(a, recursive=True))
        elif objtype is pathlib.Path and a.is_file() or objtype is str and pathlib.Path(a).is_file():
            if objtype is str:
                yield pathlib.Path(a)
            elif objtype is pathlib.Path:
                yield a
            else:
                continue
        elif objtype is str and "*" not in a and pathlib.Path(a).is_file():
            yield pathlib.Path(a)
        elif objtype is str and "*" not in a and pathlib.Path(a).is_dir() or objtype is pathlib.Path and a.is_dir():
            yield from pathlib.Path(a).rglob("*")
        else:
            yield from map(pathlib.Path, glob.glob(a, recursive=True))


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
                print(
                    f"Duplicate Files:\n {json.dumps(dups[dbname], indent=4)}")
            elif not args.dupscurrent:
                print(f"Duplicate files:\n {json.dumps(dups, indent=4)}")
        if args.dups_file and dupsexist:
            dupspath = pathlib.Path(args.dups_file)
            dupspath.write_text(json.dumps(dups, indent=4))


class SQLiteArchive:
    def __init__(self):
        self.db: pathlib.Path = pathlib.Path(args.db).resolve()
        self.files: list = []

        if self.db.is_file():
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            if not (self.dbcon.execute("PRAGMA auto_vacuum;").fetchone()[0]
                    ) == 1 and args.mode in ("add", "create"):
                self.dbcon.execute("PRAGMA auto_vacuum = 1;")
                self.dbcon.execute("VACUUM;")
        elif not self.db.is_file() and args.mode in ("add", "create"):
            self.db.touch()
            self.dbcon: sqlite3.Connection = sqlite3.connect(self.db)
            self.dbcon.execute("PRAGMA auto_vacuum = 1;")
            self.dbcon.execute("VACUUM;")
        else:
            raise RuntimeError(
                "Extract mode and Compact mode require an existing database.")

        self.dbcon.row_factory = sqlite3.Row
        if args.mode == 'extract':
            self.dbcon.text_factory = bytes
        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

        if args.mode in ("add", "extract") and 'files' in args and len(
                args.files) > 0:
            self.files = [x for x in globlist(args.files) if pathlib.Path(x).resolve() != pathlib.Path(args.db).resolve() and x.is_file()]
            self.files.sort()
            if args.debug or args.verbose:
                print("File List:")
                print(self.files, end="\n\n")
        if len(self.files) == 0 and args.mode == 'add':
            raise RuntimeError("No files were found.")

    def execquerynocommit(self,
                          query: str,
                          values: Union[tuple, list] = None,
                          one: bool = False,
                          raw: bool = False,
                          returndata=False,
                          decode: bool = False):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")
        output: Any = None
        returnlist = ("select", "SELECT", "Select")
        if (any(i in query for i in returnlist)
                and returndata is False) or returndata is True:
            if values:
                output = self.dbcon.execute(query, values)
            else:
                output = self.dbcon.execute(query)

            if one:
                _out = output.fetchone()[0]
                if type(_out) is bytes and decode:
                    _out = _out.decode(
                        sys.stdout.encoding
                    ) if sys.stdout.encoding else _out.decode("utf-8")
                return _out
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
        if values and type(values) in (list, tuple):
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
        print(f"* Deleting table {args.table}...", end=' ', flush=True)
        try:
            self.execquerycommit(f"DROP TABLE {args.table}")
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")
            if args.drop_vacuum:
                self.compact()

    def rename(self, name1: str, name2: str):
        print(f"* Renaming {name1} to {name2}...", end=' ', flush=True)
        try:
            self.execquerycommit(
                f"update {args.table} set filename = ? where filename = ?",
                (name1, name2))
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")

    def schema(self):
        createtable = f'CREATE TABLE IF NOT EXISTS {args.table} ( "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE, PRIMARY KEY("hash") );'
        self.execquerycommit(createtable)
        createindex = f'CREATE UNIQUE INDEX IF NOT EXISTS {args.table}_index ON {args.table} ( "filename", "hash" );'
        self.execquerycommit(createindex)

    def calcname(self, inpath: pathlib.Path):
        parents = sorted(inpath.parents)
        parentslen = len(parents)
        if args.verbose or args.debug:
            print(parents)

        def oldbehavior():
            if args.verbose or args.debug:
                print("Using old name calculation behavior")
            if parentslen > 2:
                return str(inpath.relative_to(inpath.parent.parent))
            else:
                return str(inpath.relative_to(inpath.parent))

        try:
            if parentslen == 1:
                return str(inpath.resolve().relative_to(pathlib.Path.cwd()))
            elif inpath.is_absolute() and str(pathlib.Path.cwd()) in str(inpath):
                return str(inpath.resolve().relative_to(pathlib.Path.cwd()))
            elif not inpath.is_absolute() and parentslen > 1:
                return str(inpath.relative_to(parents[1]))
            else:
                return oldbehavior()
        except (ValueError, IndexError):
            try:
                return oldbehavior()
            except Exception:
                raise

    def add(self):
        def insert():
            query = f"insert into {args.table} (filename, data, hash) values (?, ?, ?)"
            values = (fileinfo.name, fileinfo.data, fileinfo.digest)
            if args.atomic:
                print(f"* Queueing {fileinfo.name} for addition to {args.table}...", end=' ', flush=True)
                self.execquerynocommit(query, values)
            else:
                print(f"* Adding {fileinfo.name} to {args.table}...", end=' ', flush=True)
                self.execquerycommit(query, values)

        def replace():
            query = f"replace into {args.table} (filename, data, hash) values (?, ?, ?)"
            values = (fileinfo.name, fileinfo.data, fileinfo.digest)
            if args.atomic:
                print(f"* Queueing {fileinfo.name}'s data for replacement in {args.table} with specified file's data...", end=' ', flush=True)
                self.execquerynocommit(query, values)
            else:
                print(f"* Replacing {fileinfo.name}'s data in {args.table} with specified file's data...", end=' ', flush=True)
                self.execquerycommit(query, values)

        self.schema()
        dups: dict = {}
        dupspath = pathlib.Path(args.dups_file)
        if dupspath.is_file() and args.dups:
            dups = json.loads(dupspath.read_text())
        replaced: int = 0

        dbname: str = self.calcname(self.db)
        if dbname not in list(dups.keys()):
            dups[dbname] = {}

        for i in self.files:
            if not type(i) == pathlib.Path:
                i = pathlib.Path(i)
            fullpath: pathlib.Path = i.resolve()
            fileinfo = FileInfo(name=self.calcname(i))
            try:
                if i.is_file():
                    exists: int = None
                    if args.replace:
                        exists = int(
                            self.execquerynocommit(
                                f"select count(distinct filename) from {args.table} where filename = ?",
                                values=(fileinfo.name, ),
                                one=True))
                        if args.debug or args.verbose:
                            print(exists)
                    fileinfo.data = bytes(i.read_bytes())
                    fileinfo.digest = fileinfo.calculatehash()
                    if args.replace and exists and exists > 0:
                        replace()
                        replaced += 1
                    else:
                        insert()
            except sqlite3.IntegrityError:

                query = self.execquerynocommit(
                    f"select filename from {args.table} where hash == ?",
                    (fileinfo.digest, ), one=True)
                if query and query["filename"] and len(query["filename"]) >= 1:
                    print("duplicate")

                if type(query) is list and len(query) >= 1:
                    if query is not None:
                        dups[dbname][str(fullpath)] = query["filename"]

                for z in tuple(dups[dbname].keys()):
                    if query["filename"] in z:
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
            print("* Finishing up...", end=' ', flush=True)
            try:
                self.dbcon.commit()
            except sqlite3.DatabaseError:
                print("failed")
                if args.debug:
                    raise
            else:
                print("done")

        if args.replace and args.replace_vacuum and replaced > 0:
            self.compact()
        if args.dups:
            duplist(dups, dbname)

    def extract(self):
        def calcextractquery():
            if args.files and len(args.files) > 0:
                if len(self.files) > 1:
                    questionmarks: Any = '?' * len(args.files)
                    out = f"select rowid, data from {args.table} where filename in ({','.join(questionmarks)}) order by filename asc"
                elif args.files and len(args.files) == 1:
                    out = f"select rowid, data from {args.table} where filename == ? order by filename asc"
            else:
                out = f"select rowid, data from {args.table} order by filename asc"

            return out

        if len(
                tuple(self.execquerynocommit(f"pragma table_info({args.table})",
                                       returndata=True))) < 1:
            raise sqlite3.OperationalError("No such table")

        if not args.out:
            args.out = pathlib.Path.cwd().joinpath(args.table.replace(
                '_', ' '))

        outputdir: pathlib.Path = None
        if args.out and pathlib.Path(args.out).exists():
            outputdir = pathlib.Path(args.out).resolve()
        else:
            outputdir = pathlib.Path(args.out)

        if outputdir.is_file():
            raise RuntimeError(
                "The output directory specified points to a file.")

        if not outputdir.exists():
            if args.verbose or args.debug:
                print("Creating output directory...")
            outputdir.mkdir(parents=True)

        if not outputdir.is_absolute():
            outputdir = outputdir.resolve()
        if args.debug or args.verbose:
            print(len(self.files))
            print(repr(tuple(self.files)))
        query: list = calcextractquery()
        if args.debug or args.verbose:
            print(query)
        cursor: sqlite3.Cursor = None

        if self.files and len(self.files) > 0 or "?" in query:
            cursor = self.execquerynocommit(query, self.files, raw=True)
        else:
            cursor = self.execquerynocommit(query, raw=True)

        row: Any = cursor.fetchone()
        while row:
            try:
                fileinfo: FileInfo = FileInfo()
                fileinfo.data = bytes(row["data"])
                fileinfo.name = self.execquerynocommit(
                    f"select filename from {args.table} where rowid == ?",
                    values=(str(row["rowid"]), ),
                    one=True,
                    decode=True)
                fileinfo.digest = self.execquerynocommit(
                    f"select hash from {args.table} where rowid == ?",
                    values=(str(row["rowid"]), ),
                    one=True,
                    decode=True)

                if not fileinfo.verify(fileinfo.digest) and not args.force:
                    if args.debug or args.verbose:
                        print(f"Calculated Digest: {fileinfo.calculatehash()}")
                        print(f"Recorded Hash: {fileinfo.digest}")
                    raise ValueError(
                        "The digest in the database does not match the calculated digest for the data."
                    )

                outpath: pathlib.Path = outputdir.joinpath(fileinfo.name)

                parent = pathlib.Path(outpath.parent)
                if not parent.exists():
                    parent.mkdir(parents=True)

                print(f"* Extracting {str(outpath)}...", end=' ', flush=True)
                outpath.write_bytes(fileinfo.data)
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
        print("* Compacting the database, this might take a while...",
              end=' ',
              flush=True)

        try:
            self.dbcon.execute("VACUUM;")
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")


sqlitearchive: SQLiteArchive = SQLiteArchive()

if args.mode == 'create':
    sqlitearchive.schema()
elif args.mode == 'drop':
    sqlitearchive.drop()
elif args.mode == 'compact' and not args.files:
    sqlitearchive.compact()
elif args.mode == 'compact' and args.files:
    sqlitearchive.add()
    atexit.register(sqlitearchive.compact)
elif args.mode == 'extract':
    sqlitearchive.extract()
else:
    sqlitearchive.add()
