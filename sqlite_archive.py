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
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union
from sqlite3_archive.utility import DBUtility, globlist, duplist, calcname, cleantablename, infertable
from sqlite3_archive.fileinfo import FileInfo

files_args: Tuple = ("files", "*")
lowercase_table_args: Dict = {"long": "--lowercase-table", "action": "store_true", "dest": "lower",
                              "help": "Modify the inferred table name to be lowercase (has no effect if table name is specified)."}
table_arguments: Dict = {"long": "--table", "short": "-t", "dest": "table", "help": "Name of table to use."}
autovacuum_args: Dict = {"long": "--autovacuum-mode", "short": "-a", "nargs": 1, "dest": "autovacuum",
                         "choices_av1": [1, 'enabled', 'enable', 'full'],
                         "choices_av2": [2, 'incremental'],
                         "choices_av0": [0, 'disabled', 'disable'],
                         "default": "full",
                         "help": "Sets the automatic vacuum mode.", "default": "full"}
autovacuum_args["choices"] = []
autovacuum_args["choices"].extend(autovacuum_args["choices_av1"])
autovacuum_args["choices"].extend(autovacuum_args["choices_av2"])
autovacuum_args["choices"].extend(autovacuum_args["choices_av0"])

parser: argparse.ArgumentParser = argparse.ArgumentParser(
    description="Imports or Exports files from an sqlite3 database.")
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
parser.add_argument("--verbose",
                    "-v",
                    action="store_true",
                    help="Print some more information without changing the exception raising policy."
                    )
walargs = parser.add_mutually_exclusive_group()
walargs.add_argument("--wal", "-w", action="store_true", dest="wal", help="Use Write-Ahead Logging instead of rollback journal.")
walargs.add_argument("--rollback", "-r", action="store_true", dest="rollback", help="Switch back to rollback journal if Write-Ahead Logging is active.")

subparsers: argparse._SubParsersAction = parser.add_subparsers(dest="mode")

drop: argparse.ArgumentParser = subparsers.add_parser('drop',
                                                      aliases=['drop-table', 'drop_table'],
                                                      help="Drop the specified table. NOTE: this will run VACUUM when done, by default."
                                                      )
drop.add_argument("--no-drop-vacuum",
                  action="store_false",
                  dest="drop_vacuum",
                  help="Do not execute VACUUM when dropping a table")
drop.add_argument("table", help="Name of table to use")

add = subparsers.add_parser("add", help="Add files to the database.")
add.add_argument(autovacuum_args["long"], autovacuum_args["short"], default=autovacuum_args["default"],
                 dest=autovacuum_args["dest"], choices=autovacuum_args["choices"], help=autovacuum_args["help"])
add.add_argument(table_arguments["long"],
                 table_arguments["short"],
                 dest=table_arguments["dest"],
                 type=str,
                 help=table_arguments["help"])
add.add_argument("--replace",
                 "-r",
                 action="store_true",
                 help="Replace any existing file entry's data instead of skipping. By default, the VACUUM command will be run to prevent database fragmentation."
                 )
add.add_argument("--no-replace-vacuum",
                 action="store_false",
                 dest="replace_vacuum",
                 help="Do not run the VACUUM command after replacing data.")
add.add_argument("--dups-file",
                 type=str,
                 dest="dups_file",
                 help="Location of the file to store the list of duplicate files to. Defaults to duplicates.json in current directory.",
                 default=f"{pathlib.Path.cwd()}/duplicates.json")
add.add_argument("--no-dups",
                 action="store_false",
                 dest="dups",
                 help="Disables saving the duplicate list as a json file or reading an existing one from an existing file."
                 )
add.add_argument("--hide-dups",
                 dest="showdups",
                 action="store_false",
                 help="Hides the list of duplicate files.")
add.add_argument("--dups-current-db",
                 dest="dupscurrent",
                 action="store_true",
                 help="Only show the duplicates from the current database.")
add.add_argument(lowercase_table_args["long"],
                 action=lowercase_table_args["action"],
                 dest=lowercase_table_args["dest"],
                 help=lowercase_table_args["help"])
add.add_argument("--no-atomic",
                 action="store_false",
                 dest="atomic",
                 help="Run commit on every insert instead of at the end of the loop.")
add.add_argument(files_args[0],
                 nargs=files_args[1],
                 help="Files to be archived in the SQLite Database.")

compact = subparsers.add_parser("compact",
                                help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while)")

create = subparsers.add_parser("create",
                               aliases=['create-table', 'create_table'],
                               help="Runs the table creation queries and exits.")
create.add_argument(autovacuum_args["long"], autovacuum_args["short"], nargs=autovacuum_args["nargs"],
                    default=autovacuum_args["default"],
                    dest=autovacuum_args["dest"], choices=autovacuum_args["choices"],
                    help=autovacuum_args["help"])
create.add_argument("table", help=table_arguments["help"])

extract: argparse.ArgumentParser = subparsers.add_parser('extract', help="Extract files from a table instead of adding them.")
extract.add_argument(table_arguments["long"],
                     table_arguments["short"],
                     dest=table_arguments["dest"],
                     type=str,
                     help=table_arguments["help"])
extract.add_argument("--output-dir",
                     "-o",
                     dest="out",
                     type=str,
                     help="Directory to output files to. Defaults to a directory named after the table in the current directory."
                     )
extract.add_argument(lowercase_table_args["long"],
                     action=lowercase_table_args["action"],
                     dest=lowercase_table_args["dest"],
                     help=lowercase_table_args["help"])
extract.add_argument("--force",
                     "-f",
                     dest="force",
                     action="store_true",
                     help="Forces extraction of a file from the database, even if the digest of the data does not match the one recorded in the database."
                     )
extract.add_argument("--infer-pop-file",
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


if "table" in args and args.table:
    args.table = cleantablename(args.table, lower=args.lower)
elif "table" in args and not args.table:
    if args.mode in ('add', 'extract') and args.files:
        if args.mode == "add":
            args.table = infertable(mode=args.mode, lower=args.lower, files=args.files)
        elif args.mode == "extract":
            args.table = infertable(mode=args.mode, lower=args.lower, files=args.files, out=args.out, pop=args.pop)

    if not args.table:
        raise RuntimeError(
            "File or Directory specified not found and table was not specified."
        )


class SQLiteArchive(DBUtility):
    def __init__(self):
        addorcreate = args.mode in ("add", "create")
        super().__init__(args)
        self.files: list = []

        def setwal() -> bool:
            try:
                self.execquerynocommit("PRAGMA journal_mode=WAL;")
                new_journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)
                if addorcreate and new_journal_mode != journal_mode:
                    return True
                else:
                    return False
            except sqlite3.DatabaseError:
                return False
            return None

        def setdel() -> bool:
            try:
                self.execquerynocommit("PRAGMA journal_mode=delete;")
                new_journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)
                if addorcreate and new_journal_mode != journal_mode:
                    return True
                else:
                    return False
            except sqlite3.DatabaseError:
                return False
            return None

        def setav() -> bool:
            avstate = self.execquerynocommit("PRAGMA auto_vacuum;", one=True, returndata=True)
            avstate2 = None
            notchanged = "autovacuum mode not changed"
            if args.verbose or args.debug:
                print(f"current autovacuum mode: {avstate}")
            if args.autovacuum and args.autovacuum in ("enable", "enabled", "full", 1, "1") and not avstate == 1:
                self.execquerynocommit("PRAGMA auto_vacuum = 1")
                avstate2 = self.execquerynocommit("PRAGMA auto_vacuum;", one=True, returndata=True)
                if not args.mode == "compact" and avstate2 == 1:
                    return True
                else:
                    if avstate2 != 1 and avstate != 1 and args.verbose or args.debug:
                        print(notchanged)
                    return False
                if args.verbose or args.debug:
                    print("full auto_vacuum")
            elif args.autovacuum and args.autovacuum in ("incremental", 2, "2") and not avstate == 2:
                self.execquerynocommit("PRAGMA auto_vacuum = 2;")
                avstate2 = self.execquerynocommit("PRAGMA auto_vacuum;", one=True, returndata=True)
                if not args.mode == "compact" and avstate2 == 2:
                    return True
                else:
                    if avstate2 != 2 and avstate != 2 and args.verbose or args.debug:
                        print(notchanged)
                    return False
                if args.verbose or args.debug:
                    print("incremental auto_vacuum")
            elif args.autovacuum and args.autovacuum in ("disable", "disabled", 0, "0") and not avstate == 0:
                self.execquerynocommit("PRAGMA auto_vacuum = 0;")
                avstate2 = self.execquerynocommit("PRAGMA_auto_vacuum;", one=True, returndata=True)
                if not args.mode == "compact" and avstate2 == 0:
                    return True
                else:
                    if avstate2 != 0 and avstate != 0 and args.verbose or args.debug:
                        print(notchanged)
                    return False
                if args.verbose or args.debug:
                    print("auto_vacuum disabled")
            else:
                print("Somehow, argparse messed up and the autovacuum mode argument is not one of the valid choices, defaulting to full autovacuum mode.")
                if not avstate == 1:
                    self.execquerynocommit("PRAGMA auto_vacuum = 1;")
                    avstate2 = self.execquerynocommit("PRAGMA auto_vacuum;", one=True, returndata=True)
                    if not args.mode == "compact" and avstate2 == 1:
                        return True
                    else:
                        if avstate2 != 1 and avstate != 1 and args.verbose or args.debug:
                            print(notchanged)
                        return False
                else:
                    return False
            return None

        needsvacuum = False
        if addorcreate and "autovacuum" in args and args.autovacuum:
            needsvacuum = setav()

        journal_mode = self.execquerynocommit("PRAGMA journal_mode", one=True, returndata=True)
        wal = ("WAL", "wal", "Wal", "WAl")
        rollback = ("delete", "Delete", "DELETE")

        if addorcreate and "wal" in args and args.wal and journal_mode not in wal:
            needsvacuum = setwal()
        elif addorcreate and "rollback" in args and args.rollback and journal_mode not in rollback:
            needsvacuum = setdel()

        if needsvacuum:
            self.execquerynocommit("VACUUM;")

        self.dbcon.row_factory = sqlite3.Row

        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

        if args.mode == "add" and 'files' in args and len(args.files) > 0:
            self.files = [x for x in globlist(args.files, args.mode) if pathlib.Path(x).resolve() != pathlib.Path(args.db).resolve() and pathlib.Path(x).is_file()]
            self.files.sort()
            if args.debug or args.verbose:
                print("File List:")
                print(self.files, end="\n\n")
        elif args.mode == "extract" and "files" in args and len(args.files) > 0:
            for i in args.files:
                if "*" in i:
                    if args.verbose or args.debug:
                        print(f"Removing {i} from file list because it contains a glob character.")
                    args.files.remove(i)
                    if len(args.files) == 0:
                        raise ValueError("File list is empty when it's not supposed to be.")
            self.files = args.files
        if len(self.files) == 0 and args.mode == 'add':
            raise RuntimeError("No files were found.")

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

        dbname: str = calcname(self.db, verbose=args.verbose)
        if dbname not in list(dups.keys()):
            dups[dbname] = {}

        for i in self.files:
            if not type(i) == pathlib.Path:
                i = pathlib.Path(i)
            fullpath: pathlib.Path = i.resolve()
            fileinfo = FileInfo(name=calcname(i, verbose=args.verbose))
            try:
                if i.is_file():
                    exists: int = None
                    if args.replace:
                        exists = int(
                            self.execquerynocommit(
                                f"select count(distinct filename) from {args.table} where filename = ?", values=(fileinfo.name, ), one=True)[0])
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
                query = self.execquerynocommit(f"select filename from {args.table} where hash == ?", (fileinfo.digest, ))[0][0]
                querytype = type(query)
                querylen = len(query)
                if args.debug or args.verbose:
                    print(querytype)
                    print(querylen)
                    if querytype == sqlite3.Row:
                        print(tuple(query))
                    else:
                        print(query)
                if query and querylen >= 1:
                    print("duplicate")

                    dups[dbname][str(fullpath)] = str(query)
                    if args.debug or args.verbose:
                        print(query)

                def removefromdict():
                    try:
                        dups[dbname].pop(z)
                    except KeyError:
                        pass
                for z in tuple(dups[dbname].keys()):
                    query = str(pathlib.Path(str(query)).resolve())
                    if query in z:
                        removefromdict()

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
            duplist(dups, dbname, outfile=args.dups_file, show=args.showdups, currentdb=args.dupscurrent)

    def extract(self):
        def calcextractquery():
            fileslen = len(self.files)
            if args.files and fileslen > 0:
                if fileslen > 1:
                    questionmarks: Any = '?' * fileslen
                    out = f"select rowid, data from {args.table} where filename in ({','.join(questionmarks)}) order by filename asc"
                    # out = f"select rowid, data from {args.table} where filename in (?) order by filename asc" # executemany doesn't work on select satements, apparently
                if self.files and fileslen == 1:
                    out = f"select rowid, data from {args.table} where filename == ? order by filename asc"
            else:
                out = f"select rowid, data from {args.table} order by filename asc"

            return out

        self.dbcon.text_factory = bytes
        if not type(self.files) in (list, tuple):
            raise TypeError("self.files must be a list or tuple")
        if len(
                tuple(self.execquerynocommit(f"pragma table_info({args.table})",
                                             returndata=True))) < 1:
            raise sqlite3.OperationalError("No such table")

        if not args.out:
            args.out = pathlib.Path.cwd().joinpath(args.table.replace('_', ' '))

        outputdir: pathlib.Path = None
        if args.out and pathlib.Path(args.out).exists():
            outputdir = pathlib.Path(args.out).resolve()
        else:
            outputdir = pathlib.Path(args.out)

        if outputdir.is_file():
            raise RuntimeError("The output directory specified points to a file.")

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
        
        cursor: sqlite3.Cursor = None

        if self.files and len(self.files) > 0:
            if args.debug or args.verbose:
                print(query)
            cursor = self.execquerynocommit(query, self.files, raw=True, returndata=True)
        else:
            if args.debug or args.verbose:
                print(query)
            cursor = self.execquerynocommit(query, raw=True, returndata=True)

        row: Any = cursor.fetchone()
        while row:
            try:
                fileinfo: FileInfo = FileInfo()
                fileinfo.data = bytes(row["data"])
                fileinfo.name = self.execquerynocommit(
                    f"select filename from {args.table} where rowid == ?", values=(str(row["rowid"]), ), one=True, decode=True)
                fileinfo.digest = self.execquerynocommit(
                    f"select hash from {args.table} where rowid == ?", values=(str(row["rowid"]), ), one=True, decode=True)

                if not fileinfo.verify(fileinfo.digest) and not args.force:
                    if args.debug or args.verbose:
                        print(f"Calculated Digest: {fileinfo.calculatehash()}")
                        print(f"Recorded Hash: {fileinfo.digest}")
                    raise ValueError("The digest in the database does not match the calculated digest for the data.")

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
