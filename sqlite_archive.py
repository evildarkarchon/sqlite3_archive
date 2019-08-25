#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import json
import pathlib
import sqlite3
from typing import Any, Dict, List, Tuple, Union

from sqlite3_archive.fileinfo import FileInfo
from sqlite3_archive.utility import (DBUtility, calcname, cleantablename,
                                     duplist, globlist, infertable)


def parse_args() -> argparse.Namespace:

    files_args: Tuple = ("files", "*")
    lowercase_table_args: Dict = {
        "long":
        "--lowercase-table",
        "action":
        "store_true",
        "dest":
        "lower",
        "help":
        "Modify the inferred table name to be lowercase (has no effect if table name is specified)."
    }
    table_arguments: Dict = {
        "long": "--table",
        "short": "-t",
        "dest": "table",
        "help": "Name of table to use."
    }
    autovacuum_args: Dict = {
        "long": "--autovacuum-mode",
        "short": "-a",
        "nargs": 1,
        "dest": "autovacuum",
        "choices_av1": [1, 'enabled', 'enable', 'full'],
        "choices_av2": [2, 'incremental'],
        "choices_av0": [0, 'disabled', 'disable'],
        "default": "full",
        "help": "Sets the automatic vacuum mode.",
        "default": "full"
    }
    autovacuum_args["choices"] = []
    autovacuum_args["choices"].extend(autovacuum_args["choices_av1"])
    autovacuum_args["choices"].extend(autovacuum_args["choices_av2"])
    autovacuum_args["choices"].extend(autovacuum_args["choices_av0"])

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Imports or Exports files from an sqlite3 database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("db", type=str, help="SQLite DB filename.")
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Supress any exception skipping and print some additional info.")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print some more information without changing the exception raising policy."
    )
    parser.add_argument(autovacuum_args["long"],
                        autovacuum_args["short"],
                        nargs=autovacuum_args["nargs"],
                        default=autovacuum_args["default"],
                        dest=autovacuum_args["dest"],
                        choices=autovacuum_args["choices"],
                        help=autovacuum_args["help"])
    walargs = parser.add_mutually_exclusive_group()
    walargs.add_argument(
        "--wal",
        "-w",
        action="store_true",
        dest="wal",
        help="Use Write-Ahead Logging instead of rollback journal.")
    walargs.add_argument(
        "--rollback",
        "-r",
        action="store_true",
        dest="rollback",
        help="Switch back to rollback journal if Write-Ahead Logging is active."
    )

    subparsers: argparse._SubParsersAction = parser.add_subparsers(dest="mode")

    drop: argparse.ArgumentParser = subparsers.add_parser(
        'drop',
        aliases=['drop-table', 'drop_table'],
        help="Drop the specified table. NOTE: this will run VACUUM when done, by default.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    drop.add_argument("--no-vacuum",
                      dest="no_drop_vacuum",
                      action="store_true",
                      help="Do not execute VACUUM when dropping a table")
    drop.add_argument("table", help="Name of table to use")

    add = subparsers.add_parser(
        "add",
        help="Add files to the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add.add_argument(table_arguments["long"],
                     table_arguments["short"],
                     dest=table_arguments["dest"],
                     type=str,
                     help=table_arguments["help"])
    add.add_argument(
        "--replace",
        "-r",
        action="store_true",
        help="Replace any existing file entry's data instead of skipping. By default, the VACUUM command will be run to prevent database fragmentation."
    )
    add.add_argument(
        "--no-replace-vacuum",
        action="store_true",
        dest="no_replace_vacuum",
        help="Do not run the VACUUM command after replacing data.")
    add.add_argument(
        "--dups-file",
        type=str,
        dest="dups_file",
        help="Location of the file to store the list of duplicate files to.",
        default=f"{pathlib.Path.cwd().joinpath('duplicates.json')}")
    add.add_argument(
        "--no-dups",
        action="store_true",
        dest="nodups",
        help="Disables saving the duplicate list as a json file or reading an existing one from an existing file."
    )
    add.add_argument("--hide-dups",
                     dest="hidedups",
                     action="store_true",
                     help="Hides the list of duplicate files.")
    add.add_argument(
        "--dups-current-db",
        dest="dupscurrent",
        action="store_true",
        help="Only show the duplicates from the current database.")
    add.add_argument(lowercase_table_args["long"],
                     action=lowercase_table_args["action"],
                     dest=lowercase_table_args["dest"],
                     help=lowercase_table_args["help"])
    add.add_argument(
        "--no-atomic",
        action="store_true",
        dest="no_atomic",
        help="Run commit on every insert instead of at the end of the loop.")
    add.add_argument(
        "--exclude",
        action="append",
        dest="exclude",
        help="Name of a file to exclude from the file list (can be specified multiple times)"
    )
    add.add_argument("--vacuum",
                     action="store_true",
                     dest="vacuum",
                     help="Run VACUUM at the end.")
    add.add_argument(files_args[0],
                     nargs=files_args[1],
                     help="Files to be archived in the SQLite Database.")

    compact = subparsers.add_parser(
        "compact",
        help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    create = subparsers.add_parser(
        "create",
        aliases=['create-table', 'create_table'],
        help="Runs the table creation queries and exits.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create.add_argument("table", help=table_arguments["help"])

    extract: argparse.ArgumentParser = subparsers.add_parser(
        'extract',
        help="Extract files from a table instead of adding them.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    extract.add_argument(table_arguments["long"],
                         table_arguments["short"],
                         dest=table_arguments["dest"],
                         type=str,
                         help=table_arguments["help"])
    extract.add_argument(
        "--output-dir",
        "-o",
        dest="out",
        type=str,
        help="Directory to output files to. Defaults to a directory named after the table in the current directory."
    )
    extract.add_argument(lowercase_table_args["long"],
                         action=lowercase_table_args["action"],
                         dest=lowercase_table_args["dest"],
                         help=lowercase_table_args["help"])
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
    extract.add_argument(
        files_args[0],
        nargs=files_args[1],
        help="Files to be extracted from the SQLite Database. Leaving this empty will extract all files from the specified table."
    )

    return parser.parse_args()


class SQLiteArchive(DBUtility):
    def __init__(self):
        self.args: argparse.Namespace = parse_args()
        if self.args.verbose or self.args.debug:
            print("* Parsed Command Line Arguments: ", end=' ', flush=True)
            print(self.args)

        if "table" in self.args and self.args.table:
            self.args.table = cleantablename(self.args.table,
                                             lower=self.args.lower)

        addorcreate: bool = self.args.mode in ("add", "create")
        super().__init__(self.args)
        self.files: list = []

        self.set_journal_and_av(self.args)

        self.dbcon.row_factory = sqlite3.Row

        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

        if self.args.mode == "add" and 'files' in self.args and len(
                self.args.files) > 0:
            self.files = [
                x for x in globlist(self.args.files, self.args.mode)
                if pathlib.Path(x).resolve() != pathlib.Path(
                    self.args.db).resolve() and pathlib.Path(x).is_file()
            ]
            self.files.sort()
            if not self.args.exclude:
                self.args.exclude = ["Thumbs.db"]
            for i in self.files:
                if str(i) in self.args.exclude:
                    self.files.remove(i)

            if self.args.debug or self.args.verbose:
                print("File List:")
                print(self.files, end="\n\n")
        elif self.args.mode == "extract" and "files" in self.args and len(
                self.args.files) > 0:
            for i in self.args.files:
                if "*" in i:
                    if self.args.verbose or self.args.debug:
                        print(
                            f"Removing {i} from file list because it contains a glob character."
                        )
                    self.args.files.remove(i)
                    if len(self.args.files) == 0:
                        raise ValueError(
                            "File list is empty when it's not supposed to be.")
            self.files = self.args.files
        if len(self.files) == 0 and self.args.mode == 'add':
            raise RuntimeError("No files were found.")

    def drop(self):
        print(f"* Deleting table {self.args.table}...", end=' ', flush=True)
        try:
            self.execquerycommit(f"DROP TABLE {self.args.table}")
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")
            if not self.args.no_drop_vacuum:
                self.compact()

    def rename(self, name1: str, name2: str):
        print(f"* Renaming {name1} to {name2}...", end=' ', flush=True)
        try:
            self.execquerycommit(
                f"update {self.args.table} set filename = ? where filename = ?",
                (name1, name2))
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")

    def schema(self):
        createtable = f'CREATE TABLE IF NOT EXISTS {self.args.table} ( "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE, PRIMARY KEY("hash") );'
        self.execquerycommit(createtable)
        createindex = f'CREATE UNIQUE INDEX IF NOT EXISTS {self.args.table}_index ON {self.args.table} ( "filename", "hash" );'
        self.execquerycommit(createindex)

    def add(self):
        def insert():
            query = f"insert into {self.args.table} (filename, data, hash) values (?, ?, ?)"
            values = (fileinfo.name, fileinfo.data, fileinfo.digest)
            if self.args.no_atomic:
                print(f"* Adding {fileinfo.name} to {self.args.table}...",
                      end=' ',
                      flush=True)
                self.execquerycommit(query, values)
            else:
                print(
                    f"* Queueing {fileinfo.name} for addition to {self.args.table}...",
                    end=' ',
                    flush=True)
                self.execquerynocommit(query, values)

        def replace():
            query = f"replace into {self.args.table} (filename, data, hash) values (?, ?, ?)"
            values = (fileinfo.name, fileinfo.data, fileinfo.digest)
            if self.args.no_atomic:
                print(
                    f"* Replacing {fileinfo.name}'s data in {self.args.table} with specified file's data...",
                    end=' ',
                    flush=True)
                self.execquerycommit(query, values)
            else:
                print(
                    f"* Queueing {fileinfo.name}'s data for replacement in {self.args.table} with specified file's data...",
                    end=' ',
                    flush=True)
                self.execquerynocommit(query, values)

        if not self.args.table:
            self.args.table = infertable(mode=self.args.mode,
                                         lower=self.args.lower,
                                         files=self.files)

        if "table" in self.args and not self.args.table:
            raise RuntimeError(
                "File or Directory specified not found and table was not specified."
            )

        if self.args.table:
            self.schema()
        dups: dict = {}
        if "dups_file" in self.args and self.args.dups_file:
            dupspath = pathlib.Path(self.args.dups_file)
            if dupspath.exists():
                dupspath = dupspath.resolve()
            if dupspath.is_file() and not self.args.nodups:
                dups = json.loads(dupspath.read_text())
        replaced: int = 0

        dbname: str = calcname(self.db, verbose=self.args.verbose)
        if dbname not in list(dups.keys()):
            dups[dbname] = {}

        for i in self.files:
            if not type(i) == pathlib.Path:
                i = pathlib.Path(i)
            fullpath: pathlib.Path = i.resolve()
            fileinfo = FileInfo(name=calcname(i, verbose=self.args.verbose))
            try:
                if i.is_file():
                    exists: int = None
                    if self.args.replace:
                        exists = int(
                            self.execquerynocommit(
                                f"select count(distinct filename) from {self.args.table} where filename = ?",
                                values=(fileinfo.name, ),
                                one=True)[0])
                        if self.args.debug or self.args.verbose:
                            print(exists)
                    fileinfo.data = bytes(i.read_bytes())
                    fileinfo.digest = fileinfo.calculatehash()
                    if self.args.replace and exists and exists > 0:
                        replace()
                        replaced += 1
                    else:
                        insert()
            except sqlite3.IntegrityError:
                query = self.execquerynocommit(
                    f"select filename from {self.args.table} where hash == ?",
                    (fileinfo.digest, ))[0][0]
                querytype = type(query)
                querylen = len(query)
                if self.args.debug or self.args.verbose:
                    print(querytype)
                    print(querylen)
                    if querytype == sqlite3.Row:
                        print(tuple(query))
                    else:
                        print(query)
                if query and querylen >= 1:
                    print("duplicate")

                    dups[dbname][str(fullpath)] = str(query)
                    if self.args.debug or self.args.verbose:
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

                if self.args.debug:
                    raise
                else:
                    continue
            except sqlite3.InterfaceError:
                if i.stat().st_size > 1000000000:
                    print("too big, skipping.")
                else:
                    print("failed")
                if self.args.debug:
                    raise
                else:
                    continue
            else:
                print("done")
        if not self.args.no_atomic:
            print("* Finishing up...", end=' ', flush=True)
            try:
                self.dbcon.commit()
            except sqlite3.DatabaseError:
                print("failed")
                if self.args.debug:
                    raise
            else:
                print("done")

        if self.args.replace and not self.args.no_replace_vacuum and replaced > 0 or self.args.vacuum:
            self.compact()
        if not self.args.nodups:
            duplist(dups,
                    dbname,
                    outfile=self.args.dups_file,
                    hide=self.args.hidedups,
                    currentdb=self.args.dupscurrent)

    def extract(self):
        if not self.args.table:
            self.args.table = infertable(mode=self.args.mode,
                                         lower=self.args.lower,
                                         files=self.args.files,
                                         out=self.args.out,
                                         pop=self.args.pop)

        if "table" in self.args and not self.args.table:
            raise RuntimeError(
                "File or Directory specified not found and table was not specified."
            )

        def calcextractquery():
            fileslen = len(self.files)
            if self.args.files and fileslen > 0:
                if fileslen > 1:
                    questionmarks: Any = '?' * fileslen
                    out = f"select rowid, data from {self.args.table} where filename in ({','.join(questionmarks)}) order by filename asc"
                    # out = f"select rowid, data from {self.args.table} where filename in (?) order by filename asc" # executemany doesn't work on select satements, apparently
                if self.files and fileslen == 1:
                    out = f"select rowid, data from {self.args.table} where filename == ? order by filename asc"
            else:
                out = f"select rowid, data from {self.args.table} order by filename asc"

            return out

        self.dbcon.text_factory = bytes
        if not type(self.files) in (list, tuple):
            raise TypeError("self.files must be a list or tuple")
        if len(
                tuple(
                    self.execquerynocommit(
                        f"pragma table_info({self.args.table})",
                        returndata=True))) < 1:
            raise sqlite3.OperationalError("No such table")

        if not self.args.out:
            self.args.out = pathlib.Path.cwd().joinpath(
                self.args.table.replace('_', ' '))

        outputdir: pathlib.Path = None
        if self.args.out and pathlib.Path(self.args.out).exists():
            outputdir = pathlib.Path(self.args.out).resolve()
        else:
            outputdir = pathlib.Path(self.args.out)

        if outputdir.is_file():
            raise RuntimeError(
                "The output directory specified points to a file.")

        if not outputdir.exists():
            if self.args.verbose or self.args.debug:
                print("Creating output directory...")
            outputdir.mkdir(parents=True)

        if not outputdir.is_absolute():
            outputdir = outputdir.resolve()
        if self.args.debug or self.args.verbose:
            print(len(self.files))
            print(repr(tuple(self.files)))
        query: list = calcextractquery()

        cursor: Union[sqlite3.Cursor, None] = None

        if self.files and len(self.files) > 0:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.execquerynocommit(query,
                                            self.files,
                                            raw=True,
                                            returndata=True)
        else:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.execquerynocommit(query, raw=True, returndata=True)

        row: Any = cursor.fetchone()
        while row:
            try:
                fileinfo: FileInfo = FileInfo()
                fileinfo.data = bytes(row["data"])
                fileinfo.name = self.execquerynocommit(
                    f"select filename from {self.args.table} where rowid == ?",
                    values=(str(row["rowid"]), ),
                    one=True,
                    decode=True)
                fileinfo.digest = self.execquerynocommit(
                    f"select hash from {self.args.table} where rowid == ?",
                    values=(str(row["rowid"]), ),
                    one=True,
                    decode=True)

                if not fileinfo.verify(
                        fileinfo.digest) and not self.args.force:
                    if self.args.debug or self.args.verbose:
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

                if self.args.debug:
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

if sqlitearchive.args.mode == 'create':
    sqlitearchive.schema()
elif sqlitearchive.args.mode == 'drop':
    sqlitearchive.drop()
elif sqlitearchive.args.mode == 'compact':
    sqlitearchive.compact()
elif sqlitearchive.args.mode == 'extract':
    sqlitearchive.extract()
elif sqlitearchive.args.mode == 'add':
    sqlitearchive.add()
