#!/usr/bin/env python3
from __future__ import annotations

import atexit
import json
import pathlib
import sqlite3
from typing import Any, Dict, List, Tuple, Union

from argparse import Namespace
from fileinfo import fileinfo
from utility import files, parse_args, table, utility

from .imageproc import imageproc


class SQLiteArchive(utility.DBUtility):
    def __init__(self):
        self.args: Namespace = parse_args.parse_args()
        if self.args.verbose or self.args.debug:
            print("* Parsed Command Line Arguments: ", end=' ', flush=True)
            print(self.args)

        if "table" in self.args and self.args.table:
            self.args.table = table.cleantablename(self.args.table,
                                             lower=self.args.lower)

        self.args.files = list(set(self.args.files))

        super().__init__(self.args)

        self.set_journal_and_av(self.args)

        self.dbcon.row_factory = sqlite3.Row

        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

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
            self.execquerycommit(f"update {self.args.table} set filename = ? where filename = ?", (name1, name2))
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")

    def schema(self):
        self.execquerycommit(f'CREATE TABLE IF NOT EXISTS {self.args.table} ( "filename" TEXT NOT NULL UNIQUE, "data" BLOB NOT NULL, "hash" TEXT NOT NULL UNIQUE, PRIMARY KEY("hash") );')

    def add(self):
        if len(self.args.files) > 0:
            fileslist: List = []
            fileslist = [x for x in files.globlist(self.args.files) if pathlib.Path(x).resolve() != pathlib.Path(self.args.db).resolve()]
            fileslist.sort()
            
            """The file exclusion code is currently a WIP. It currently only works based on file names
            because it strips all directory components from the exclusion list."""
            if not self.args.exclude or len(self.args.exclude) == 0:
                self.args.exclude = ["Thumbs.db"]
            self.args.exclude = list(set(self.args.exclude))
            self.args.exclude = [pathlib.Path(i).name for i in self.args.exclude]
            fileslist = [i for i in fileslist if pathlib.Path(i).name not in self.args.exclude]

            if self.args.debug or self.args.verbose:
                print("File List:")
                print(fileslist)
                print("Exclude List:")
                print(self.args.exclude, end="\n\n")
            if not fileslist:
                raise RuntimeError("No files were found.")

        def insert():
            query: str = f"insert into {self.args.table} (filename, data, hash) values (?, ?, ?)"
            values: Tuple = (info.name, info.data, info.digest)
            if self.args.no_atomic:
                print(f"* Adding {info.name} to {self.args.table}...",
                      end=' ',
                      flush=True)
                self.execquerycommit(query, values)
            else:
                print(
                    f"* Queueing {info.name} for addition to {self.args.table}...",
                    end=' ',
                    flush=True)
                self.execquerynocommit(query, values)

        def replace():
            query: str = f"replace into {self.args.table} (filename, data, hash) values (?, ?, ?)"
            values: Tuple = (info.name, info.data, info.digest)
            if self.args.no_atomic:
                print(
                    f"* Replacing {info.name}'s data in {self.args.table} with specified file's data...",
                    end=' ',
                    flush=True)
                self.execquerycommit(query, values)
            else:
                print(
                    f"* Queueing {info.name}'s data for replacement in {self.args.table} with specified file's data...",
                    end=' ',
                    flush=True)
                self.execquerynocommit(query, values)

        if not self.args.table:
            self.args.table = table.infertable(mode=self.args.mode,
                                         lower=self.args.lower,
                                         files=fileslist)

        if "table" in self.args and not self.args.table:
            raise RuntimeError("File or Directory specified not found and table was not specified.")

        if self.args.table:
            self.schema()
        
        dbname: str = files.calcname(self.db, verbose=self.args.verbose)
        dups: dict = {}
        dups[dbname] = {}
        if "dups_file" in self.args and self.args.dups_file:
            dupspath: pathlib.Path = pathlib.Path(self.args.dups_file).resolve()
            if dupspath.is_file() and not self.args.nodups:
                dups.update(json.loads(dupspath.read_text()))
        replaced: int = 0

        if dbname in list(dups.keys()):
            dups[dbname] = {files.calcname(i):[] for i in fileslist if i not in dups[dbname]}

        if self.args.verbose or self.args.debug:
            print("Dups Dict:")
            print(dups)
    
        for i in fileslist:
            if not type(i) == pathlib.Path:
                i = pathlib.Path(i)
            fullpath: pathlib.Path = i.resolve()
            info: fileinfo.FileInfo = fileinfo.FileInfo(name=files.calcname(i, verbose=self.args.verbose))
            try:
                if i.is_file():
                    exists: int = None
                    if self.args.replace:
                        exists = int(
                            self.execquerynocommit(
                                f"select count(distinct filename) from {self.args.table} where filename = ?",
                                values=(info.name, ),
                                returndata=True,
                                one=True)[0])
                        if self.args.debug or self.args.verbose:
                            print(exists)
                    info.data = bytes(i.read_bytes())
                    info.digest = info.calculatehash()
                    if self.args.replace and exists and exists > 0:
                        replace()
                        replaced += 1
                    else:
                        insert()
            except sqlite3.IntegrityError:
                query = self.execquerynocommit(f"select filename from {self.args.table} where hash == ?", (info.digest, ), returndata=True)[0][0]
                querytype: str = type(query)
                querylen: int = len(query)
                if self.args.debug or self.args.verbose:
                    print(querytype)
                    print(querylen)
                    if querytype == sqlite3.Row:
                        print(tuple(query))
                    else:
                        print(query)
                if query and querylen >= 1:
                    print("duplicate")
                    try:
                        dups[dbname][str(query)].append(str(fullpath))
                    except KeyError:
                        dups[dbname][str(query)] = [str(fullpath)]
                    if self.args.debug or self.args.verbose:
                        print(query)
                
                dups[dbname][str(query)] = [g for g in dups[dbname][str(query)] if query not in g]

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

        dups[dbname] = {h:i for h, i in dups[dbname].items() if len(dups[dbname][h]) > 0}

        if self.args.replace and not self.args.no_replace_vacuum and replaced > 0 or self.args.vacuum:
            self.compact()
        if not self.args.nodups:
            files.duplist(dups,
                    dbname,
                    outfile=self.args.dups_file,
                    hide=self.args.hidedups,
                    currentdb=self.args.dupscurrent)

    def extract(self):
        if self.args.files:
            self.args.files = list(set(self.args.files))
            self.args.files = [i for i in self.args.files if "*" not in i]
        if not self.args.table:
            self.args.table = table.infertable(mode=self.args.mode,
                                         lower=self.args.lower,
                                         files=self.args.files,
                                         out=self.args.out,
                                         pop=self.args.pop)

        if "table" in self.args and not self.args.table:
            raise RuntimeError("File or Directory specified not found and table was not specified.")

        def calcextractquery():
            fileslen: int = len(self.args.files)
            if self.args.files and fileslen > 0:
                if fileslen > 1:
                    questionmarks: Any = '?' * fileslen
                    out = f"select rowid, data from {self.args.table} where filename in ({','.join(questionmarks)}) order by filename asc"
                    # out = f"select rowid, data from {self.args.table} where filename in (?) order by filename asc" # executemany doesn't work on select satements, apparently
                if self.args.files and fileslen == 1:
                    out = f"select rowid, data from {self.args.table} where filename == ? order by filename asc"
            else:
                out = f"select rowid, data from {self.args.table} order by filename asc"

            return out

        self.dbcon.text_factory = bytes
        if not type(self.args.files) in (list, tuple):
            raise TypeError("self.args.files must be a list or tuple")

        if len(tuple(self.execquerynocommit(f"pragma table_info({self.args.table})", returndata=True))) < 1:
            raise sqlite3.OperationalError("No such table")

        if not self.args.out:
            self.args.out = pathlib.Path.cwd().joinpath(self.args.table.replace('_', ' '))

        outputdir: pathlib.Path = None
        if self.args.out and pathlib.Path(self.args.out).exists():
            outputdir = pathlib.Path(self.args.out).resolve()
        else:
            outputdir = pathlib.Path(self.args.out)

        if outputdir.is_file():
            raise RuntimeError("The output directory specified points to a file.")

        if not outputdir.exists():
            if self.args.verbose or self.args.debug:
                print("Creating output directory...")
            outputdir.mkdir(parents=True)

        if not outputdir.is_absolute():
            outputdir = outputdir.resolve()
        if self.args.debug or self.args.verbose:
            print(len(self.args.files))
            print(repr(tuple(self.args.files)))
        query: List = calcextractquery()

        cursor: Union[sqlite3.Cursor, None] = None

        if self.args.files and len(self.args.files) > 0:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.execquerynocommit(query, self.args.files, raw=True, returndata=True)
        else:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.execquerynocommit(query, raw=True, returndata=True)

        row: Any = cursor.fetchone()
        while row:
            try:
                info: info = info.info()
                info.data = bytes(row["data"])
                try:
                    info.name = self.execquerynocommit(
                        f"select filename from {self.args.table} where rowid == ?",
                        values=(str(row["rowid"]), ),
                        one=True,
                        returndata=True,
                        decode=True)
                    info.digest = self.execquerynocommit(
                        f"select hash from {self.args.table} where rowid == ?",
                        values=(str(row["rowid"]), ),
                        one=True,
                        returndata=True,
                        decode=True)
                except IndexError:
                    info.name = self.execquerynocommit(
                        f"select filename from {self.args.table} where pk = ?",
                        values=(str(row["pk"]), ),
                        one=True,
                        returndata=True,
                        decode=True
                    )
                    info.digest = self.execquerynocommit(
                        f"select hash from {self.args.table} where pk == ?",
                        values=(str(row["pk"]), ),
                        one=True,
                        returndata=True,
                        decode=True
                    )

                if not info.verify(info.digest, self.args) and not self.args.force:
                    if self.args.debug or self.args.verbose:
                        print(f"Calculated Digest: {info.calculatehash()}")
                        print(f"Recorded Hash: {info.digest}")
                    raise ValueError("The digest in the database does not match the calculated digest for the data.")

                outpath: pathlib.Path = outputdir.joinpath(info.name)

                parent = pathlib.Path(outpath.parent)
                if not parent.exists():
                    parent.mkdir(parents=True)

                print(f"* Extracting {str(outpath)}...", end=' ', flush=True)
                outpath.write_bytes(info.data)
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
    def upgrade(self):
        print("This is a placeholder function for now.")
        # self.compact()