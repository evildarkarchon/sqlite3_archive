#!/usr/bin/env python3
import argparse
import atexit
import json
import pathlib
import sqlite3
from typing import Any, List, Dict, Union

from sqlite3_archive.fileinfo import FileInfo
from sqlite3_archive.utility import (DBUtility, calc_name, clean_table_name,
                                     process_duplicates, glob_list, infer_table)


def parse_args() -> argparse.Namespace:

    files_args: tuple = ("files", "*")
    lowercase_table_args: dict = {
        "long": "--lowercase-table",
        "action": "store_true",
        "dest": "lower",
        "help": "Modify the inferred table name to be lowercase (has no effect if table name is specified)."
    }
    table_arguments: dict = {
        "long": "--table",
        "short": "-t",
        "dest": "table",
        "help": "Name of table to use."
    }

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
    parser.add_argument("--autovacuum-mode",
                        "-a",
                        nargs=1,
                        default=1,
                        type=int,
                        dest="autvacuum",
                        choices=[0, 1, 2],
                        help="Sets the automatic vacuum mode. (0 = disabled, 1 = full autovacuum mode, 2 = incremental autovacuum mode)")
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
        help="Run commit on every insert instead of at the end of the loop (slower).")
    add.add_argument(
        "--exclude",
        action="append",
        dest="exclude",
        type=str,
        help="Name of a file to exclude from the file list (can be specified multiple times). Any directories specified are ignored at this time (per directory exclusion is WIP)."
    )
    add.add_argument("--vacuum",
                     action="store_true",
                     dest="vacuum",
                     help="Run VACUUM at the end.")
    add.add_argument(files_args[0],
                     nargs=files_args[1],
                     type=str,
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
        help="Directory to output files to. Defaults to a directory named after the table in the current directory. WARNING: Any existing files will have their data overwritten."
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
        type=str,
        help="Files to be extracted from the SQLite Database. Leaving this empty will extract all files from the specified table."
    )

    return parser.parse_args()


def calc_extract_query(args) -> str:
    files_len = len(args.files)

    if args.files and files_len > 0:
        question_marks = '?' * files_len
        if files_len > 1:
            return f"SELECT rowid, data FROM {args.table} WHERE filename IN ({','.join(question_marks)}) ORDER BY filename ASC"
        else:
            return f"SELECT rowid, data FROM {args.table} WHERE filename == ? ORDER BY filename ASC"
    return f"SELECT rowid, data FROM {args.table} ORDER BY filename ASC"


class SQLiteArchive(DBUtility):
    def __init__(self):
        self.args: argparse.Namespace = parse_args()

        if self.args.verbose or self.args.debug:
            print("* Parsed Command Line Arguments: ", end=' ', flush=True)
            print(self.args)

        if "table" in self.args and self.args.table:
            self.args.table = clean_table_name(self.args.table, lower=self.args.lower)

        self.args.files = list(set(self.args.files))

        super().__init__(self.args)

        self.set_journal_and_av(self.args)

        self.dbcon.row_factory = sqlite3.Row

        atexit.register(self.dbcon.close)
        atexit.register(self.dbcon.execute, "PRAGMA optimize;")

        self.files = self.args.files

    def drop(self):
        print(f"* Deleting table {self.args.table}...", end=' ', flush=True)
        try:
            self.exec_query_commit(f"DROP TABLE {self.args.table}")
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
            self.exec_query_commit(f"update {self.args.table} set filename = ? where filename = ?", (name1, name2))
        except sqlite3.DatabaseError:
            print("failed")
            raise
        else:
            print("done")

    def schema(self):
        self.exec_query_commit(f'''CREATE TABLE IF NOT EXISTS {self.args.table} (
            "filename" TEXT NOT NULL UNIQUE,
            "data" BLOB NOT NULL,
            "hash" TEXT NOT NULL UNIQUE,
            PRIMARY KEY("hash")
        );
        CREATE UNIQUE INDEX IF NOT EXISTS "{self.args.table}_filename_hash_index" ON "{self.args.table}" ("filename" ASC, "hash");
        CREATE UNIQUE INDEX IF NOT EXISTS "{self.args.table}_filename_index" ON "{self.args.table}" ("filename" ASC);
        CREATE UNIQUE INDEX IF NOT EXISTS "{self.args.table}_hash_index" ON "{self.args.table}" ("hash");
        CREATE UNIQUE INDEX IF NOT EXISTS "{self.args.table}_hash_filename_index" ON "{self.args.table}" ("hash", "filename" ASC);''')

    def filter_files_insert(self, files: List[str], exclude: List[str]) -> List[str]:
        return [file for file in files if pathlib.Path(file).name not in exclude]

    def print_file_info(self, files: List[str], exclude: List[str]) -> None:
        print("File List:")
        print(files)
        print("Exclude List:")
        print(exclude, end="\n\n")

    def prepare_files_and_exclusions(self) -> None:
        if self.args.files:
            self.files = [x for x in glob_list(self.args.files) if pathlib.Path(x).resolve() != pathlib.Path(self.args.db).resolve()]
            self.files.sort()

            if not self.args.exclude:
                self.args.exclude = ["Thumbs.db"]
            self.args.exclude = list(set(self.args.exclude))
            self.args.exclude = [pathlib.Path(i).name for i in self.args.exclude]
            files = self.filter_files_insert([str(items) for items in self.files], self.args.exclude)

            if self.args.verbose or self.args.debug:
                self.print_file_info(files, self.args.exclude)

            if not files:
                raise RuntimeError("No files were found.")

    def insert(self, fileinfo) -> None:
        query = f"insert into {self.args.table} (filename, data, hash) values (?, ?, ?)"
        values = (fileinfo.name, fileinfo.data, fileinfo.digest)
        if self.args.no_atomic:
            print(f"* Adding {fileinfo.name} to {self.args.table}...",
                  end=' ',
                  flush=True)
            self.exec_query_commit(query, values)
        else:
            print(
                f"* Queueing {fileinfo.name} for addition to {self.args.table}...",
                end=' ',
                flush=True)
            self.exec_query_no_commit(query, values)

    def filter_files_replace(self, file_paths: List[str]) -> List[str]:
        return [x for x in file_paths if pathlib.Path(x).resolve() != pathlib.Path(self.args.db).resolve()]

    def sorted_files(self, file_paths: List[str]) -> List[str]:
        sorted_files = self.filter_files_replace(file_paths)
        sorted_files.sort()
        return sorted_files

    def replace_fileinfo(self, fileinfo: FileInfo, query: str, values: tuple) -> None:
        if self.args.no_atomic:
            print(
                f"* Replacing {fileinfo.name}'s data in {self.args.table} with specified file's data...",
                end=' ',
                flush=True)
            self.exec_query_commit(query, values)
        else:
            print(
                f"* Queueing {fileinfo.name}'s data for replacement in {self.args.table} with specified file's data...",
                end=' ',
                flush=True)
            self.exec_query_no_commit(query, values)

    def replace(self, fileinfo: FileInfo) -> None:
        self.files = self.sorted_files(self.args.files)
        query = f"replace into {self.args.table} (filename, data, hash) values (?, ?, ?)"
        values = (fileinfo.name, fileinfo.data, fileinfo.digest)
        self.replace_fileinfo(fileinfo, query, values)

    def ensure_path_type(self, file):
        if not isinstance(file, pathlib.Path):
            return pathlib.Path(file)
        return file

    def process_file(self, file, replaced):
        i = self.ensure_path_type(file)
        fullpath = i.resolve()
        fileinfo = FileInfo(name=calc_name(fullpath, verbose=self.args.verbose))  # Assuming you have a FileInfo class.

        if i.is_file():
            exists = 0
            if self.args.replace:
                exists = self.check_existing_file(fileinfo)

            fileinfo.data = bytes(i.read_bytes())
            fileinfo.digest = fileinfo.calculate_hash()

            if self.args.replace and exists > 0:
                self.replace(fileinfo)
                replaced += 1
            else:
                self.insert(fileinfo)
        return replaced

    def check_existing_file(self, fileinfo):
        exists = int(
            self.exec_query_no_commit(
                f"select count(distinct filename) from {self.args.table} where filename = ?",
                values=(fileinfo.name,),
                return_data=True,
                one=True)[0])  # type: ignore
        if self.args.debug or self.args.verbose:
            print(exists)
        return exists

    def handle_integrity_error(self, fileinfo, fullpath, dbname, dups):
        query = self.exec_query_no_commit(f"select filename from {self.args.table} where hash == ?", (fileinfo.digest,), return_data=True)[0][0]  # type: ignore
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

    def handle_interface_error(self, i):
        if i.stat().st_size > 1000000000:
            print("too big, skipping.")
        else:
            print("failed")

    def process_files(self, dups: Dict, dbname: str):
        replaced = 0
        verbose = self.args.verbose
        for i in self.files:
            fullpath = pathlib.Path(i).resolve()
            fileinfo = FileInfo(name=calc_name(fullpath, verbose))
            try:
                replaced = self.process_file(i, replaced)
            except sqlite3.IntegrityError:
                self.handle_integrity_error(fileinfo, fullpath, dbname, dups)
                if self.args.debug:
                    raise
                else:
                    continue
            except sqlite3.InterfaceError:
                self.handle_interface_error(i)
                if self.args.debug:
                    raise
                else:
                    continue
            else:
                print("done")
        return replaced

    def finish_and_commit(self):
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

    def filter_non_empty_duplicates(self, dups, dbname):
        return {h: i for h, i in dups[dbname].items() if len(dups[dbname][h]) > 0}

    def process_vacuum_and_compact(self, replaced):
        if self.args.replace and not self.args.no_replace_vacuum and replaced > 0 or self.args.vacuum:
            self.compact()

    def process_all(self, dups: Dict, dbname: str, replaced: int):
        self.finish_and_commit()

        dups[dbname] = self.filter_non_empty_duplicates(dups, dbname)

        self.process_vacuum_and_compact(replaced)

        if not self.args.nodups:
            process_duplicates(dups,
                               dbname,
                               outfile=self.args.dups_file,
                               hide=self.args.hidedups,
                               currentdb=self.args.dupscurrent)

    def add(self):
        if not self.args.table:
            self.args.table = infer_table(mode=self.args.mode,
                                          lower=self.args.lower,
                                          files=self.files)  # type: ignore

        if "table" in self.args and not self.args.table:
            raise RuntimeError("File or Directory specified not found and table was not specified.")

        if self.args.table:
            self.schema()

        dbname: str = calc_name(self.db, verbose=self.args.verbose)
        dups: dict = {}
        dups[dbname] = {}
        if "dups_file" in self.args and self.args.dups_file:
            dupspath: pathlib.Path = pathlib.Path(self.args.dups_file).resolve()
            if dupspath.is_file() and not self.args.nodups:
                dups.update(json.loads(dupspath.read_text()))
        replaced: int = 0

        if dbname in list(dups.keys()):
            dups[dbname] = {calc_name(pathlib.Path(i)): [] for i in self.files if i not in dups[dbname]}

        if self.args.verbose or self.args.debug:
            print("Dups Dict:")
            print(dups)

        self.process_files(dups, dbname)

        self.process_all(dups, dbname, replaced)

    def create_output_dir(self, outputdir: pathlib.PurePath) -> pathlib.Path:
        outputdir = pathlib.Path(outputdir)
        if not outputdir.exists():
            if self.args.verbose or self.args.debug:
                print("Creating output directory...")
            outputdir.mkdir(parents=True)
        return outputdir.resolve()

    def fetch_fileinfo(self, row: Any) -> FileInfo:
        fileinfo = FileInfo()
        fileinfo.data = bytes(row["data"])
        fileinfo.name = self.exec_query_no_commit(
            f"select filename from {self.args.table} where rowid == ?",
            values=(str(row["rowid"]), ),
            one=True,
            return_data=True,
            decode=True) # type: ignore
        fileinfo.digest = self.exec_query_no_commit(
            f"select hash from {self.args.table} where rowid == ?",
            values=(str(row["rowid"]), ),
            one=True,
            return_data=True,
            decode=True) # type: ignore
        return fileinfo

    def extract_file(self, fileinfo: FileInfo, outputdir: pathlib.Path) -> None:
        outpath = outputdir.joinpath(fileinfo.name) # type: ignore
        parent = pathlib.Path(outpath.parent)
        if not parent.exists():
            parent.mkdir(parents=True)

        print(f"* Extracting {str(outpath)}...", end=' ', flush=True)
        outpath.write_bytes(fileinfo.data)
        print("done")

    def extract(self):
        if self.args.files:
            self.args.files = list(set(self.args.files))
            self.args.files = [i for i in self.args.files if "*" not in i]
        if not self.args.table:
            self.args.table = infer_table(mode=self.args.mode,
                                          lower=self.args.lower,
                                          files=self.args.files,
                                          out=self.args.out,
                                          pop=self.args.pop)

        if "table" in self.args and not self.args.table:
            raise RuntimeError("File or Directory specified not found and table was not specified.")

        self.dbcon.text_factory = bytes
        if not type(self.args.files) in (list, tuple):
            raise TypeError("self.args.files must be a list or tuple")

        if len(tuple(self.exec_query_no_commit(f"pragma table_info({self.args.table})", return_data=True))) < 1:  # type: ignore
            raise sqlite3.OperationalError("No such table")

        if not self.args.out:
            self.args.out = pathlib.Path.cwd().joinpath(self.args.table.replace('_', ' '))  # type: ignore

        outputdir = self.create_output_dir(self.args.out)
        if self.args.debug or self.args.verbose:
            print(len(self.args.files))
            print(repr(tuple(self.args.files)))
        query: list = calc_extract_query()  # type: ignore

        cursor: sqlite3.Cursor | None = None

        if self.args.files and len(self.args.files) > 0:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.exec_query_no_commit(query, self.args.files, raw=True, return_data=True)  # type: ignore
        else:
            if self.args.debug or self.args.verbose:
                print(query)
            cursor = self.exec_query_no_commit(query, raw=True, return_data=True)  # type: ignore

        row: Any = cursor.fetchone()  # type: ignore
        while row:
            try:
                fileinfo: FileInfo = FileInfo()
                fileinfo.data = bytes(row["data"])
                try:
                    fileinfo.name = self.exec_query_no_commit(  # type: ignore
                        f"select filename from {self.args.table} where rowid == ?",
                        values=(str(row["rowid"]), ),
                        one=True,
                        return_data=True,
                        decode=True)  # type: ignore
                    fileinfo.digest = self.exec_query_no_commit(
                        f"select hash from {self.args.table} where rowid == ?",
                        values=(str(row["rowid"]), ),
                        one=True,
                        return_data=True,
                        decode=True)  # type: ignore
                except IndexError:
                    fileinfo.name = self.exec_query_no_commit(  # type: ignore
                        f"select filename from {self.args.table} where pk = ?",
                        values=(str(row["pk"])),
                        one=True,
                        return_data=True,
                        decode=True
                    )  # type: ignore
                    fileinfo.digest = self.exec_query_no_commit(
                        f"select hash from {self.args.table} where pk == ?",
                        values=(str(row["pk"]), ),
                        one=True,
                        return_data=True,
                        decode=True
                    )  # type: ignore

                if not fileinfo.verify(fileinfo.digest, self.args) and not self.args.force: # type: ignore
                    if self.args.debug or self.args.verbose:
                        print(f"Calculated Digest: {fileinfo.calculate_hash()}")
                        print(f"Recorded Hash: {fileinfo.digest}")
                    raise ValueError("The digest in the database does not match the calculated digest for the data.")

                self.extract_file(fileinfo, outputdir)
            except sqlite3.DatabaseError:
                print("failed")

                if self.args.debug:
                    raise
                else:
                    row = cursor.fetchone() # type: ignore
                    continue

            row = cursor.fetchone()  # type: ignore  # Normal end of loop

    def compact(self):
        print("* Compacting the database, this might take a while...",
              end='... ',
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
