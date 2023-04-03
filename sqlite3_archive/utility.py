from __future__ import annotations

import glob
import json
import pathlib
import sqlite3
import sys
from argparse import Namespace
from typing import Any, AnyStr, Generator, Iterable, Union, Optional, List, Dict


def clean_table_name(instring: str, lower: bool = False) -> str:
    replacements = {
        ".": "_",
        " ": "_",
        "'": "_",
        ",": "",
        "/": "_",
        "\\": "_",
        "-": "_",
        "#": "",
    }

    out = instring

    for old_char, new_char in replacements.items():
        out = out.replace(old_char, new_char)

    return out.lower() if lower else out


def infer_table(mode: str,
                lower: bool,
                files: List[str],
                out: Optional[str] = None,
                pop: bool = False) -> Optional[str]:
    def clean_table_name(name: str, lower_case: bool = False) -> str:
        cleaned_name = name.replace(" ", "_").replace("-", "_")
        return cleaned_name.lower() if lower_case else cleaned_name

    base = None
    if mode == "add":
        base = pathlib.Path(files[0]).resolve()

    if not base or not base.exists():
        return None

    table_name = ""
    if mode == "add":
        if base.is_file():
            table_name = clean_table_name(base.parent.name, lower)
        elif base.is_dir():
            table_name = clean_table_name(base.name, lower)
    elif mode == "extract":
        if out:
            table_name = clean_table_name(pathlib.Path(out).name)
        elif files[0] and not out:
            table_name = clean_table_name(pathlib.Path(files[0]).stem)
            if pop:
                files.pop(0)

    return table_name if table_name else None


def glob_list(input_paths: Union[Iterable, str]) -> Generator[pathlib.Path, None, None]:
    if isinstance(input_paths, str) and "*" not in input_paths:
        input_paths = [input_paths]

    unique_paths = list(set(input_paths))

    for path in unique_paths:
        path_obj = pathlib.Path(path)

        if isinstance(path, str) and "*" in path:
            yield from (pathlib.Path(matched_path) for matched_path in glob.glob(path, recursive=True) if path_obj.is_file())
        elif path_obj.is_dir():
            yield from (pathlib.Path(file_path) for file_path in path_obj.rglob("*") if path_obj.is_file())
        elif path_obj.is_file():
            yield path_obj


def print_duplicates(dups: Dict, dbname: str, currentdb: bool) -> None:
    if currentdb:
        try:
            print(f"Duplicate Files:\n {json.dumps(dups[dbname], indent=4)}")
        except KeyError:
            pass
    else:
        print(f"Duplicate files:\n {json.dumps(dups, indent=4)}")


def write_duplicates_to_file(dups: Dict, outfile: str) -> None:
    dups_path = pathlib.Path(outfile)
    dups_path.write_text(json.dumps(dups, indent=4))


def process_duplicates(dups: Dict, dbname: str, outfile: str, hide: bool, currentdb: bool) -> None:
    duplicates_exist = any(len(dups[key]) >= 1 for key in dups.keys())

    if not hide and duplicates_exist:
        print_duplicates(dups, dbname, currentdb)

    if outfile and duplicates_exist:
        write_duplicates_to_file(dups, outfile)


def calc_name(inpath: pathlib.Path, verbose: bool = False) -> str:
    parents = sorted(inpath.parents)
    parents_len = len(parents)

    if verbose:
        print(parents)

    def old_behavior() -> str:
        if verbose:
            print("Using old name calculation behavior")
        if parents_len > 2:
            return str(inpath.relative_to(inpath.parent.parent))
        else:
            return str(inpath.relative_to(inpath.parent))

    def get_name_for_single_parent() -> str:
        return str(inpath.relative_to(parents[0]))

    def get_name_for_absolute_path() -> str:
        return str(inpath.resolve().relative_to(pathlib.Path.cwd()))

    def get_name_for_relative_path() -> str:
        return str(inpath.relative_to(parents[1]))

    try:
        if parents_len == 1:
            return get_name_for_single_parent()
        elif inpath.is_absolute() and str(pathlib.Path.cwd()) in str(inpath):
            return get_name_for_absolute_path()
        elif not inpath.is_absolute() and parents_len > 1:
            return get_name_for_relative_path()
        else:
            return old_behavior()
    except (ValueError, IndexError):
        try:
            return old_behavior()
        except Exception:
            raise


class DBUtility:
    def __init__(self, args: Namespace):
        def createdb() -> sqlite3.Connection:
            dbcon: sqlite3.Connection | None = None

            if self.db.is_file():
                dbcon = sqlite3.Connection(self.db)
            else:
                self.db.touch()
                dbcon = sqlite3.Connection(self.db)

            return dbcon

        self.db: pathlib.Path = pathlib.Path(args.db)
        if self.db.exists():
            self.db = self.db.resolve()
        self.dbcon: sqlite3.Connection = createdb()

    def exec_query_no_commit(
        self,
        query: str,
        values: Optional[Iterable[Any]] = None,
        one: bool = False,
        raw: bool = False,
        return_data: bool = False,
        decode: bool = False,
    ) -> Union[list[Any], sqlite3.Cursor, None]:
        def _validate_values() -> None:
            if values and not isinstance(values, (list, tuple)):
                raise TypeError("Values argument must be a list or tuple.")

        def _execute_query() -> sqlite3.Cursor:
            return self.dbcon.execute(query, values) if values else self.dbcon.execute(query) # type: ignore

        def _decode_output(output: Any) -> Any:
            return (
                output.decode(sys.stdout.encoding)
                if sys.stdout.encoding
                else output.decode("utf-8")
            )

        def _get_one_result(cursor: sqlite3.Cursor) -> Any:
            result = cursor.fetchone()[0]
            return _decode_output(result) if isinstance(result, bytes) and decode else result

        _validate_values()
        cursor = _execute_query()

        if not return_data:
            return None

        if one:
            return _get_one_result(cursor)
        elif raw:
            return cursor
        else:
            return cursor.fetchall()

    def exec_query_commit(self, query: str, values: Optional[Iterable[Any]] = None) -> None:
        self._validate_values(values) # type: ignore

        try:
            if values:
                self.dbcon.execute(query, values) # type: ignore
            else:
                self.dbcon.execute(query)
        except Exception:
            raise
        else:
            self.dbcon.commit()

    def _validate_values(self, values: Iterable[Any]) -> None:
        if values and not isinstance(values, (list, tuple)):
            raise TypeError("Values argument must be a list or tuple.")

    def exec_many_commit(self, query: str, values: Iterable[Any]) -> None:
        self._validate_values(values)

        try:
            self.dbcon.executemany(query, values)
        except Exception:
            raise
        else:
            self.dbcon.commit()

    def exec_query_many_no_commit(
        self,
        query: str,
        values: Iterable[Any],
        one: bool = False,
        raw: bool = False,
        return_data: bool = False,
        decode: bool = False,
    ) -> List[Any]:
        self._validate_values(values)

        cursor = self.dbcon.cursor()
        cursor.executemany(query, values)

        if cursor.description is None:
            return []

        if return_data or cursor.description[0][0].lower() == "select":
            rows = cursor.fetchall()

            if one:
                row = rows[0][0]
                return [row.decode(sys.stdout.encoding or "utf-8")] if isinstance(row, bytes) and decode else [row]

            if raw:
                return rows

            return [row[0] for row in rows]

        return []
    
    def _get_current_av_state(self) -> Optional[int]:
        return self.exec_query_no_commit("PRAGMA auto_vacuum;", one=True, return_data=True) # type: ignore

    def _set_av_state(self, av_state: int):
        self.exec_query_no_commit(f"PRAGMA auto_vacuum = {av_state};")

    def _av_state_changed(self, old_state: Optional[int], new_state: Optional[int], expected_state: int) -> bool:
        return old_state != expected_state and new_state == expected_state

    def _print_current_av_state(self, args: Namespace, av_state: Optional[int]):
        if args.verbose or args.debug:
            print(f"Current autovacuum mode: {av_state}")

    def _print_av_state_changed(self, args: Namespace, new_av_state: Optional[int]):
        if args.verbose or args.debug:
            av_mode = {
                0: "disabled",
                1: "full",
                2: "incremental",
            }.get(new_av_state, "unknown") # type: ignore
            print(f"{av_mode} auto_vacuum")

    def _print_av_state_not_changed(self, args: Namespace, av_state: Optional[int]):
        if args.verbose or args.debug:
            print("Autovacuum mode not changed")

    def set_journal_and_av(self, args: Namespace):
        if args.debug:
            print("function run")
        journal_mode = self.exec_query_no_commit("PRAGMA journal_mode;", one=True, return_data=True)

        def setwal() -> bool | None:
            if args.debug or args.verbose:
                print("wal run")
            try:
                self.exec_query_no_commit("PRAGMA journal_mode=wal;")
                new_journal_mode = self.exec_query_no_commit("PRAGMA journal_mode;", one=True, return_data=True)
                if args.verbose or args.debug:
                    print(journal_mode)
                    print(new_journal_mode)
                if new_journal_mode != journal_mode:
                    return True
                else:
                    return False
            except sqlite3.DatabaseError:
                print("something went wrong.")
                return False

        def set_del(self) -> Optional[bool]:
            def _set_journal_mode(mode: str) -> Optional[str]:
                try:
                    self.exec_query_no_commit(f"PRAGMA journal_mode={mode};")
                    return self.exec_query_no_commit("PRAGMA journal_mode;", one=True, return_data=True)
                except sqlite3.DatabaseError:
                    return None

            journal_mode = "delete"
            new_journal_mode = _set_journal_mode(journal_mode)

            if new_journal_mode is None:
                return False
            return new_journal_mode != journal_mode

        def set_av(self, args: Namespace) -> bool:
            current_av_state = self._get_current_av_state()
            self._print_current_av_state(args, current_av_state)

            if args.autovacuum is not None:
                self._set_av_state(args.autovacuum)
                new_av_state = self._get_current_av_state()

                if self._av_state_changed(current_av_state, new_av_state, args.autovacuum):
                    self._print_av_state_changed(args, new_av_state)
                    return not (args.mode == "compact")
                else:
                    self._print_av_state_not_changed(args, current_av_state)

            return False
        
        needsvacuum: bool | None = False
        if "autovacuum" in args and args.autovacuum:
            needsvacuum = set_av(self, args)

        wal = ("WAL", "wal", "Wal", "WAl")
        rollback = ("delete", "Delete", "DELETE")

        if "wal" in args and args.wal and journal_mode not in wal:
            needsvacuum = setwal()
        elif "rollback" in args and args.rollback and journal_mode not in rollback:
            needsvacuum = set_del(self)

        if needsvacuum:
            self.exec_query_no_commit("VACUUM;")
