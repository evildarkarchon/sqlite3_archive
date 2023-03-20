from __future__ import annotations

import glob
import json
import pathlib
import sqlite3
import sys
from argparse import Namespace
from typing import Any, AnyStr, Generator, Iterable


def cleantablename(instring: str, lower: bool = False) -> str:
    out = instring.replace(".", "_").replace(
        ' ', '_').replace("'", '_').replace(",", "").replace("/", '_').replace(
            '\\', '_').replace('-', '_').replace('#', '')
    if lower:
        return out.lower()
    else:
        return out


def infertable(mode: str,
               lower: bool,
               files: list,
               out: str | None = None,
               pop: bool = False) -> str | None:
    base: pathlib.Path | None = None
    if mode == "add":
        base = pathlib.Path(files[0]).resolve()

    if not base.exists():  # type: ignore
        return None

    f: str = str()
    if mode == "add" and base.is_file():  # type: ignore
        f = cleantablename(base.parent.name, lower)  # type: ignore
    elif mode == "add" and base.is_dir():  # type: ignore
        f = cleantablename(base.name, lower)  # type: ignore

    if mode == "extract":
        if out:
            f = cleantablename(pathlib.Path(out).name)

        if files[0] and not out:
            f = cleantablename(pathlib.Path(files[0]).stem)
            if pop:
                files.pop(0)
    if f:
        return f
    else:
        return None


def globlist(listglob: Iterable | AnyStr) -> Generator:
    if isinstance(listglob, str) and "*" not in listglob:
        listglob = [listglob]
    listglob = list(set(listglob))
    for a in listglob:
        if isinstance(a, str) and "*" in a:  # type: ignore
            yield from [pathlib.Path(i) for i in glob.glob(a, recursive=True) if pathlib.Path(i).is_file()]
        elif pathlib.Path(a).is_dir():  # type: ignore
            yield from [pathlib.Path(i) for i in pathlib.Path(a).rglob("*") if pathlib.Path(i).is_file()]  # type: ignore
        elif pathlib.Path(a).is_file():  # type: ignore
            yield pathlib.Path(a)  # type: ignore


def duplist(dups: dict, dbname: str, outfile: str, hide: bool,
            currentdb: bool):
    keylist: list = list(dups.keys())
    dupsexist: bool = False
    for i in keylist:
        if len(dups[i]) >= 1:
            dupsexist = True
    if not hide and dupsexist:
        if currentdb:
            try:
                print(f"Duplicate Files:\n {json.dumps(dups[dbname], indent=4)}")
            except KeyError:
                pass
        else:
            print(f"Duplicate files:\n {json.dumps(dups, indent=4)}")
    if outfile and dupsexist:
        dupspath: pathlib.Path = pathlib.Path(outfile)
        dupspath.write_text(json.dumps(dups, indent=4))


def calcname(inpath: pathlib.Path, verbose: bool = False) -> str:
    parents: list = sorted(inpath.parents)
    parentslen: int = len(parents)
    if verbose:
        print(parents)

    def oldbehavior() -> str:
        if verbose:
            print("Using old name calculation behavior")
        if parentslen > 2:
            return str(inpath.relative_to(inpath.parent.parent))
        else:
            return str(inpath.relative_to(inpath.parent))

    try:
        if parentslen == 1:
            return str(inpath.relative_to(parents[0]))
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

    def execquerynocommit(self,
                          query: str,
                          values: Iterable[Any] | None = None,
                          one: bool = False,
                          raw: bool = False,
                          returndata=False,
                          decode: bool = False
                          ) -> list[Any] | sqlite3.Cursor | None:
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")

        output: Any = None

        if returndata is True:
            if values:
                output = self.dbcon.execute(query, values)  # type: ignore
            else:
                output = self.dbcon.execute(query)

            if one:
                out = output.fetchone()[0]
                if type(out) is bytes and decode:
                    out = out.decode(sys.stdout.encoding) if sys.stdout.encoding else out.decode("utf-8")
                return out  # type: ignore
            elif raw:
                return output
            else:
                return output.fetchall()
        else:
            if values:
                self.dbcon.execute(query, values)  # type: ignore
            else:
                self.dbcon.execute(query)
            return None

    def execquerycommit(self, query: str, values: Iterable[Any] = None):  # type: ignore
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")
        if values:
            try:
                self.dbcon.execute(query, values)  # type: ignore
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

    def execmanycommit(self, query: str, values: Iterable[Any]):
        if values and type(values) not in (list, tuple):
            raise TypeError("Values argument must be a list or tuple.")

        try:
            self.dbcon.executemany(query, values)
        except Exception:
            raise
        else:
            self.dbcon.commit()

    def execquerymanynocommit(self,
                              query: str,
                              values: Iterable[Any],
                              one: bool = False,
                              raw: bool = False,
                              returndata: bool = False,
                              decode: bool = False) -> list[Any]:
        cursor = self.dbcon.cursor()
        cursor.executemany(query, values)
        if cursor.description is None:
            return []
        if returndata or cursor.description[0][0].lower() == "select":
            rows = cursor.fetchall()
            if one:
                row = rows[0][0]
                if isinstance(row, bytes) and decode:
                    row = row.decode(sys.stdout.encoding or "utf-8")
                return [row]
            elif raw:
                return rows
            else:
                return [row[0] for row in rows]
        else:
            return []

    def set_journal_and_av(self, args: Namespace):
        if args.debug:
            print("function run")
        journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)

        def setwal() -> bool | None:
            if args.debug or args.verbose:
                print("wal run")
            try:
                self.execquerynocommit("PRAGMA journal_mode=wal;")
                new_journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)
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

        def setdel() -> bool | None:
            try:
                self.execquerynocommit("PRAGMA journal_mode=delete;")
                new_journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)
                if new_journal_mode != journal_mode:
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
            if args.autovacuum and args.autovacuum == 1 and not avstate == 1:
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
            elif args.autovacuum and args.autovacuum == 2 and not avstate == 2:
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
            elif args.autovacuum and args.autovacuum == 0 and not avstate == 0:
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
            return False

        needsvacuum: bool | None = False
        if "autovacuum" in args and args.autovacuum:
            needsvacuum = setav()

        wal = ("WAL", "wal", "Wal", "WAl")
        rollback = ("delete", "Delete", "DELETE")

        if "wal" in args and args.wal and journal_mode not in wal:
            needsvacuum = setwal()
        elif "rollback" in args and args.rollback and journal_mode not in rollback:
            needsvacuum = setdel()

        if needsvacuum:
            self.execquerynocommit("VACUUM;")


__all__ = ["cleantablename", "infertable", "globlist", "duplist", "calcname", "DBUtility"]
