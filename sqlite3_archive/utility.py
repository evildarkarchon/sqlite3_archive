from __future__ import annotations

import glob
import json
import pathlib
import sqlite3
import sys
import xmltodict
from argparse import Namespace
from typing import Any, Iterable, List, Tuple, Union


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
               out: str = None,
               pop: bool = False) -> Union[str, None]:
    if mode == "add":
        base: pathlib.Path = pathlib.Path(files[0]).resolve()

    if not base.exists():
        return None

    f: str = str()
    if mode == "add" and base.is_file():
        f = cleantablename(base.parent.name, lower)
    elif mode == "add" and base.is_dir():
        f = cleantablename(base.name, lower)

    if mode == "extract":
        if files[0]:
            if files[0] and not out:
                f = cleantablename(pathlib.Path(files[0]).stem)
                if pop:
                    files.pop(0)
            elif out and not files[0]:
                f = cleantablename(pathlib.Path(out).name)

    if f:
        return f
    else:
        return None


def globlist(listglob: List, mode: str):
    if mode == "extract":
        yield from listglob
    elif mode == "add":
        for a in listglob:
            objtype = type(a)

            if objtype is str and "*" in a:
                yield from map(pathlib.Path, glob.glob(a, recursive=True))
            elif objtype is pathlib.Path and a.is_file(
            ) or objtype is str and pathlib.Path(a).is_file():
                if objtype is str:
                    yield pathlib.Path(a)
                elif objtype is pathlib.Path:
                    yield a
                else:
                    continue
            elif objtype is str and "*" not in a and pathlib.Path(a).is_file():
                yield pathlib.Path(a)
            elif objtype is str and "*" not in a and pathlib.Path(
                    a).is_dir() or objtype is pathlib.Path and a.is_dir():
                yield from pathlib.Path(a).rglob("*")
            else:
                yield from map(pathlib.Path, glob.glob(a, recursive=True))


def duplist(dups: dict, dbname: str, outfile: str, hide: bool,
            currentdb: bool):
    if len(dups[dbname]) == 0:
            dups.pop(dbname)
    else:
        keylist: List = list(dups.keys())
        dupsexist: bool = False
        for i in keylist:
            if len(dups[i]) >= 1:
                dupsexist = True
                break
        if not hide and dupsexist:
            if currentdb and dbname in keylist:
                print(f"Duplicate Files:\n {xmltodict.unparse(dups[dbname], pretty=True)}")
            elif not currentdb:
                print(f"Duplicate files:\n {xmltodict.unparse(dups, pretty=True)}")
        if outfile and dupsexist:
            dupspath: pathlib.Path = pathlib.Path(outfile)
            dupspath.write_text(xmltodict.unparse(dups, pretty=True))


def calcname(inpath: pathlib.Path, verbose: bool) -> str:
    parents: List = sorted(inpath.parents)
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


class DBUtility:
    def __init__(self, args: Namespace):
        def createdb() -> sqlite3.Connection:
            dbcon: Union[sqlite3.Connection, None] = None

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
                          values: Iterable[Any] = None,
                          one: bool = False,
                          raw: bool = False,
                          returndata = False,
                          decode: bool = False
                          ) -> Union[List[Any], sqlite3.Cursor, None]:
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

    def execquerycommit(self, query: str, values: Iterable[Any] = None):
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
                              returndata = False,
                              decode: bool = False
                              ) -> Union[List[Any], sqlite3.Cursor, None]:
        output: Any = self.dbcon.cursor()
        returnlist = ("select", "SELECT", "Select")
        if (any(i in query for i in returnlist)
                and returndata is False) or returndata is True:
            output = output.executemany(query, values)

            if one:
                _out = output.fetchone()[0]
                if type(_out) is bytes and decode:
                    _out = _out.decode(
                        sys.stdout.encoding
                    ) if sys.stdout.encoding else _out.decode("utf-8")
                print(output, flush=True)
                return _out
            elif raw:
                print(output, flush=True)
                return output
            else:
                print(output, flush=True)
                return output.fetchall()
        else:
            output.executemany(output, values)
        return None

    def set_journal_and_av(self, args: Namespace):
        if args.debug:
            print("function run")
        journal_mode = self.execquerynocommit("PRAGMA journal_mode;",
                                              one=True,
                                              returndata=True)

        def setwal() -> Union[bool, None]:
            if args.debug:
                print("wal run")
            try:
                self.execquerynocommit("PRAGMA journal_mode=wal;")
                new_journal_mode = self.execquerynocommit(
                    "PRAGMA journal_mode;", one=True, returndata=True)
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
            return None

        def setdel() -> Union[bool, None]:
            try:
                self.execquerynocommit("PRAGMA journal_mode=delete;")
                new_journal_mode = self.execquerynocommit(
                    "PRAGMA journal_mode;", one=True, returndata=True)
                if new_journal_mode != journal_mode:
                    return True
                else:
                    return False
            except sqlite3.DatabaseError:
                return False
            return None

        def setav() -> bool:
            avstate = self.execquerynocommit("PRAGMA auto_vacuum;",
                                             one=True,
                                             returndata=True)
            avstate2 = None
            notchanged = "autovacuum mode not changed"
            if args.verbose or args.debug:
                print(f"current autovacuum mode: {avstate}")
            if args.autovacuum and args.autovacuum == 1 and not avstate == 1:
                self.execquerynocommit("PRAGMA auto_vacuum = 1")
                avstate2 = self.execquerynocommit("PRAGMA auto_vacuum;",
                                                  one=True,
                                                  returndata=True)
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
                avstate2 = self.execquerynocommit("PRAGMA auto_vacuum;",
                                                  one=True,
                                                  returndata=True)
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
                avstate2 = self.execquerynocommit("PRAGMA_auto_vacuum;",
                                                  one=True,
                                                  returndata=True)
                if not args.mode == "compact" and avstate2 == 0:
                    return True
                else:
                    if avstate2 != 0 and avstate != 0 and args.verbose or args.debug:
                        print(notchanged)
                    return False
                if args.verbose or args.debug:
                    print("auto_vacuum disabled")
            return False

        needsvacuum: Union[bool, None] = False
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
