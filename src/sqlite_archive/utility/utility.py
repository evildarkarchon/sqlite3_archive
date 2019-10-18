from __future__ import annotations

import pathlib
import sqlite3
import sys
from argparse import Namespace
from typing import Any, Iterable, List, Tuple, Union


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
                          values: Union[Iterable[Any], str] = None,
                          one: bool = False,
                          raw: bool = False,
                          returndata = False,
                          decode: bool = False
                          ) -> Union[List[Any], sqlite3.Cursor, None]:
        if values and type(values) not in (list, tuple):
            if type(values) is str:
                values = (values,)
            else:
                raise TypeError("Values argument must be a list or tuple.")

        output: Any = None

        if returndata is True:
            if values:
                output = self.dbcon.execute(query, values)
            else:
                output = self.dbcon.execute(query)

            if one:
                out = output.fetchone()[0]
                if type(out) is bytes and decode:
                    out = out.decode(sys.stdout.encoding) if sys.stdout.encoding else out.decode("utf-8")
                return out
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
            if type(values) is str:
                values = (values,)
            else:
                raise TypeError("Values argument must be a list or tuple.")
        if values:
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

    def execmanycommit(self, query: str, values: Union[Iterable[Any], str]):
        if values and type(values) not in (list, tuple):
            if type(values) is str:
                values = (values,)
            else:
                raise TypeError("Values argument must be a list or tuple.")

        try:
            self.dbcon.executemany(query, values)
        except Exception:
            raise
        else:
            self.dbcon.commit()

    def execquerymanynocommit(self,
                              query: str,
                              values: Union[Iterable[Any], str],
                              one: bool = False,
                              raw: bool = False,
                              returndata = False,
                              decode: bool = False
                              ) -> Union[List[Any], sqlite3.Cursor, None]:
        if values and type(values) not in (list, tuple):
            if type(values) is str:
                values = (values,)
            else:
                raise TypeError("Values argument must be a list or tuple.")
        output: Any = self.dbcon.cursor()
        returnlist = ("select", "SELECT", "Select")
        if (any(i in query for i in returnlist)
                and returndata is False) or returndata is True:
            output = output.executemany(query, values)

            if one:
                _out = output.fetchone()[0]
                if type(_out) is bytes and decode:
                    _out = _out.decode(sys.stdout.encoding) if sys.stdout.encoding else _out.decode("utf-8")
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
        journal_mode = self.execquerynocommit("PRAGMA journal_mode;", one=True, returndata=True)

        def setwal() -> Union[bool, None]:
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
            return None

        def setdel() -> Union[bool, None]:
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


__all__ = ["DBUtility"]
