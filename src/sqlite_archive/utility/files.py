import glob
import json
import pathlib

from typing import Any, AnyStr, Generator, Iterable, List, Tuple, Union

def globlist(listglob: Union[Iterable, AnyStr]) -> Generator:
    if isinstance(listglob, str):
        listglob = [listglob]
    listglob = list(set(listglob))
    for a in listglob:
        if isinstance(a, str) and "*" in a:
            yield from [pathlib.Path(i) for i in glob.glob(a, recursive=True) if pathlib.Path(i).is_file()]
        elif pathlib.Path(a).is_dir():
            yield from [pathlib.Path(i) for i in pathlib.Path(a).rglob("*") if pathlib.Path(i).is_file()]
        elif pathlib.Path(a).is_file():
            yield pathlib.Path(a)

def duplist(dups: dict, dbname: str, outfile: str, hide: bool,
            currentdb: bool):
    keylist: List = list(dups.keys())
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

__all__ = ["globlist", "duplist", "calcname"]