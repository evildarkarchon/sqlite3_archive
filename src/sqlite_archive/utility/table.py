import pathlib

from typing import Union

def cleantablename(instring: str, lower: bool = False) -> str:
    out: str = instring.replace(".", "_").replace(
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

__all__ = ["cleantablename", "infertable"]