import pathlib
# import copy

def parents(path: pathlib.Path, num: int):
    out = None
    while num > 0:
        out = path.parent
        num -= 1
    return out