import hashlib
import pathlib
from argparse import Namespace
from dataclasses import dataclass
from typing import Union


@dataclass
class FileInfo:
    name: str = ''
    data: bytes = b''
    digest: str = ''
    mtime: int = 0

    def __post_init__(self):
        path: Union[pathlib.Path, None] = None
        if self.name:
            path = pathlib.Path(self.name)
            if path.exists():
                path = path.resolve()
        if path and path.is_file():
            if not self.data:
                self.data = path.read_bytes()
            if not self.mtime:
                self.mtime = path.stat().st_mtime_ns
        if self.data and not self.digest:
            self.digest = self.calculatehash()

    def calculatehash(self) -> Union[str, None]:
        if self.data:
            filehash = hashlib.sha512()
            filehash.update(self.data)
            return filehash.hexdigest()
        else:
            return None

    def verify(self, refhash: str, args: Namespace) -> Union[bool, None]:
        calchash = self.calculatehash()
        if args.debug or args.verbose:
            print(f"* Verifying digest for {self.name}...",
                  end=' ',
                  flush=True)
        if calchash == refhash:
            if args.debug or args.verbose:
                print("pass", flush=True)
            return True
        elif calchash != refhash:
            if args.debug or args.verbose:
                print("failed", flush=True)
            return False
        return None


__all__ = ["FileInfo"]
