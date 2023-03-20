import hashlib
import pathlib
from argparse import Namespace
from dataclasses import dataclass
from sqlite3 import Cursor


@dataclass
class FileInfo:
    name: str | Cursor = ''
    data: bytes = b''
    digest: str | None = ''

    def __post_init__(self):
        path: pathlib.Path | None = None
        if self.name:
            if isinstance(self.name, Cursor):
                self.name = self.name.fetchone()[0]
            else:
                self.name = str(self.name)
            path = pathlib.Path(self.name)  # type: ignore
            if path.exists():
                path = path.resolve()
        if path and path.is_file() and not self.data:
            self.data = path.read_bytes()
        if self.data and not self.digest:
            self.digest = self.calculatehash()

    def calculatehash(self) -> str | None:
        if self.data:
            filehash = hashlib.sha512()
            filehash.update(self.data)
            return filehash.hexdigest()
        else:
            return None

    def verify(self, refhash: str, args: Namespace) -> bool | None:
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
