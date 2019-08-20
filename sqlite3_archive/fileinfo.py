from __future__ import annotations

import hashlib
import pathlib
from argparse import Namespace
from dataclasses import dataclass

@dataclass
class FileInfo:
    name: str = ''
    data: bytes = b''
    digest: str = ''

    def __post_init__(self):
        name = None
        if self.name:
            name = pathlib.Path(self.name)
        if name and name.resolve().is_file() and not self.data:
            self.data = name.resolve().read_bytes()
        if self.data and not self.digest:
            self.digest = self.calculatehash()

    def calculatehash(self):
        if self.data:
            filehash = hashlib.sha512()
            filehash.update(self.data)
            return filehash.hexdigest()
        else:
            return None

    def verify(self, refhash: str, args: Namespace):
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