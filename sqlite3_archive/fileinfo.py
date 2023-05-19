import hashlib
import pathlib
from dataclasses import dataclass
from sqlite3 import Cursor
from typing import Optional, Union

from sqlite_archive import Args


@dataclass
class FileInfo:
    name: Union[str, Cursor] = ''
    data: bytes = b''
    digest: Optional[str] = None

    def __post_init__(self):
        self.name = self._resolve_name()
        path = self._resolve_path()
        if not self.data and path:
            self.data = self._read_data_from_path(path)
        if self.data and not self.digest:
            self.digest = self.calculate_hash()

    def _resolve_name(self) -> str:
        if self.name:
            if isinstance(self.name, Cursor):
                self.name = self.name.fetchone()[0]
            else:
                self.name = str(self.name)
        return self.name

    def _resolve_path(self) -> Optional[pathlib.Path]:
        if not self.name:
            return None
        if isinstance(self.name, Cursor):
            self.name = self.name.fetchone()[0]
        path = pathlib.Path(self.name)
        if path.exists():
            return path.resolve()
        return None

    def _read_data_from_path(self, path: pathlib.Path) -> bytes:
        if path.is_file():
            return path.read_bytes()
        return b''

    def calculate_hash(self) -> Optional[str]:
        if self.data:
            file_hash = hashlib.sha512()
            file_hash.update(self.data)
            return file_hash.hexdigest()
        return None

    def verify(self, refhash: str, args: Args) -> Optional[bool]:
        calc_hash = self.calculate_hash()
        self._print_verification_message(args, calc_hash, refhash)

        if calc_hash == refhash:
            return True
        elif calc_hash != refhash:
            return False
        return None

    def _print_verification_message(self, args: Args, calc_hash: Optional[str], refhash: str):
        if args.debug or args.verbose:
            status = "pass" if calc_hash == refhash else "failed"
            print(f"* Verifying digest for {self.name}... {status}", flush=True)
