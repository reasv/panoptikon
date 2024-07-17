from dataclasses import dataclass


@dataclass
class FileScanData:
    sha256: str
    md5: str
    mime_type: str
    last_modified: str
    size: int
    path: str
    path_in_db: bool
    modified: bool


@dataclass
class ItemWithPath:
    sha256: str
    md5: str
    type: str
    size: int
    time_added: str
    path: str
