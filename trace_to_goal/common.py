from typing import Tuple, List
import tempfile
import time
from pathlib import Path

DEFAULT_DUMP_DIR: str = str((
    Path(tempfile.gettempdir()) / "trace2goal" / f"exec_{round(time.time())}"
).absolute())

Addr = int
Id = int
SliceId = Id
BssId = Id
CcsId = Id
# Address range inclusive start, exclusive end
SliceRange = Tuple[Addr, Addr]
# SliceId to corresponding address range mapping
SliceMap = List[SliceRange]
# SliceId to corresponding CCS mapping
SliceResponsibility = List[CcsId]
# CcsId to corresponding BssIds mapping
BssResponsibility = List[List[BssId]]
