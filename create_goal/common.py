from enum import Enum
from typing import Tuple, List

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


class InteractionKind(Enum):
    READ = "read"
    WRITE = "write"
    MOUNT = "mount"
