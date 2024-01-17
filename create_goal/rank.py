from typing import List, Optional, Callable
from pathlib import Path
from io import TextIOWrapper


class RankBuilder:
    rank_id: int
    get_new_label: Callable[[Optional[str]], str]

    use_file: bool = False
    _lines: List[str] = []
    _lines_file: Optional[TextIOWrapper] = None
    _lines_file_path: Optional[Path] = None

    def __init__(self, rank_id: int,
                 get_new_label: Callable[[Optional[str]], str],
                 dump_dir: Optional[str]):
        self.rank_id = rank_id
        self.get_new_label = get_new_label
        if dump_dir is not None:
            self.use_file = True
            self._lines_file_path = (
                Path(dump_dir) / f"rank_{self.rank_id}.state").absolute()
            self._lines_file = open(self._lines_file_path, 'a')

        self.add_line(f"rank {self.rank_id} {{")

    def __del__(self):
        assert self._lines_file is not None, "unreachable"
        self._lines_file.close()

    def add_line(self, line):
        if not self.use_file:
            self._lines.append(line)
        else:
            assert self._lines_file, "Unreachable"
            self._lines_file.write(line + '\n')
            del line

    def add_send(self, len: int, to_rank: int,
                 tag: Optional[int] = None) -> str:
        label = self.get_new_label('s')

        line = f"{label}: send {len}b to {to_rank}" + \
            (f" tag {tag}" if tag else "")

        self.add_line(line)
        return label

    def add_recv(self, len: int, from_rank: int,
                 tag: Optional[int] = None) -> str:
        label = self.get_new_label('r')

        line = f"{label}: recv {len}b from {from_rank}" + \
            (f" tag {tag}" if tag else "")

        self.add_line(line)
        return label

    def add_calc(self, time: int) -> str:
        label = self.get_new_label('c')
        line = f"{label}: calc {time}"
        self.add_line(line)
        return label

    def add_comment(self, comment: str):
        # Escape new lines
        comment = comment.replace('\n', '\n// ')
        self.add_line(f"// {comment}")

    def require_dependency(self, label0: str, label1: str):
        line = f"{label0} requires {label1}"
        self.add_line(line)

    def serialize(self):
        if self.use_file:
            assert self._lines_file is not None, "unreachable - lines_file is None"
            self._lines_file.flush()
            assert self._lines_file_path is not None, "unreachable - lines_file_path is None"
            with open(self._lines_file_path, "r") as file:
                result = file.read()
        else:
            result = '\n'.join(self._lines)

        return result + "}"
