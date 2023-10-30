from typing import List, Optional, Callable


class RankBuilder:
    rank_id: int
    get_new_label: Callable[[Optional[str]], str]

    _lines: List[str]

    def __init__(self, rank_id: int,
                 get_new_label: Callable[[Optional[str]], str]):
        self.rank_id = rank_id
        self.get_new_label = get_new_label
        self._lines = [f"rank {self.rank_id} {{"]
        # + '\n'.join(self._lines) + "}"

    def add_send(self, len: int, to_rank: int,
                 tag: Optional[int] = None) -> str:
        label = self.get_new_label('s')

        line = f"{label}: send {len}b to {to_rank}" + \
            (f" tag {tag}" if tag else "")

        self._lines.append(line)
        return label

    def add_recv(self, len: int, from_rank: int,
                 tag: Optional[int] = None) -> str:
        label = self.get_new_label('r')

        line = f"{label}: recv {len}b from {from_rank}" + \
            (f" tag {tag}" if tag else "")

        self._lines.append(line)
        return label

    def add_calc(self, time: int) -> str:
        label = self.get_new_label('c')
        line = f"{label}: calc {time}"
        self._lines.append(line)
        return label

    def add_comment(self, comment: str):
        # Escape new lines
        comment = comment.replace('\n', '\n// ')
        self._lines.append(f"// {comment}")

    def require_dependency(self, label0: str, label1: str):
        line = f"{label0} requires {label1}"
        self._lines.append(line)

    def serialize(self):
        self._lines.append("}")
        result = '\n'.join(self._lines)
        self._lines.pop()
        return result
