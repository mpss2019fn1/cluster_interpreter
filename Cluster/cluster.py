from typing import List


class Cluster:

    def __init__(self, id_: int):
        self.id: int = id_
        self.entities: List[str] = []
