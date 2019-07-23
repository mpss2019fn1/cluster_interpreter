from typing import List


class Cluster:

    def __init__(self, id_: int):
        self.id: int = id_
        self.entities: List[str] = []

    def to_json_object(self) -> object:
        return {"id": self.id, "number_of_entities": len(self.entities)}
