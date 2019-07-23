from abc import ABC, abstractmethod
from typing import List

from Relation.relation import Relation
from util.utils import measure


class AbstractRelationSource(ABC):

    def relations_for(self, entities) -> List[Relation]:
        return measure(f"Retrieving relations for {len(entities)} entities", self._retrieve_relations_for, entities)

    @abstractmethod
    def _retrieve_relations_for(self, entities) -> List[Relation]:
        raise NotImplementedError

    @abstractmethod
    def chunk_size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError
