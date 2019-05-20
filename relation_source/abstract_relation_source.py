from abc import ABC, abstractmethod

from util.utils import measure


class AbstractRelationSource(ABC):

    def relations_for(self, entities):
        return measure(f"Retrieving relations for {len(entities)} entities", self._retrieve_relations_for, entities)

    @abstractmethod
    def _retrieve_relations_for(self, entities):
        raise NotImplementedError

    @abstractmethod
    def chunk_size(self):
        raise NotImplementedError()
