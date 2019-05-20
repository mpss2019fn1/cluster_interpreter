import threading
from abc import ABC, abstractmethod


class AbstractRelationSink(ABC):

    def __init__(self):
        self._lock = threading.Lock()

    def persist(self, relations):
        with self._lock:
            self._perform_persist(relations)

    @abstractmethod
    def _perform_persist(self, relations):
        raise NotImplementedError()
