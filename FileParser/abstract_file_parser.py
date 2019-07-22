from abc import ABC


class AbstractFileParser(ABC):

    def __init__(self, file_path):
        self._file_path = file_path
