from abc import abstractmethod
from pathlib import Path

from relation_sink.abstract_relation_sink import AbstractRelationSink


class AbstractFileRelationSink(AbstractRelationSink):

    def __init__(self, file_path):
        super().__init__()
        self._file_path = Path(file_path)
        self._file_path.parent.mkdir(parents=True, exist_ok=True)

    def _perform_persist(self, relations):
        file_existed = self._file_path.exists()
        write_mode = "a" if self._file_path.exists() else "w+"

        with self._file_path.open(mode=write_mode) as sink:
            if not file_existed:
                print(*self._generate_file_headers(relations), sep="\n", end="\n", file=sink)
            else:
                print("\n", end="", file=sink)

            print(*self._generate_file_content(relations), sep="\n", end="", file=sink)

    @abstractmethod
    def _generate_file_headers(self, relations):
        raise NotImplementedError()

    @abstractmethod
    def _generate_file_content(self, relations):
        raise NotImplementedError()
