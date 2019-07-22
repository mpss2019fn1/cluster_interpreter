import argparse
import os
from pathlib import Path
from typing import Optional


class ReadableFile(argparse.Action):

    def __call__(self, parser: argparse.ArgumentParser, parser_namespace: object, values: Path,
                 option_string: Optional[str] = None) -> None:
        if not values.is_file():
            raise argparse.ArgumentError(self, "{0} is not a valid file".format(values.absolute()))

        if not os.access(str(values.absolute()), os.R_OK):
            raise argparse.ArgumentError(self, "Permission denied to read from {0}".format(values.absolute()))

        setattr(parser_namespace, self.dest, values)


class WriteableDirectory(argparse.Action):

    def __call__(self, parser: argparse.ArgumentParser, parser_namespace: object, values: Path,
                 option_string: Optional[str] = None) -> None:
        if not values.is_dir():
            raise argparse.ArgumentError(self, "{0} is not a valid directory".format(values.absolute()))

        if not os.access(str(values.absolute()), os.W_OK):
            raise argparse.ArgumentError(self, "Permission denied to write to {0}".format(values.absolute()))

        setattr(parser_namespace, self.dest, values)
