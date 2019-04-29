import argparse
import os


class AccessibleTextFile(argparse.Action):

    def __call__(self, parser, parser_namespace, values, option_string=None):
        path = os.path.abspath(os.path.expanduser(values))

        if not os.path.isfile(path):
            raise argparse.ArgumentError(self, "{0} is not a valid file".format(path))

        if not os.access(path, os.R_OK):
            raise argparse.ArgumentError(self, "Permission denied to read from {0}".format(path))

        setattr(parser_namespace, self.dest, path)


class AccessibleDirectory(argparse.Action):

    def __call__(self, parser, parser_namespace, values, option_string=None):
        path = os.path.abspath(os.path.expanduser(values))

        if not os.path.isdir(path):
            raise argparse.ArgumentError(self, "{0} is not a valid directory".format(path))

        if not os.access(path, os.R_OK):
            raise argparse.ArgumentError(self, "Permission denied to read from {0}".format(path))

        setattr(parser_namespace, self.dest, path)
