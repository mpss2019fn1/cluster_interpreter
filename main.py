import argparse
import logging
from pathlib import Path
from typing import Iterable

from Cluster.cluster import Cluster
from EntityLinking.entity_linkings import EntityLinkings
from FileParser.ClusterFileParser.cluster_file_parser import ClusterFileParser
from FileParser.EntityLinkingFileParsing.entity_linking_file_parser import EntityLinkingFileParser
from util.filesystem_validators import WriteableDirectory, ReadableFile


def main():
    logging.basicConfig(format='%(asctime)s : [%(threadName)s] %(levelname)s : %(message)s', level=logging.INFO)
    parser: argparse.ArgumentParser = _initialize_parser()

    args = parser.parse_args()

    entity_linkings: EntityLinkings = EntityLinkingFileParser.create_from_file(args.linkings)
    clusters: Iterable[Cluster] = ClusterFileParser.create_from_file(args.clusters)


def _initialize_parser():
    general_parser = argparse.ArgumentParser(
        description='Enrich clusters with further information by utilizing external sources')
    general_parser.add_argument("--clusters", help='CSV file containing clustered entities', action=ReadableFile,
                                required=True, type=Path)
    general_parser.add_argument("--linkings", help='CSV file containing entity to wikidata linkings',
                                action=ReadableFile, required=True, type=Path)
    general_parser.add_argument("--output", help='Location for enriched clusters', action=WriteableDirectory,
                                required=True, type=Path)
    general_parser.add_argument("--threads", help='Number of threads', type=int, required=False, default=8)
    return general_parser


if __name__ == "__main__":
    main()
