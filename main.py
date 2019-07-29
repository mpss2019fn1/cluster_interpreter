import argparse
import json
import logging
from pathlib import Path
from typing import Iterable, List

from Cluster.cluster import Cluster
from ClusterAnnotater.cluster_annotator import ClusterAnnotator
from EntityLinking.entity_linkings import EntityLinkings
from FileParser.ClusterFileParser.cluster_file_parser import ClusterFileParser
from FileParser.EntityLinkingFileParsing.entity_linking_file_parser import EntityLinkingFileParser
from Relation.relation_metrics import RelationMetrics
from util.filesystem_validators import WriteableDirectory, ReadableFile


def main():
    logging.basicConfig(format='%(asctime)s : [%(threadName)s] %(levelname)s : %(message)s', level=logging.INFO)
    parser: argparse.ArgumentParser = _initialize_parser()

    args = parser.parse_args()

    logging.info("Loading linkings...")
    entity_linkings: EntityLinkings = EntityLinkingFileParser.create_from_file(args.linkings)

    logging.info("Loading clusters...")
    clusters: Iterable[Cluster] = ClusterFileParser.create_from_file(args.clusters)

    logging.info("Annotating clusters...")
    cluster_annotator: ClusterAnnotator = ClusterAnnotator(entity_linkings, clusters, args.threads)
    result: List[RelationMetrics] = list(cluster_annotator.run())

    logging.info("Printing results...")
    _print_relations(result, args.output)


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


def _print_relations(result: List[RelationMetrics], output_directory: Path):
    result_as_json: List[object] = []
    for metric in result:
        result_as_json.append(metric.to_json_object())
    with Path(output_directory, f"enriched_cluster.json").open("w+") as output_file:
        json.dump(result_as_json, output_file)


if __name__ == "__main__":
    main()
