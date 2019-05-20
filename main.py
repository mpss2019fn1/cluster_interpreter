import argparse
import logging
import queue
from pathlib import Path

from cluster import Cluster
from cluster_annotator import ClusterAnnotator
from relation_sink import NullRelationSink, CsvRelationSink
from relation_source.csv_relation_source import CsvRelationSource
from relation_source.wikidata_relation_source import WikidataRelationSource
from util.filesystem_validators import AccessibleDirectory, AccessibleTextFile
from util.utils import measure
from wiki_index import WikidataPageProps
from wikidata_endpoint import WikidataEndpointConfiguration, WikidataEndpoint

thread_pool = []
working_queue = queue.Queue()


def main():
    logging.basicConfig(format='%(asctime)s : [%(threadName)s] %(levelname)s : %(message)s', level=logging.INFO)
    parser = _initialize_parser()

    args = parser.parse_args()

    wikipedia_wikidata_mapping = measure("Building in-memory mapping", _create_wikidata_wikipedia_mapping,
                                         args.mapping_file)

    relation_sink = NullRelationSink()
    if args.relations_output:
        relation_sink = CsvRelationSink(args.relations_output)

    if args.relations_input:
        relation_source = CsvRelationSource(args.relations_input)
    else:
        endpoint = _create_wikidata_endpoint()
        relation_source = WikidataRelationSource(endpoint)

    global working_queue
    working_queue = Cluster.create_clusters(args.input)
    _initialize_threads(args.workers, relation_source, args.output, wikipedia_wikidata_mapping, relation_sink)

    for thread in thread_pool:
        thread.join()


def _initialize_parser():
    general_parser = argparse.ArgumentParser(
        description='Enrich clusters with further information by utilizing external sources')
    general_parser.add_argument("--input", help='File containing clustered entities', action=AccessibleTextFile,
                                required=True)
    general_parser.add_argument("--mapping_file", help='CSV File containing wikidpedia page ids to wikidata mappings',
                                action=AccessibleTextFile,
                                required=True)
    general_parser.add_argument("--output", help='Desired location for enriched clusters', action=AccessibleDirectory,
                                required=True)
    general_parser.add_argument("--workers", help='Number of workers to start in parallel', type=int,
                                required=False, default=16)
    general_parser.add_argument("--relations-output", help="File to persist fetched relation to", required=False,
                                default=None)
    general_parser.add_argument("--relations-input", help="Csv file to read persisted relation from", required=False,
                                action=AccessibleTextFile, default=None)
    return general_parser


def _create_wikidata_wikipedia_mapping(mapping_file):
    return WikidataPageProps.initialize_instance_from_csv(mapping_file)


def _create_wikidata_endpoint():
    config = WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini"))
    return WikidataEndpoint(config)


def _initialize_threads(number_of_workers, relation_source, output_directory, wikipedia_wikidata_mapping, relation_sink):
    for x in range(number_of_workers):
        _thread = ClusterAnnotator(x, working_queue, relation_source, output_directory, wikipedia_wikidata_mapping,
                                   relation_sink)
        _thread.start()
        thread_pool.append(_thread)


if __name__ == "__main__":
    main()
