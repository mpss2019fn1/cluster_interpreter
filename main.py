import argparse
import logging
import queue
import time
from pathlib import Path

from cluster import Cluster
from cluster_annotator import ClusterAnnotator
from wiki_index import InMemoryCsv, WikidataPageProps
from util.filesystem_validators import AccessibleDirectory, AccessibleTextFile
from wikidata_endpoint import WikidataEndpointConfiguration, WikidataEndpoint

thread_pool = []
working_queue = queue.Queue()


def main():
    logging.basicConfig(format='%(asctime)s : [%(threadName)s] %(levelname)s : %(message)s', level=logging.INFO)
    parser = _initialize_parser()

    args = parser.parse_args()

    start_time = time.perf_counter()
    wikipedia_wikidata_mapping = _create_wikidata_wikipedia_mapping(args.mapping_file)
    end_time = time.perf_counter()
    logging.info(f"Creating in-memory mapping index took {end_time - start_time} seconds")

    global working_queue
    working_queue = Cluster.create_clusters(args.input)
    _initialize_threads(args.workers, args.output, wikipedia_wikidata_mapping)

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
    return general_parser


def _create_wikidata_wikipedia_mapping(mapping_file):
    return WikidataPageProps.initialize_instance_from_csv(mapping_file)


def _initialize_threads(number_of_workers, output_directory, wikipedia_wikidata_mapping):
    endpoint = _create_wikidata_endpoint()
    for x in range(number_of_workers):
        _thread = ClusterAnnotator(x, working_queue, endpoint, output_directory, wikipedia_wikidata_mapping)
        _thread.start()
        thread_pool.append(_thread)


def _create_wikidata_endpoint():
    config = WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini"))
    return WikidataEndpoint(config)


if __name__ == "__main__":
    main()
