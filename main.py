import argparse
import logging
import queue
from pathlib import Path

from cluster import Cluster
from cluster_annotator import ClusterAnnotator
from util.filesystem_validators import AccessibleDirectory, AccessibleTextFile
from wikidata_endpoint import WikidataEndpointConfiguration, WikidataEndpoint

thread_pool = []
working_queue = queue.Queue()


def main():
    logging.basicConfig(format='%(asctime)s : [%(threadName)s] %(levelname)s : %(message)s', level=logging.INFO)
    parser = _initialize_parser()

    args = parser.parse_args()

    global working_queue
    working_queue = Cluster.create_clusters(args.input)
    _initialize_threads(args.workers)

    for thread in thread_pool:
        thread.join()


def _initialize_parser():
    general_parser = argparse.ArgumentParser(
        description='Enrich clusters with further information by utilizing external sources')
    general_parser.add_argument("--input", help='File containing clustered entities', action=AccessibleTextFile,
                                required=True)
    general_parser.add_argument("--output", help='Desired location for enriched clusters', action=AccessibleDirectory,
                                required=True)
    general_parser.add_argument("--workers", help='Number of workers to start in parallel', type=int,
                                required=False, default=16)
    return general_parser


def _initialize_threads(number_of_workers):
    endpoint = _create_wikidata_endpoint()
    for x in range(number_of_workers):
        _thread = ClusterAnnotator(x, working_queue, endpoint)
        _thread.start()
        thread_pool.append(_thread)


def _create_wikidata_endpoint():
    config = WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini"))
    return WikidataEndpoint(config)


if __name__ == "__main__":
    main()
