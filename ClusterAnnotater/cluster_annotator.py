import os
import queue
from pathlib import Path
from typing import Iterable, List

from Cluster.cluster import Cluster
from ClusterAnnotater.cluster_worker import ClusterWorker
from EntityLinking.entity_linkings import EntityLinkings
from RelationSource import AbstractRelationSource
from RelationSource.caching_wikidata_relation_source import CachingWikidataRelationSource
from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration


class ClusterAnnotator:

    DEFAULT_CONFIG_WIKIDATA_ENDPOINT = Path(os.path.dirname(os.path.abspath(__file__)), "..", "resources",
                                            "wikidata_endpoint_config.ini")

    def __init__(self, linkings: EntityLinkings, clusters: Iterable[Cluster], workers: int):
        self._linkings: EntityLinkings = linkings
        self._clusters: Iterable[Cluster] = clusters
        self._relation_source: AbstractRelationSource = self._create_caching_relation_source()
        self._working_queue: queue.Queue[Cluster] = queue.Queue()
        self._workers: List[ClusterWorker] = []

        self._fill_working_queue()
        self._create_workers(workers)

    def _create_caching_relation_source(self) -> CachingWikidataRelationSource:
        config: WikidataEndpointConfiguration = WikidataEndpointConfiguration(
            ClusterAnnotator.DEFAULT_CONFIG_WIKIDATA_ENDPOINT)
        wikidata_endpoint: WikidataEndpoint = WikidataEndpoint(config)
        return CachingWikidataRelationSource(self._linkings, wikidata_endpoint, True)

    def _fill_working_queue(self):
        map(self._working_queue.put, self._clusters)

    def _create_workers(self, workers: int) -> None:
        for i in range(workers):
            self._workers.append(ClusterWorker(i, self._working_queue, self._relation_source))
