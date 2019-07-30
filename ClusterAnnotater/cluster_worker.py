import logging
import queue
import threading
from typing import List

from Cluster.cluster import Cluster
from Relation.relation_metrics import RelationMetrics
from RelationSource.abstract_relation_source import AbstractRelationSource


class ClusterWorker(threading.Thread):
    MAX_NUMBER_OF_RETRIES = 3

    def __init__(self, id_: int, working_queue: queue.Queue, relation_source: AbstractRelationSource):
        super(ClusterWorker, self).__init__(name=str(id_))
        self._working_queue: queue.Queue = working_queue
        self._relation_source: AbstractRelationSource = relation_source
        self._results: List[RelationMetrics] = []

    def run(self) -> None:
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self) -> bool:
        try:
            cluster: Cluster = self._working_queue.get_nowait()
            logging.info(f"Start analyzing cluster #{cluster.id}")
            self._analyze_entities(cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster: Cluster) -> None:
        index = 0
        error_counter = 0
        metrics = RelationMetrics(cluster)

        while index < len(cluster.entities):
            self._chunk_size = min(len(cluster.entities), self._relation_source.chunk_size())
            chunk = cluster.entities[index:index + self._chunk_size]

            logging.info(f"[CLUSTER-{cluster.id}] Getting relation for batch [{index},{index + len(chunk)}]")
            relations = self._relation_source.relations_for(chunk)

            if len(relations) > 0:
                # request succeeded
                error_counter = 0
                index += len(chunk)
                for relation in relations:
                    metrics.add_relation(relation)

            if len(relations) == 0:
                # No relation has been fetched from cache AND remote source due to unsuccessful linking or remote error
                error_counter += 1

            if error_counter == ClusterWorker.MAX_NUMBER_OF_RETRIES:
                error_counter = 0
                index += self._chunk_size

        self._results.append(metrics)

    @property
    def result(self) -> List[RelationMetrics]:
        return self._results
