import logging
import queue
import threading
from queue import Queue

from Cluster.cluster import Cluster
from Relation import RelationMetrics
from RelationSource import AbstractRelationSource


class ClusterWorker(threading.Thread):
    
    def __init__(self, id_: int, working_queue: Queue[Cluster], relation_source: AbstractRelationSource):
        super(ClusterWorker, self).__init__(name=str(id_))
        self._working_queue: Queue[Cluster] = working_queue
        self._relation_source: AbstractRelationSource = relation_source

    def run(self):
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self):
        try:
            cluster: Cluster = self._working_queue.get_nowait()
            logging.info(f"Start analyzing cluster #{cluster.id}")
            self._analyze_entities(cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster: Cluster):
        index = 0
        metrics = RelationMetrics(len(cluster.entities))

        while index < len(cluster.entities):
            self._chunk_size = min(len(cluster.entities), self._relation_source.chunk_size())
            chunk = cluster.entities[index:index + self._chunk_size]

            logging.info(f"Getting relation for batch [{index},{index + len(chunk)}]")
            relations = self._relation_source.relations_for(chunk)

            if len(relations) > 0:
                # request succeeded
                index += len(chunk)

            ClusterWorker._count_relations(relations, metrics)
            self._relation_sink.persist(relations)

        self._print_relations(cluster, metrics)

    @staticmethod
    def _count_relations(relations, metrics):
        for relation in relations:
            metrics.add_relation(relation)

    def _print_relations(self, cluster: Cluster, cluster_metrics):
        with open(f"{self._output_directory}/enriched_cluster_{cluster.id}.txt", "w+") as output_file:
            print(f"\n== TOP RELATIONS FOR #{cluster.id} {cluster.name} ({len(cluster.entities)} Entities) ==",
                  file=output_file)
            print(cluster_metrics, file=output_file)