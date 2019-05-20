import logging
import queue
import threading
from collections import Counter

from relation.relation_metrics import RelationMetrics
from util.utils import measure


class ClusterAnnotator(threading.Thread):

    def __init__(self, thread_id, work_queue, relation_source, output_directory, wikipedia_wikidata_mapping,
                 relation_sink):
        threading.Thread.__init__(self)

        self._thread_id = thread_id
        self._work_queue = work_queue

        self._cluster_annotations = {"relation": Counter()}

        self._relation_source = relation_source
        self._output_directory = output_directory
        self._wikipedia_wikidata_mapping = wikipedia_wikidata_mapping
        self._relation_sink = relation_sink

    def run(self):
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self):
        try:
            cluster = self._work_queue.get_nowait()
            logging.info(f"Start analyzing cluster #{cluster.id}")
            measure(f"Fetching wikidata ids for cluster #{cluster.id} ({len(cluster.entities)} entities)",
                    cluster.fetch_wikidata_ids,
                    self._wikipedia_wikidata_mapping)
            measure(f"Analyzing cluster #{cluster.id} ({len(cluster.entities)} entities)", self._analyze_entities,
                    cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster):
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

            ClusterAnnotator._count_relations(relations, metrics)
            self._relation_sink.persist(relations)

        self._print_relations(cluster, metrics)

    @staticmethod
    def _count_relations(relations, metrics):
        for relation in relations:
            metrics.add_relation(relation)

    def _print_relations(self, cluster, cluster_metrics):
        with open(f"{self._output_directory}/enriched_cluster_{cluster.id}.txt", "w+") as output_file:
            print(f"\n== TOP RELATIONS FOR #{cluster.id} {cluster.name} ({len(cluster.entities)} Entities) ==",
                  file=output_file)
            print(cluster_metrics, file=output_file)
