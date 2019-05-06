import logging
import queue
import threading
from collections import Counter

from relation import Relation
from relation_metrics import RelationMetrics
from resources import constant


class ClusterAnnotator(threading.Thread):

    def __init__(self, thread_id, work_queue, wikidata_endpoint, output_directory):
        threading.Thread.__init__(self)

        self._thread_id = thread_id
        self._work_queue = work_queue

        self._cluster_annotations = {"relations": Counter()}

        self._wikidata_endpoint = wikidata_endpoint
        self._mysql_connection = constant.create_mysql_connection()
        self._chunk_size = constant.STANDARD_CHUNK_SIZE
        self._output_directory = output_directory

    def run(self):
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self):
        try:
            cluster = self._work_queue.get_nowait()
            logging.info(f"Start analyzing cluster {cluster.name}")
            cluster.fetch_wikidata_ids(self._mysql_connection)
            logging.info(f"Finished fetching wikidata ids from MySQL")
            self._analyze_entities(cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster):
        index = 0
        metrics = RelationMetrics(len(cluster.entities))

        while index < len(cluster.entities):
            self._chunk_size = min(len(cluster.entities), ClusterAnnotator._synchronized_chunk_size())
            chunk = cluster.entities[index:index + self._chunk_size]
            query = constant.named_entity_relations_sparql_query(chunk)

            logging.info(f"Executing SPARQL query for batch [{index},{index + len(chunk)}]")
            with self._wikidata_endpoint.request() as request:
                relations = [Relation.from_wikidata_record(record) for record in
                             request.post(query,
                                          on_timeout=self._on_timeout_wikidata_endpoint,
                                          on_error=self._on_error_wikidata_endpoint)]

                if len(relations) > 0:
                    # request succeeded
                    index += len(chunk)
                    self._increase_chunk_size()

            ClusterAnnotator._count_relations(relations, metrics)

        ClusterAnnotator._print_relations(cluster, metrics)

    def _on_timeout_wikidata_endpoint(self, request):
        self._decrease_chunk_size()

    def _on_error_wikidata_endpoint(self, request, error):
        pass

    def _increase_chunk_size(self):
        with ClusterAnnotator.__chunk_size_lock:
            if self._chunk_size < ClusterAnnotator.__chunk_size:
                # __chunk_size has not fully been utilized
                return

            ClusterAnnotator.__succeeded_requests += 1
            if ClusterAnnotator.__succeeded_requests > self._wikidata_endpoint.config().concurrent_requests():
                ClusterAnnotator.__chunk_size = int(self._chunk_size * 1.25)
                ClusterAnnotator.__succeeded_requests = 0
                logging.info(f"Increased chunk size to {ClusterAnnotator.__chunk_size}")

    def _decrease_chunk_size(self):
        with ClusterAnnotator.__chunk_size_lock:
            if self._chunk_size > ClusterAnnotator.__chunk_size:
                # __chunk_size has already been decreased by another thread
                return

            ClusterAnnotator.__chunk_size = int(self._chunk_size * 0.75)
            ClusterAnnotator.__succeeded_requests = 0
            logging.info(f"Decreasing chunk size to {ClusterAnnotator.__chunk_size}")

    __chunk_size_lock = threading.Lock()
    __chunk_size = constant.STANDARD_CHUNK_SIZE
    __succeeded_requests = 0

    @staticmethod
    def _synchronized_chunk_size():
        with ClusterAnnotator.__chunk_size_lock:
            return ClusterAnnotator.__chunk_size

    @staticmethod
    def _count_relations(relations, metrics):
        for relation in relations:
            metrics.add_relation(relation)

    def _print_relations(self, cluster, cluster_metrics):
        with open(f"{self._output_directory}/enriched_cluster_{cluster.id}.txt", "w+") as output_file:
            print(f"\n== TOP RELATIONS FOR #{cluster.id} {cluster.name} ({len(cluster.entities)} Entities) ==",
                  file=output_file)
            print(cluster_metrics, file=output_file)
