import collections
import threading
from collections import Counter
import queue

import requests

from resources import constant
from sparql_endpoint import SparqlEndpoint


class ClusterAnnotator(threading.Thread):

    def __init__(self, thread_id, work_queue):
        threading.Thread.__init__(self)

        self._thread_id = thread_id
        self._work_queue = work_queue

        self._cluster_annotations = {"relations": Counter()}

        self._mysql_connection = constant.create_mysql_connection()
        self._sparql_endpoint = SparqlEndpoint(constant.WIKIDATA_API_URL)

    def run(self):
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self):
        try:
            cluster = self._work_queue.get_nowait()
            cluster.fetch_wikidata_ids(self._mysql_connection)
            self._analyze_entities(cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster):
        chunk_size = 200
        index = 0
        relations = {}

        while index < len(cluster.entities):
            chunk = cluster.entities[index:index + chunk_size]
            query = constant.named_entity_relations_sparql_query([f"wd:{x.wikidata_id}" for x in chunk])

            try:
                records = self._sparql_endpoint.query(query)
                ClusterAnnotator._count_relations(records, relations)
                index += len(chunk)

            except requests.exceptions.Timeout:
                chunk_size //= 2
                pass
        ClusterAnnotator._print_relations(cluster, relations)

    @staticmethod
    def _count_relations(records, relations):
        for record in records:
            relation = record["wdLabel"]
            target = record["ps_Label"]
            if relation not in relations:
                relations[relation] = collections.Counter()

            relations[relation][target] += 1

    @staticmethod
    def _print_relations(cluster, relations):
        relations_count = {}
        for relation in relations:
            relations_count[relation] = relations[relation].most_common(3)

        top_results_printed = "\t".join([f"{relations_count[relation]} - {relation}\n" for relation in relations_count])
        print(f"TOP RESULTS FOR #{cluster.id} {cluster.name}:\n{top_results_printed}")
