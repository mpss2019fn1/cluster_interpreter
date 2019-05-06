import threading
from collections import Counter
import queue
import logging
from pathlib import Path

from resources import constant
from wikidata_endpoint import WikidataEndpoint
from wikidata_endpoint import WikidataEndpointConfiguration


class Relation:

    def __init__(self, source, name, value):
        self.source = source
        self.name = name
        self.value = value

    @staticmethod
    def from_wikidata_record(record):
        return Relation(record["person"], record["wdLabel"], record["ps_Label"])


class RelationMetrics:

    def __init__(self, number_of_entities):
        self._unique_relation_participants = {}
        self._value_per_relation = {}
        self._unique_relations_counter = Counter()
        self._number_of_entities = number_of_entities

    def add_relation(self, relation):
        if relation.name not in self._unique_relation_participants:
            self._unique_relation_participants[relation.name] = set()
            self._value_per_relation[relation.name] = Counter()
        if relation.source not in self._unique_relation_participants[relation.name]:
            self._unique_relations_counter[relation.name] += 1
            self._unique_relation_participants[relation.name].add(relation.source)
        self._value_per_relation[relation.name][relation.value] += 1

    def top_relations(self, max_relations, min_occurrence_factor=0.3):
        return list(filter(lambda x: x[1] > self._number_of_entities * min_occurrence_factor,
                           self._unique_relations_counter.most_common(max_relations)))

    def top_values(self, relation_name, max_values, min_occurrence_factor=0.1):
        return list(filter(lambda x: x[1] > self._number_of_entities * min_occurrence_factor,
                           self._value_per_relation[relation_name].most_common(max_values)))

    def __str__(self):
        representation = []
        for relation, relation_count in self.top_relations(constant.MAX_NUMBER_OF_RELATIONS_PER_CLUSTER):
            relation_percentage = round(relation_count / self._number_of_entities * 100, 2)
            representation.append(f"Relation: {relation} {relation_percentage}%")
            for value, value_count in self.top_values(relation, constant.MAX_NUMBER_OF_VALUES_PER_RELATION):
                value_percentage = round(value_count / self._number_of_entities * 100, 2)
                representation.append("\t↳ {:5.2f}% {}".format(value_percentage, value))
        return "\n".join(representation)


class ClusterAnnotator(threading.Thread):

    def __init__(self, thread_id, work_queue):
        threading.Thread.__init__(self)

        self._thread_id = thread_id
        self._work_queue = work_queue

        self._cluster_annotations = {"relations": Counter()}

        self._mysql_connection = constant.create_mysql_connection()
        self._wikidata_endpoint_config = WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini"))
        self._wikidata_endpoint = WikidataEndpoint(self._wikidata_endpoint_config)
        self._chunk_size = constant.STANDARD_CHUNK_SIZE

    def run(self):
        while self._analyze_cluster():
            pass

    def _analyze_cluster(self):
        try:
            cluster = self._work_queue.get_nowait()
            logging.info(f"Start analyzing cluster {cluster.name} on Thread #{self._thread_id}")
            cluster.fetch_wikidata_ids(self._mysql_connection)
            logging.info(f"Finished fetching wikidata ids from MySQL on Thread #{self._thread_id}")
            self._analyze_entities(cluster)
            return True
        except queue.Empty:
            return False

    def _analyze_entities(self, cluster):
        index = 0
        metrics = RelationMetrics(len(cluster.entities))

        while index < len(cluster.entities):
            chunk = cluster.entities[index:index + self._chunk_size]
            query = constant.named_entity_relations_sparql_query(chunk)

            logging.info(
                f"Executing SPARQL query for batch [{index},{index + len(chunk)}] on Thread #{self._thread_id}")
            with self._wikidata_endpoint.request() as request:
                relations = [Relation.from_wikidata_record(record) for record in
                             request.post(query,
                                          on_timeout=self._on_timeout_wikidata_endpoint,
                                          on_error=self._on_error_wikidata_endpoint)]
                index += self._chunk_size if len(relations) > 0 else 0
            logging.info(
                f"Finished executing SPARQL query on Thread #{self._thread_id}")
            ClusterAnnotator._count_relations(relations, metrics)

        ClusterAnnotator._print_relations(cluster, metrics)

    def _on_timeout_wikidata_endpoint(self, request):
        self._chunk_size = int(self._chunk_size * (3/4))

    def _on_error_wikidata_endpoint(self, request, error):
        pass

    @staticmethod
    def _count_relations(relations, metrics):
        for relation in relations:
            metrics.add_relation(relation)

    @staticmethod
    def _print_relations(cluster, cluster_metrics):
        print(f"\n== TOP RELATIONS FOR #{cluster.id} {cluster.name} ({len(cluster.entities)} Entities) ==")
        print(cluster_metrics)
