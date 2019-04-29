import threading
from collections import Counter
import queue

from resources import constant


class ClusterAnnotator(threading.Thread):

    def __init__(self, thread_id, work_queue):
        threading.Thread.__init__(self)

        self._thread_id = thread_id
        self._work_queue = work_queue

        self._cluster_annotations = {"relations": Counter()}

        self._mysql_connection = constant.create_mysql_connection()

    def run(self):
        while self._analyze_cluster:
            pass

    def _analyze_cluster(self):
        try:
            cluster = self._work_queue.get_nowait()
            cluster.fetch_wikidata_ids(self._mysql_connection)
            for entity in cluster.entities:
                self._analyze_entity(entity)
            return True
        except queue.Empty:
            return False

    def _analyze_entity(self, entity):
        pass

    def _analyze_relations(self, entity):
        pass
