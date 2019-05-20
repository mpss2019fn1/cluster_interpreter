import logging
import threading

from relation_source.abstract_relation_source import AbstractRelationSource
from relation import Relation
from resources import constant


class WikidataRelationSource(AbstractRelationSource):

    def __init__(self, wikidata_endpoint):
        self._wikidata_endpoint = wikidata_endpoint
        self._chunk_size = constant.STANDARD_CHUNK_SIZE

    def _retrieve_relations_for(self, entities):
        query = constant.named_entity_relations_sparql_query(entities)

        with self._wikidata_endpoint.request() as request:
            relations = [Relation.from_wikidata_record(record) for record in
                         request.post(query,
                                      on_timeout=self._on_timeout_wikidata_endpoint,
                                      on_error=self._on_error_wikidata_endpoint)]

            if len(relations) > 0:
                self._increase_chunk_size()

            return relations

    def _on_timeout_wikidata_endpoint(self, request):
        self._decrease_chunk_size()

    def _on_error_wikidata_endpoint(self, request, error):
        pass

    def _increase_chunk_size(self):
        with WikidataRelationSource.__chunk_size_lock:
            if self._chunk_size < WikidataRelationSource.__chunk_size:
                # __chunk_size has not fully been utilized
                return

            WikidataRelationSource.__succeeded_requests += 1
            if WikidataRelationSource.__succeeded_requests > self._wikidata_endpoint.config().concurrent_requests():
                WikidataRelationSource.__chunk_size = int(self._chunk_size * 1.25)
                WikidataRelationSource.__succeeded_requests = 0
                logging.info(f"Increased chunk size to {WikidataRelationSource.__chunk_size}")

    def _decrease_chunk_size(self):
        with WikidataRelationSource.__chunk_size_lock:
            if self._chunk_size > WikidataRelationSource.__chunk_size:
                # __chunk_size has already been decreased by another thread
                return

            WikidataRelationSource.__chunk_size = int(self._chunk_size * 0.75)
            WikidataRelationSource.__succeeded_requests = 0
            logging.info(f"Decreasing chunk size to {WikidataRelationSource.__chunk_size}")

    __chunk_size_lock = threading.Lock()
    __chunk_size = constant.STANDARD_CHUNK_SIZE
    __succeeded_requests = 0

    def chunk_size(self):
        return WikidataRelationSource._synchronized_chunk_size()

    @staticmethod
    def _synchronized_chunk_size():
        with WikidataRelationSource.__chunk_size_lock:
            return WikidataRelationSource.__chunk_size
