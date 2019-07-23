import csv
import logging
import os
import threading
from pathlib import Path
from typing import List, Dict, Iterator, Generator, Any

from EntityLinking.entity_linkings import EntityLinkings
from Relation.relation import Relation
from RelationSource.abstract_relation_source import AbstractRelationSource
from resources import constant
from wikidata_endpoint import WikidataEndpoint, WikidataRequestExecutor


class CachingWikidataRelationSource(AbstractRelationSource):

    DEFAULT_CHUNK_SIZE = 500
    CACHE_FILE = Path(os.path.dirname(os.path.abspath(__file__)), "..", ".cached_relations.csv")

    def __init__(self, linkings: EntityLinkings, wikidata_endpoint: WikidataEndpoint, cache_enabled: bool = True):
        self._linkings: EntityLinkings = linkings
        self._cache_enabled: bool = cache_enabled
        self._cached_relations: Dict[str, List[Relation]] = self._load_cached_relations()
        self._wikidata_endpoint: WikidataEndpoint = wikidata_endpoint
        self._chunk_size: int = CachingWikidataRelationSource.DEFAULT_CHUNK_SIZE

    def _load_cached_relations(self) -> Dict[str, List[Relation]]:
        cached_relations: Dict[str, List[Relation]] = {}

        if not self.CACHE_FILE.exists():
            return cached_relations

        with self.CACHE_FILE.open("r") as input_stream:
            csv_reader: csv.reader = csv.reader(input_stream)
            header_row: bool = True

            for row in csv_reader:
                if header_row:
                    header_row = False
                    continue

                if not row:
                    continue  # ignore blank lines

                relation: Relation = Relation.from_csv_record(row)
                if relation.source not in cached_relations:
                    cached_relations[relation.source] = []

                cached_relations[relation.source].append(relation)

        return cached_relations

    def _save_cached_relations(self) -> None:
        with self.CACHE_FILE.open("w+") as output_stream:
            print("source,name,target", file=output_stream)

            for relation_source in self._cached_relations:
                relations: List[Relation] = self._cached_relations[relation_source]

                for relation in relations:
                    print(f"{relation.source},{relation.name},{relation.target}", file=output_stream)

    def _retrieve_relations_for(self, embedding_tags: List[str]) -> List[Relation]:
        self._chunk_size = len(embedding_tags)
        entities: List[str] = [self._linkings[tag] for tag in embedding_tags]
        relations: List[Relation] = []
        if self._cache_enabled:
            cached_entities: Iterator[str] = filter(lambda x: x in self._cached_relations, entities)
            cached_relations: Generator[List[Relation]] = (self._cached_relations[entity] for entity in cached_entities)
            relations.extend([relation for relations in cached_relations for relation in relations])  # list flatting
            entities = list(set(entities) - set(cached_entities))

        remote_relations: List[Relation] = self._retrieve_relations_from_remote(entities)
        self._update_cache(remote_relations)
        relations.extend(remote_relations)
        return relations

    def _retrieve_relations_from_remote(self, entities: List[str]) -> List[Relation]:
        query = constant.named_entity_relations_sparql_query(entities)

        with self._wikidata_endpoint.request() as request:
            relations = [Relation.from_wikidata_record(record) for record in
                         request.post(query,
                                      on_timeout=self._on_timeout_wikidata_endpoint,
                                      on_error=self._on_error_wikidata_endpoint)]

            if len(relations) > 0:
                self._increase_chunk_size()

            return relations

    def _update_cache(self, relations: List[Relation]) -> None:
        if not self._cache_enabled:
            return

        for relation in relations:
            if relation.source not in self._cached_relations:
                self._cached_relations[relation.source] = []
            self._cached_relations[relation.source].append(relation)

    def _on_timeout_wikidata_endpoint(self, request: WikidataRequestExecutor) -> None:
        self._decrease_chunk_size()

    def _on_error_wikidata_endpoint(self, request: WikidataRequestExecutor, error: Any) -> None:
        pass

    def _increase_chunk_size(self) -> None:
        with CachingWikidataRelationSource.__chunk_size_lock:
            if self._chunk_size < CachingWikidataRelationSource.__chunk_size:
                # __chunk_size has not fully been utilized
                return

            CachingWikidataRelationSource.__succeeded_requests += 1
            if CachingWikidataRelationSource.__succeeded_requests > self._wikidata_endpoint.config().concurrent_requests():
                CachingWikidataRelationSource.__chunk_size = int(self._chunk_size * 1.25)
                CachingWikidataRelationSource.__succeeded_requests = 0
                logging.info(f"Increased chunk size to {CachingWikidataRelationSource.__chunk_size}")

    def _decrease_chunk_size(self) -> None:
        with CachingWikidataRelationSource.__chunk_size_lock:
            if self._chunk_size > CachingWikidataRelationSource.__chunk_size:
                # __chunk_size has already been decreased by another thread
                return

            CachingWikidataRelationSource.__chunk_size = int(self._chunk_size * 0.75)
            CachingWikidataRelationSource.__succeeded_requests = 0
            logging.info(f"Decreasing chunk size to {CachingWikidataRelationSource.__chunk_size}")

    __chunk_size_lock = threading.Lock()
    __chunk_size = DEFAULT_CHUNK_SIZE
    __succeeded_requests = 0

    def chunk_size(self) -> int:
        return CachingWikidataRelationSource._synchronized_chunk_size()

    @staticmethod
    def _synchronized_chunk_size() -> int:
        with CachingWikidataRelationSource.__chunk_size_lock:
            return CachingWikidataRelationSource.__chunk_size

    def shutdown(self):
        self._save_cached_relations()
