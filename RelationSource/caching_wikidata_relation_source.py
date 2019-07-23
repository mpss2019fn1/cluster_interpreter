import csv
import logging
import os
import threading
from pathlib import Path
from typing import List, Dict, Iterator, Generator, Any, Iterable, Optional

from EntityLinking.entity_linkings import EntityLinkings
from Relation.relation import Relation
from RelationSource.abstract_relation_source import AbstractRelationSource
from resources import constant
from wikidata_endpoint import WikidataEndpoint, WikidataRequestExecutor


class CachingWikidataRelationSource(AbstractRelationSource):
    DEFAULT_CHUNK_SIZE = 500

    def __init__(self, linkings: EntityLinkings, wikidata_endpoint: WikidataEndpoint):
        self._linkings: EntityLinkings = linkings
        self._wikidata_endpoint: WikidataEndpoint = wikidata_endpoint
        self._chunk_size: int = CachingWikidataRelationSource.DEFAULT_CHUNK_SIZE

    def _retrieve_relations_for(self, embedding_tags: List[str]) -> List[Relation]:
        self._chunk_size = len(embedding_tags)
        entities: List[str] = [self._linkings[tag] for tag in embedding_tags if tag in self._linkings]
        relations: List[Relation] = []

        relations.extend(CachingWikidataRelationSource._retrieve_relations_from_cache(entities))
        uncached_entities: List[str] = list(set(entities) - set([relation.source for relation in relations]))

        logging.info(f"Cache-Hit for {len(entities) - len(uncached_entities)} (out of {len(entities)}) entities")

        remote_relations: List[Relation] = self._retrieve_relations_from_remote(uncached_entities)

        CachingWikidataRelationSource._add_to_cache(remote_relations)
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

    __chunk_size_lock: threading.Lock = threading.Lock()
    __chunk_size: int = DEFAULT_CHUNK_SIZE
    __succeeded_requests: int = 0
    __cache_lock: threading.Lock = threading.Lock()
    __cached_relations: Optional[Dict[str, List[Relation]]] = None
    __new_cached_relations: List[Relation] = []
    __caching_file_lock: threading.Lock = threading.Lock()
    __cache_file: Path = Path(os.path.dirname(os.path.abspath(__file__)), "..", ".cached_relations.csv")

    def chunk_size(self) -> int:
        return CachingWikidataRelationSource._synchronized_chunk_size()

    @staticmethod
    def _synchronized_chunk_size() -> int:
        with CachingWikidataRelationSource.__chunk_size_lock:
            return CachingWikidataRelationSource.__chunk_size

    @staticmethod
    def _retrieve_relations_from_cache(entities: List[str]) -> Iterable[Relation]:
        cached_relations: Optional[Generator[List[Relation]]] = None

        with CachingWikidataRelationSource.__cache_lock:
            CachingWikidataRelationSource.__initialize_cache()

            cached_entities: Iterator[str] = filter(lambda x: x in CachingWikidataRelationSource.__cached_relations,
                                                    entities)
            cached_relations = (CachingWikidataRelationSource.__cached_relations[entity] for
                                entity in cached_entities)

        if cached_relations is None:
            raise StopIteration

        for list_of_relations in cached_relations:
            yield from list_of_relations

    @staticmethod
    def _add_to_cache(relations: Iterable[Relation]) -> None:
        with CachingWikidataRelationSource.__cache_lock:
            CachingWikidataRelationSource.__initialize_cache()

            for relation in relations:
                if relation.source not in CachingWikidataRelationSource.__cached_relations:
                    CachingWikidataRelationSource.__cached_relations[relation.source] = []

                CachingWikidataRelationSource.__cached_relations[relation.source].append(relation)
                CachingWikidataRelationSource.__new_cached_relations.append(relation)

        CachingWikidataRelationSource._save_cached_relations()

    @staticmethod
    def __initialize_cache() -> None:
        if CachingWikidataRelationSource.__cached_relations is not None:
            return

        CachingWikidataRelationSource.__cached_relations = {}

        if not CachingWikidataRelationSource.__cache_file.exists():
            return

        with CachingWikidataRelationSource.__cache_file.open("r") as input_stream:
            csv_reader: csv.reader = csv.reader(input_stream)
            header_row: bool = True

            for row in csv_reader:
                if header_row:
                    header_row = False
                    continue

                if not row:
                    continue  # ignore blank lines

                relation: Relation = Relation.from_csv_record(row)
                if relation.source not in CachingWikidataRelationSource.__cached_relations:
                    CachingWikidataRelationSource.__cached_relations[relation.source] = []

                CachingWikidataRelationSource.__cached_relations[relation.source].append(relation)

        logging.info(f"Done loading cached relations... "
                     f"{len(CachingWikidataRelationSource.__cached_relations)} relations loaded")

    @staticmethod
    def _save_cached_relations() -> None:
        new_relations: List[Relation] = []
        with CachingWikidataRelationSource.__cache_lock:
            new_relations.extend(CachingWikidataRelationSource.__new_cached_relations)
            CachingWikidataRelationSource.__new_cached_relations.clear()

        if len(new_relations) < 1:
            return

        with CachingWikidataRelationSource.__caching_file_lock:
            if not CachingWikidataRelationSource.__cache_file.exists():
                with CachingWikidataRelationSource.__cache_file.open("w+") as output_stream:
                    print("source,name,target", file=output_stream)

            with CachingWikidataRelationSource.__cache_file.open("a") as output_stream:
                for relation in new_relations:
                    print(f"{relation.source},{relation.name},{relation.target}", file=output_stream)
