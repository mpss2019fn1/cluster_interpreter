import sys

from RelationSource.abstract_relation_source import AbstractRelationSource
from Relation import Relation
from wiki_index.wikidata_entity_relations import WikidataEntityRelations


class CsvRelationSource(AbstractRelationSource):

    def __init__(self, csv):
        self._relations_mapping = WikidataEntityRelations.initialize_instance_from_csv(csv)

    def _retrieve_relations_for(self, entities):
        for entity in entities:
            yield from (Relation.from_csv_record(row) for row in
                        self._relations_mapping.relations(entity.wikidata_id))

    def chunk_size(self):
        return sys.maxsize
