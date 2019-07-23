import csv

from EntityLinking.entity_linkings import EntityLinkings
from FileParser.abstract_file_parser import AbstractFileParser


class EntityLinkingFileParser(AbstractFileParser):
    COLUMN_INDEX_KNOWLEDGEBASE_ID = 1
    COLUMN_INDEX_EMBEDDING_LABEL = 0

    @staticmethod
    def create_from_file(configuration_file):
        with open(configuration_file, 'r') as csv_stream:
            csv_reader = csv.reader(csv_stream, delimiter=',')
            linkings = EntityLinkings()

            next(csv_reader, None)  # skip header
            for row in csv_reader:
                if not row:
                    continue

                knowledgebase_id = row[EntityLinkingFileParser.COLUMN_INDEX_KNOWLEDGEBASE_ID]
                embedding_tag = row[EntityLinkingFileParser.COLUMN_INDEX_EMBEDDING_LABEL]
                linkings.add(embedding_tag, knowledgebase_id)

            return linkings
