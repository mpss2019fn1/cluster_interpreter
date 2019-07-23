import re
from typing import Match, List, Dict

from resources import constant


class Relation:

    ID_EXTRACTION_REGEX = re.compile(r"^.+(Q\d+)$")

    def __init__(self, source: str, name: str, target: str):
        self.source: str = source
        self.name: str = name
        self.target: str = target

    @staticmethod
    def from_wikidata_record(record: Dict[str, str]) -> "Relation":
        match: Match = Relation.ID_EXTRACTION_REGEX.match(record[constant.RELATION_SOURCE_LABEL])
        if not match:
            raise ValueError("record does not contain a wikidata id")

        return Relation(match.group(1),
                        record[constant.RELATION_NAME_LABEL],
                        record[constant.RELATION_TARGET_LABEL])

    @staticmethod
    def from_csv_record(record: List[str]) -> "Relation":
        return Relation(record[0], record[1], record[2])
