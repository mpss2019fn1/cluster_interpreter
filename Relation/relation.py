import re
from typing import Match

from resources import constant


class Relation:

    ID_EXTRACTION_REGEX = re.compile(r"^.+(Q\d+)$")

    def __init__(self, source: str, name: str, target: str):
        self.source: str = source
        self.name: str = name
        self.target: str = target

    @staticmethod
    def from_wikidata_record(record) -> "Relation":
        match: Match = Relation.ID_EXTRACTION_REGEX.match(record[constant.RELATION_SOURCE_LABEL])
        if not match:
            raise ValueError("record does not contain a wikidata id")

        return Relation(match.group(0),
                        record[constant.RELATION_NAME_LABEL],
                        record[constant.RELATION_TARGET_LABEL])

    @staticmethod
    def from_csv_record(record) -> "Relation":
        return Relation(f"https://www.wikidata.org/wiki/{record['source']}", record["name"], record["value"])
