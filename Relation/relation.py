class Relation:

    def __init__(self, source, name, value):
        self.source = source
        self.name = name
        self.value = value

    @staticmethod
    def from_wikidata_record(record):
        return Relation(record["person"], record["wdLabel"], record["ps_Label"])

    @staticmethod
    def from_csv_record(record):
        return Relation(f"https://www.wikidata.org/wiki/{record['source']}", record["name"], record["value"])
