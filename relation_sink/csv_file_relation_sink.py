from relation_sink.abstract_file_relation_sink import AbstractFileRelationSink


class CsvRelationSink(AbstractFileRelationSink):

    def __init__(self, file_path):
        super().__init__(file_path)

    def _generate_file_headers(self, relations):
        yield "source,name,value"

    def _generate_file_content(self, relations):
        yield from (f"\"{relation.source.split('/')[-1]}\",\"{relation.name}\",\"{relation.target}\"" for relation in
                    relations)
