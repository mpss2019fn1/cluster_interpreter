from relation_sink.abstract_relation_sink import AbstractRelationSink


class NullRelationSink(AbstractRelationSink):

    def _perform_persist(self, relations):
        pass
