import time


class EntityLinkings:

    def __init__(self):
        self.imported = time.time()
        self._entity_mappings = {}

    def add(self, knowledgebase_id, embedding_tag):
        self._entity_mappings[knowledgebase_id] = embedding_tag

    def __getitem__(self, item):
        return self._entity_mappings[item]

    def __contains__(self, knowledgebase_id):
        return knowledgebase_id in self._entity_mappings

    def __len__(self):
        return len(self._entity_mappings)
