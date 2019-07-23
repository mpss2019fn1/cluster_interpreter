class EntityLinkings:

    def __init__(self):
        self._entity_mappings = {}

    def add(self, embedding_tag, knowledgebase_id):
        self._entity_mappings[embedding_tag] = knowledgebase_id

    def __getitem__(self, embedding_tag):
        return self._entity_mappings[embedding_tag]

    def __contains__(self, embedding_tag):
        return embedding_tag in self._entity_mappings

    def __len__(self):
        return len(self._entity_mappings)
