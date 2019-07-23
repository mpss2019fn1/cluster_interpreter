from collections import Counter
from typing import List, Tuple

from Cluster.cluster import Cluster
from resources import constant


class RelationMetrics:

    def __init__(self, cluster: Cluster):
        self._unique_relation_participants = {}
        self._value_per_relation = {}
        self._unique_relations_counter = Counter()
        self._cluster: Cluster = cluster

    def add_relation(self, relation):
        if relation.name not in self._unique_relation_participants:
            self._unique_relation_participants[relation.name] = set()
            self._value_per_relation[relation.name] = Counter()
        if relation.source not in self._unique_relation_participants[relation.name]:
            self._unique_relations_counter[relation.name] += 1
            self._unique_relation_participants[relation.name].add(relation.source)
        self._value_per_relation[relation.name][relation.value] += 1

    def top_relations(self, max_relations, min_occurrence_factor=0.3) -> List[Tuple[str, int]]:
        return list(filter(lambda x: x[1] > self.number_of_entities * min_occurrence_factor,
                           self._unique_relations_counter.most_common(max_relations)))

    def top_values(self, relation_name, max_values, min_occurrence_factor=0.1) -> List[Tuple[str, int]]:
        return list(filter(lambda x: x[1] > self.number_of_entities * min_occurrence_factor,
                           self._value_per_relation[relation_name].most_common(max_values)))

    def __str__(self):
        representation = []
        for relation, relation_count in self.top_relations(constant.MAX_NUMBER_OF_RELATIONS_PER_CLUSTER):
            relation_percentage = round(relation_count / self.number_of_entities * 100, 2)
            representation.append(f"Relation: {relation} {relation_percentage}%")
            for value, value_count in self.top_values(relation, constant.MAX_NUMBER_OF_VALUES_PER_RELATION):
                value_percentage = round(value_count / self.number_of_entities * 100, 2)
                representation.append("\t↳ {:5.2f}% {}".format(value_percentage, value))
        return "\n".join(representation)

    @property
    def number_of_entities(self) -> int:
        return len(self._cluster.entities)

    def to_json_object(self) -> object:

        def top_relations_json_object() -> object:

            def relation_json_object(relation: Tuple[str, int]) -> object:

                def relation_value_json_object(value: Tuple[str, int], entities_with_relation: int) -> object:
                    return {
                        "name": value[0],
                        "absolute_occurrence": value[0],
                        "relative_occurrence": "{:5.2f}%".format(value[1] / entities_with_relation * 100)
                    }

                top_values: List[object] = []
                for _value in self.top_values(relation[0], constant.MAX_NUMBER_OF_VALUES_PER_RELATION):
                    top_values.append(relation_value_json_object(_value, relation[1]))

                return {
                    "name": relation[0],
                    "absolute_occurrence": relation[1],
                    "relative_occurrence": "{:5.2f}%".format(relation[1] / self.number_of_entities * 100)
                }

            top_relations: List[object] = []
            for _relation in self.top_relations(constant.MAX_NUMBER_OF_RELATIONS_PER_CLUSTER):
                top_relations.append(relation_json_object(_relation))
            return top_relations

        return {
            "cluster": self._cluster.to_json_object(),
            "top_relations": top_relations_json_object()
        }
