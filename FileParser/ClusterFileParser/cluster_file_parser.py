import csv
from pathlib import Path
from typing import Dict, Iterable

from Cluster.cluster import Cluster
from FileParser.abstract_file_parser import AbstractFileParser


class ClusterFileParser(AbstractFileParser):
    COLUMN_INDEX_CLUSTER_ID = 0
    COLUMN_INDEX_EMBEDDING_LABEL = 1

    @staticmethod
    def create_from_file(configuration_file: Path) -> Iterable[Cluster]:
        with configuration_file.open("r") as csv_stream:
            csv_reader = csv.reader(csv_stream, delimiter=',')
            clusters: Dict[str, Cluster] = {}

            next(csv_reader, None)  # skip header
            for row in csv_reader:
                if not row:
                    continue

                cluster_id = row[ClusterFileParser.COLUMN_INDEX_CLUSTER_ID]
                embedding_tag = row[ClusterFileParser.COLUMN_INDEX_EMBEDDING_LABEL]

                if cluster_id not in clusters:
                    clusters[cluster_id] = Cluster(int(cluster_id))

                clusters[cluster_id].entities.append(embedding_tag)

            return clusters.values()
