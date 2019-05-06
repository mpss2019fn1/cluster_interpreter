import queue
import time
import logging
from pathlib import Path
from resources import constant


class Entity:

    def __init__(self, name):
        self.name = name
        self.wikipedia_page_id = int(name.split("_")[-1])
        self.wikidata_id = None

    def __repr__(self):
        self.__str__()

    def __str__(self):
        return "wd:" + str(self.wikidata_id)


class Cluster:

    def __init__(self):
        self.name = ""
        self.id = 0
        self.entities = []

    @staticmethod
    def create_clusters(cluster_file):
        input_file = Path(cluster_file)
        cluster_id = 0
        cluster_queue = queue.Queue()
        last_cluster = None
        with input_file.open() as file:
            for line in file:
                if line.startswith(constant.CLUSTER_HEADLINE):
                    last_cluster = Cluster._create_cluster(cluster_id, line.replace("\n", ""))
                    cluster_queue.put(last_cluster)
                    cluster_id += 1
                    continue
                last_cluster.entities.append(Entity(line[:-1]))

        return cluster_queue

    @staticmethod
    def _create_cluster(cluster_id, line):
        cluster = Cluster()
        cluster.id = cluster_id
        cluster.name = line.split(" ")[-1]
        return cluster

    def fetch_wikidata_ids(self, mysql_connection):
        db_cursor = mysql_connection.cursor()
        entity_ids_list = ",".join([str(x.wikipedia_page_id) for x in self.entities])
        start_time = time.perf_counter()
        db_cursor.execute(f"SELECT pp_page, pp_value "
                          f"FROM page_props "
                          f"WHERE pp_propname LIKE 'wikibase_item' "
                          f"AND pp_page IN ({entity_ids_list});")
        end_time = time.perf_counter()
        logging.info(f"MySQL query execution took {end_time - start_time} seconds for {len(self.entities)} entities")

        start_time = time.perf_counter()
        mapping = {}
        for record in db_cursor:
            if not record[1]:
                print(record[0])
                continue

            mapping[record[0]] = record[1]

        db_cursor.close()

        for entity in self.entities:
            if entity.wikipedia_page_id not in mapping:
                # entity is not present in wikidata
                self.entities.remove(entity)
                continue

            entity.wikidata_id = mapping[entity.wikipedia_page_id]
        end_time = time.perf_counter()
        logging.info(f"Mapping WikiData ids took {end_time - start_time} seconds for {len(self.entities)} entities")
