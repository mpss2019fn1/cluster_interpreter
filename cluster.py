import queue
from pathlib import Path
from resources import constant


class Entity:

    def __init__(self, name):
        self.name = name
        self.wikipedia_page_id = name.split("_")[-1]
        self.wikidata_id = None


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
                    last_cluster = Cluster._create_cluster(cluster_id, line)
                    cluster_queue.put(last_cluster)
                    cluster_id += 1
                    continue
                last_cluster.entities.append(Entity(line))

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
        db_cursor.execute(f"SELECT pp_page, pp_value "
                                   f"FROM page_props "
                                   f"WHERE pp_propname = 'wikibase_item' "
                                   f"AND pp_page IN ({entity_ids_list});")
        records = db_cursor.fetch_all()
        db_cursor.close()

        wikipedia_to_wikidata_key_mapping = {}
        for record in records:
            wikipedia_to_wikidata_key_mapping[record[0]] = record[1]

        for entity in self.entities:
            entity.wikidata_id = wikipedia_to_wikidata_key_mapping[entity.wikipedia_page_id]


