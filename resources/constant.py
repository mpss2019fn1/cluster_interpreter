CLUSTER_HEADLINE = "[[CLUSTER"
WIKIDATA_API_URL = "https://query.wikidata.org/sparql"


def create_mysql_connection():
    import mysql.connector
    return mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="toor",
        database="mpss2019",
        auth_plugin="caching_sha2_password",
    )


def named_entity_relations_sparql_query(wikidata_ids):
    query = """SELECT DISTINCT ?person ?wdLabel ?ps_Label WHERE {
  VALUES (?person) {%1}
  
  ?person ?p ?statement .
  ?statement ?ps ?ps_ .
  
  ?wd wikibase:claim ?p.
  ?wd wikibase:statementProperty ?ps.
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
} ORDER BY ?wd ?statement ?ps_"""

    return query.replace("\n", " ").replace("%1", " ".join([f"({x})" for x in wikidata_ids]))
