CLUSTER_HEADLINE = "[[CLUSTER"
WIKIDATA_API_URL = "https://query.wikidata.org/sparql"
MAX_NUMBER_OF_RELATIONS_PER_CLUSTER = 20
MAX_NUMBER_OF_VALUES_PER_RELATION = 20

RELATION_SOURCE_LABEL = "entity"
RELATION_NAME_LABEL = "wdLabel"
RELATION_TARGET_LABEL = "ps_Label"


def named_entity_relations_sparql_query(entities):
    query = """
    SELECT DISTINCT ?entity ?wdLabel ?ps_Label WHERE {
        VALUES (?entity) {%1}
      
        ?entity ?p ?statement .
        ?statement ?ps ?ps_ .
          
        ?wd wikibase:claim ?p.
        ?wd wikibase:statementProperty ?ps.
          
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
    } 
    ORDER BY ?wd ?statement ?ps_
    """
    wikidata_id_list = " ".join([f"(wd:{x})" for x in entities])
    return query \
        .replace("\n", " ") \
        .replace("%1", wikidata_id_list)
