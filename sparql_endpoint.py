import requests


class QueryMalformedError(Exception):
    pass


class SparqlEndpoint:

    def __init__(self, remote_url):
        self.remote_url = remote_url

    def query(self, query):
        request = requests.get(self.remote_url, params={"format": "json", "query": query})
        for query_result in request.json()['results']['bindings']:
            yield {key: query_result[key]["value"] for key in query_result.keys()}
