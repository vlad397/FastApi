import backoff
import elasticsearch
from elasticsearch.client import Elasticsearch

from functional.settings import ES_HOST, ES_PORT


@backoff.on_exception(backoff.expo,
                      elasticsearch.ConnectionError,
                      max_time=300)
def wait_for_es(es):
    es.ping()


if __name__ == "__main__":

    es = Elasticsearch([f'{ES_HOST}:{ES_PORT}'])

    wait_for_es(es)
