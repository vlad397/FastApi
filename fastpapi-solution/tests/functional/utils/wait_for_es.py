import backoff
import elasticsearch
from elasticsearch.client import Elasticsearch


@backoff.on_exception(backoff.expo,
                      elasticsearch.ConnectionError,
                      max_time=300)
def wait_for_es(es):
    es.ping()


if __name__ == "__main__":

    es = Elasticsearch('elasticsearch:9200')

    wait_for_es(es)
