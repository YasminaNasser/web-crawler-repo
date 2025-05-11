from celery import Celery
from indexer_node.elasticsearch_handler import ElasticsearchIndexer
from utils.config import get_config

app = Celery('indexer', broker='redis://redis:6379/0')
config = get_config()
indexer = ElasticsearchIndexer(config['elasticsearch'])

@app.task
def send_to_indexer(url, text):
    indexer.index_document(url, text)
    return f"Indexed: {url}"

