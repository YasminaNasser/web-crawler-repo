from celery import Celery
from elasticsearch_handler import ElasticsearchIndexer
from utils.config import get_config

class IndexerWorker:
    def __init__(self):
        self.app = Celery('indexer', broker='redis://172.31.76.191:6379/0')
        self.config = get_config()
        self.indexer = ElasticsearchIndexer(self.config['elasticsearch'])
        self.register_tasks()

    def register_tasks(self):
        @self.app.task(name='indexer.send_to_indexer',queue='indexer_queue')
        def send_to_indexer(url, text):
            return self.index_document(url, text)

    def index_document(self, url, text):
        self.indexer.index_document(url, text)
        return f"Indexed: {url}"
   
if __name__ == "__main__":
    worker = IndexerWorker()
    worker.app.worker_main(argv=['worker', '--loglevel=info','--queues=indexer_queue'])