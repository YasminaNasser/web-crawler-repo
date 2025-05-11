from utils.redis_handler import RedisQueue
from utils.logger import get_logger
from utils.config import get_config
from indexer_node.elasticsearch_handler import ElasticsearchIndexer
import socket
import time

config = get_config()
logger = get_logger('indexer')
redis_queue = RedisQueue(config['redis'])
indexer = ElasticsearchIndexer(config['elasticsearch'])

INDEXER_ID = socket.gethostname()

class Indexer:
    def __init__(self):
        self.sleep_time = config.get('index_poll_delay', 2)

    def run(self):
        logger.info(f"Indexer {INDEXER_ID} started.")
        while True:
            item = redis_queue.pop_content()
            if item is None:
                time.sleep(self.sleep_time)
                continue
            url, text = item.get('url'), item.get('text')
            if url and text:
                logger.info(f"Indexing URL: {url}")
                indexer.index_document(url, text)

if __name__ == '__main__':
    Indexer().run()