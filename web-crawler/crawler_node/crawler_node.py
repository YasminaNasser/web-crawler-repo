import time
import requests
from crawler_node.extractor import extract_urls_and_text
from crawler_node.robot_handler import is_allowed
from utils.redis_handler import RedisQueue
from utils.logger import get_logger
from utils.config import get_config
import socket

config = get_config()
logger = get_logger('crawler')
redis_queue = RedisQueue(config['redis'])

CRAWLER_ID = socket.gethostname()

class Crawler:
    def __init__(self):
        self.delay = config.get('crawl_delay', 2)

    def send_heartbeat(self):
        redis_queue.set_heartbeat(CRAWLER_ID)

    def run(self):
        logger.info(f"Crawler {CRAWLER_ID} started.")
        while True:
            self.send_heartbeat()
            url = redis_queue.pop_url()
            if url is None:
                logger.info("No URL to crawl. Sleeping...")
                time.sleep(3)
                continue
            if not is_allowed(url):
                logger.info(f"Blocked by robots.txt: {url}")
                continue
            try:
                logger.info(f"Crawling URL: {url}")
                response = requests.get(url, timeout=10)
                links, text = extract_urls_and_text(response.text, url)
                redis_queue.push_content(url, text)
                for link in links:
                    redis_queue.push_url(link)
                logger.info(f"Crawled and pushed {len(links)} new links.")
                time.sleep(self.delay)
            except Exception as e:
                logger.error(f"Failed to crawl {url}: {e}")

if __name__ == '__main__':
    Crawler().run()