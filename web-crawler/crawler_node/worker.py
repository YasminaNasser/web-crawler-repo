import threading
import time
from celery import Celery
from extractor import extract_urls_and_text
from robot_handler import is_allowed
import requests
from utils.redis_handler import is_visited, add_visited
from utils.config import get_config
from utils.logger import get_logger



config = get_config()
logger = get_logger("crawler")

class CrawlerWorker:
    def __init__(self):
        self.app = Celery('crawler', broker='redis://172.31.76.191:6379/0')
        self.register_tasks()

    def register_tasks(self):
        @self.app.task(name='crawler.crawl_page',queue='crawler_queue')
        def crawl_page(url):
            return self.crawl_page(url)

    def crawl_page(self, url):
        if is_visited(url):
            print(f"Already visited {url}, skipping...")
            return f"Skipped {url} (already visited)"
        
        add_visited(url)
        if not is_allowed(url):
            return f"Blocked by robots.txt: {url}"

        try:
            response = requests.get(url, timeout=10)
            links, text = extract_urls_and_text(response.text, url)
            print(f"Fetched URL: {url}, status code: {response.status_code}")

            print(f"Parsing content for {url}")
            # Send content to the indexer (as another Celery task)

            self.app.send_task('indexer.send_to_indexer', args=[url, text], queue='indexer_queue')   
            MAX_LINKS=10
            for link in links[:MAX_LINKS]:
                if not is_visited(link):
                    self.app.send_task('crawler.crawl_page', args=[link])
                    print(f"Queued crawling for {link}")
            return f"Crawled: {url} with {len(links)} links"
        except Exception as e:
            return f"Error crawling {url}: {e}"

def start_multiple_crawlers(thread_count=5):
    worker = CrawlerWorker()

    def start_worker_thread():
        print("Starting a new crawler thread...")
        worker.app.worker_main(argv=['worker', '--loglevel=info'])

    threads = []
    for _ in range(thread_count):
        t = threading.Thread(target=start_worker_thread, daemon=True)
        threads.append(t)
        t.start()

    # Keep main thread alive
    while True:
        time.sleep(60)

if __name__ == "__main__":
    start_multiple_crawlers(thread_count=5)  # you can change 5 to 10, 20, etc.   is there something here that causes stucking after first task?
