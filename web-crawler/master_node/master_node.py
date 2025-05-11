from celery import Celery
from utils.logger import get_logger
from utils.config import get_config
import time
from celery.events import EventReceiver
import threading
from celery.app.control import Inspect
import redis

logger = get_logger('master')
config = get_config()

class Master:
    def __init__(self):
        # Connect to Redis (Celery broker)
        #self.app = Celery('master', broker='redis://localhost:6379/0')
        # Try connecting to Redis and initialize inspector
        self.config = get_config() 
        broker_url = f'amqp://{self.config["broker"]["user"]}:{self.config["broker"]["password"]}@{self.config["broker"]["host"]}:{self.config["broker"]["port"]}//'
        self.app = Celery('master', broker=broker_url)
        try:
            self._check_redis_connection()
            self.inspector = self.app.control.inspect()
            logger.info("Successfully connected to Redis and initialized inspector.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise e

    def _check_redis_connection(self):
        try:
            r = redis.Redis(host='172.31.76.191', port=6379)
            if not r.ping():
                raise Exception("Redis is not responding")
        except redis.exceptions.ConnectionError as e:
            raise Exception(f"Redis connection error: {e}")
    def assign_tasks(self, seed_urls):
        for url in seed_urls:
            self.app.send_task('crawler.crawl_page', args=[url,0],queue='crawler_queue' )
            logger.info(f"Dispatched crawl task for {url}")
    def start_heartbeat_monitor(self, interval=15):
        def monitor():
            while True:
                try:
                    pinged = self.inspector.ping()
                    logger.info("---- Crawler Heartbeat ----")
                    if pinged:
                        for node, status in pinged.items():
                            logger.info(f"{node} is alive")
                    else:
                        logger.warning("No crawler nodes are responding.")
                except Exception as e:
                    logger.error(f"Heartbeat error: {e}")
                time.sleep(interval)

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()

    def start_event_monitor(self):
        def on_event(event):
            logger.info(f"[EVENT] {event['type']} | ID: {event.get('uuid')} | Task: {event.get('name')} | State: {event.get('state', '-')}")

        def capture_events():
            with self.app.connection() as conn:
                recv = EventReceiver(conn, handlers={"*": on_event})
                logger.info("Started task event monitoring...")
                recv.capture(limit=None, timeout=None, wakeup=True)

        thread = threading.Thread(target=capture_events, daemon=True)
        thread.start()

    def run(self):
        seed_urls = config['seeds']
        self.assign_tasks(seed_urls)
        self.start_heartbeat_monitor()
        self.start_event_monitor()
        logger.info("Master is now running and monitoring crawlers.")
        while True:
            time.sleep(60)


if __name__ == "__main__":
    master = Master()
    master.run()
