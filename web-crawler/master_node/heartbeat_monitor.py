from celery import Celery
import time
from utils.logger import get_logger
from utils.config import get_config

config = get_config()
logger = get_logger("heartbeat")

app = Celery('master', broker=f"redis://{config['redis']['host']}:{config['redis']['port']}/0")

def monitor_crawlers():
    while True:
        try:
            i = app.control.inspect()
            pinged = i.ping()

            logger.info("---- Crawler Heartbeat Check ----")
            if pinged:
                for node, status in pinged.items():
                    logger.info(f" {node} is alive")
            else:
                logger.warning(" No crawler workers are responding.")
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
        time.sleep(15)

if __name__ == "__main__":
    monitor_crawlers()
