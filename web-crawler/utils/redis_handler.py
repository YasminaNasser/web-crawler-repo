import redis
from utils.config import get_config

config = get_config()
redis_conn = redis.Redis(host=config['redis']['host'], port=config['redis']['port'], db=1)

# --- Per-job Visited ---
def is_visited(url, job_id):
    return redis_conn.sismember(f"job:{job_id}:visited", url)

def add_visited(url, job_id):
    redis_conn.sadd(f"job:{job_id}:visited", url)

# --- Per-job Queued ---
def is_queued(url, job_id):
    return redis_conn.sismember(f"job:{job_id}:queued", url)

def add_queued(url, job_id):
    redis_conn.sadd(f"job:{job_id}:queued", url)

# --- Optional: To-visit tracking (for dashboard counts) ---
def add_to_visit(url, job_id):
    redis_conn.lpush(f"job:{job_id}:to_visit", url)

def pop_to_visit(job_id):
    return redis_conn.rpop(f"job:{job_id}:to_visit")

def get_queued_count(job_id):
    return redis_conn.scard(f"job:{job_id}:queued")

def get_visited_count(job_id):
    return redis_conn.scard(f"job:{job_id}:visited")

def get_to_visit_count(job_id):
    return redis_conn.llen(f"job:{job_id}:to_visit")
