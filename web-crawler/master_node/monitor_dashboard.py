import redis
from elasticsearch import Elasticsearch
from utils.config import get_config

def redis_stats(r):
    print(" Redis Crawl Queue Stats:")
    print(f" - Visited URLs: {r.scard('crawling:visited')}")
    print(f" - Queued URLs:  {r.scard('crawling:queued')}")
    print(f" - To Visit:     {r.llen('crawling:to_visit')}\n")

def elasticsearch_stats(es, index):
    try:
        res = es.count(index=index)
        print(f"Elasticsearch Index Stats:")
        print(f" - Documents Indexed: {res['count']}\n")
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")

def main():
    config = get_config()
    redis_host = config['redis']['host']
    redis_port = config['redis']['port']
    es_host = config['elasticsearch']['host']
    es_index = config['elasticsearch']['index']

    r = redis.Redis(host=redis_host, port=redis_port, db=1)
    es = Elasticsearch(es_host)

    print(" Monitoring Report")
    print("====================")
    redis_stats(r)
    elasticsearch_stats(es, es_index)

if __name__ == "__main__":
    main()
