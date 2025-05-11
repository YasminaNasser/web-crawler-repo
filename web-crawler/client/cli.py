import argparse
from elasticsearch import Elasticsearch
from utils.config import get_config
import redis

def parse_args():
    parser = argparse.ArgumentParser(description="Distributed Search CLI")
    parser.add_argument('--mode', choices=['phrase', 'bool'], default='phrase',
                        help='Search mode: phrase or bool (default: phrase)')
    return parser.parse_args()

def build_query(query, mode):
    if mode == 'phrase':
        return {
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "phrase",
                    "fields": ["title", "body"]
                }
            }
        }
    else:  # bool mode
        return {
            "query": {
                "query_string": {
                    "query": query,
                    "fields": ["title", "body"]
                }
            }
        }

def show_status(redis_host, redis_port):
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=1)
        crawled = r.scard("crawling:visited")
        queued = r.scard("crawling:queued")
        to_visit = r.llen("crawling:to_visit")
        print(f"\n STATUS: {crawled} crawled, {queued} in queue, {to_visit} to visit")
    except Exception as e:
        print(f"\nCould not fetch Redis status: {e}")

def main():
    args = parse_args()
    config = get_config()
    es_host = config['elasticsearch']['host']
    index_name = config['elasticsearch']['index']
    redis_conf = config.get('redis', {'host': 'localhost', 'port': 6379})

    es = Elasticsearch(es_host)

    print(" Distributed Search CLI")
    print(f"Mode: {args.mode}")
    print("Type your query (or 'exit' to quit):")

    while True:
        query = input("\nSearch> ").strip()
        if query.lower() == 'exit':
            break

        es_query = build_query(query, args.mode)
        try:
            response = es.search(index=index_name, body=es_query)

            hits = response['hits']['hits']
            print(f"\n Found {len(hits)} results:")
            for hit in hits:
                url = hit['_source'].get('url', 'N/A')
                title = hit['_source'].get('title', 'N/A')
                body = hit['_source'].get('body', '')[:150].replace('\n', ' ')
                score = hit.get('_score', 0)

                print(f"→ {title[:80]}")
                print(f"    {url}")
                print(f"    Score: {score:.2f}")
                print(f"   Snippet: {body}...\n")

            show_status(redis_conf['host'], redis_conf['port'])

        except Exception as e:
            print(f" Error during search: {e}")

if __name__ == "__main__":
    main()
