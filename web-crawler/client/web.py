from flask import Flask, render_template_string, request, redirect
import redis
from elasticsearch import Elasticsearch
from celery import Celery
import uuid

from utils.config import get_config

app = Flask(__name__)
config = get_config()

# Setup Redis, Elasticsearch, Celery
redis_conn = redis.Redis(host=config['redis']['host'], port=config['redis']['port'], db=1)
es = Elasticsearch(config['elasticsearch']['host'])
celery_app = Celery('client', broker=f"{config['broker']['url']}")

# HTML template (embedded)
TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Client Dashboard</title>
    <style>
        body { font-family: Arial; background: #f0f2f5; padding: 30px; }
        h1 { color: #222; }
        .box { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 0 8px #ccc; margin-bottom: 20px; }
        input, textarea { width: 100%; margin-top: 10px; padding: 10px; }
        button { margin-top: 10px; padding: 10px 20px; }
        .result { margin-top: 10px; background: #eef; padding: 10px; border-radius: 6px; }
    </style>
</head>
<body>
    <h1>🌐 Crawler Client Dashboard</h1>

    <div class="box">
        <h2>Submit Seed URLs</h2>
        <form method="POST" action="/submit">
            <textarea name="urls" rows="4" placeholder="One URL per line" required></textarea>
            <input type="number" name="depth" value="1" min="1" max="5" required>
            <button type="submit">Submit Crawl</button>
        </form>
    </div>

    <div class="box">
        <h2>Search Your Results</h2>
        <form method="GET" action="/search">
            <input type="text" name="client_id" placeholder="Your Client ID" required>
            <input type="text" name="q" placeholder="Search..." required>
            <button type="submit">Search</button>
        </form>

        {% if results %}
            <h3>Results:</h3>
            {% for r in results %}
                <div class="result">
                    <strong><a href="{{ r.url }}" target="_blank">{{ r.url }}</a></strong><br>
                    <p>{{ r.content[:300] }}...</p>
                </div>
            {% endfor %}
        {% endif %}
    </div>

    <div class="box">
        <h2>Status</h2>
        <p>Queued URLs: {{ stats.queued }}</p>
        <p>Visited URLs: {{ stats.visited }}</p>
        <p>To Visit: {{ stats.to_visit }}</p>
        <p>Indexed Docs: {{ stats.indexed }}</p>
        <p>Celery Workers: {{ stats.heartbeat }}</p>
    </div>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def home():
    stats = {
        "queued": redis_conn.scard("crawling:queued"),
        "visited": redis_conn.scard("crawling:visited"),
        "to_visit": redis_conn.llen("crawling:to_visit"),
        "indexed": es.count(index=config['elasticsearch']['index'])["count"] if es.ping() else "N/A",
        "heartbeat": [x['hostname'] for x in celery_app.control.ping(timeout=1)] if celery_app else []
    }
    return render_template_string(TEMPLATE, results=None, stats=stats)

@app.route("/submit", methods=["POST"])
def submit():
    urls = request.form["urls"].strip().splitlines()
    depth = int(request.form.get("depth", 1))
    client_id = str(uuid.uuid4())

    try:
        response = requests.post("http://<MASTER_NODE_IP>:5001/api/submit", json={
            "urls": urls,
            "depth": depth,
            "client_id": client_id
        })
        if response.status_code == 200:
            return f"Crawl submitted to Master! Your client ID is: <b>{client_id}</b>"
        else:
            return f"Master node error: {response.text}", 500
    except Exception as e:
        return f"Failed to contact Master Node: {e}", 500


@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q")
    client_id = request.args.get("client_id")
    results = []
    if es.ping():
        try:
            response = es.search(index=config['elasticsearch']['index'], query={
                "bool": {
                    "must": [
                        {"multi_match": {"query": query, "fields": ["title", "body"]}},
                        {"match": {"client_id": client_id}}
                    ]
                }
            })
            for hit in response["hits"]["hits"]:
                results.append({
                    "url": hit["_source"]["url"],
                    "content": hit["_source"].get("body", "")
                })
        except Exception:
            pass

    stats = {
        "queued": redis_conn.scard("crawling:queued"),
        "visited": redis_conn.scard("crawling:visited"),
        "to_visit": redis_conn.llen("crawling:to_visit"),
        "indexed": es.count(index=config['elasticsearch']['index'])["count"] if es.ping() else "N/A",
        "heartbeat": [x['hostname'] for x in celery_app.control.ping(timeout=1)] if celery_app else []
    }
    return render_template_string(TEMPLATE, results=results, stats=stats)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
