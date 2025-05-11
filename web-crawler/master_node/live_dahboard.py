from flask import Flask, render_template_string
import redis
from elasticsearch import Elasticsearch
from celery import Celery
from utils.config import get_config

app = Flask(__name__)
config = get_config()

redis_conn = redis.Redis(
    host=config['redis']['host'], port=config['redis']['port'], db=1
)
es = Elasticsearch(config['elasticsearch']['host'])
celery_app = Celery(broker=f"redis://{config['redis']['host']}:{config['redis']['port']}/0")

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Distributed Crawler Monitor</title>
    <style>
        body { font-family: Arial; background-color: #f8f8f8; margin: 40px; }
        h1 { color: #333; }
        .box { background: #fff; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 0 10px #ccc; }
        .ok { color: green; }
        .fail { color: red; }
    </style>
</head>
<body>
    <h1> Distributed Crawler Monitoring</h1>

    <div class="box">
        <h2>Redis Stats</h2>
        <p>Visited URLs: <strong>{{ visited }}</strong></p>
        <p>Queued URLs: <strong>{{ queued }}</strong></p>
        <p> To Visit: <strong>{{ to_visit }}</strong></p>
    </div>

    <div class="box">
        <h2>Elasticsearch Stats</h2>
        <p> Indexed Documents: <strong>{{ indexed }}</strong></p>
    </div>

    <div class="box">
        <h2>Crawler Heartbeats</h2>
        {% if heartbeats %}
            <ul>
            {% for node, status in heartbeats.items() %}
                <li><span class="ok"> {{ node }}</span></li>
            {% endfor %}
            </ul>
        {% else %}
            <p class="fail"> No active crawlers found!</p>
        {% endif %}
    </div>
</body>
</html>
"""

@app.route("/")
def dashboard():
    visited = redis_conn.scard("crawling:visited")
    queued = redis_conn.scard("crawling:queued")
    to_visit = redis_conn.llen("crawling:to_visit")

    try:
        indexed = es.count(index=config['elasticsearch']['index'])["count"]
    except Exception:
        indexed = "N/A"

    try:
        heartbeats = celery_app.control.ping(timeout=1)
        heartbeat_nodes = {node: "OK" for node in heartbeats} if heartbeats else {}
    except Exception:
        heartbeat_nodes = {}

    return render_template_string(TEMPLATE,
                                  visited=visited,
                                  queued=queued,
                                  to_visit=to_visit,
                                  indexed=indexed,
                                  heartbeats=heartbeat_nodes)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
