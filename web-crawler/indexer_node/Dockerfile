FROM python:3.10-slim

WORKDIR /app
COPY ../ ./
RUN pip install -r requirements.txt

CMD ["celery", "-A", "indexer_node.tasks", "worker", "--loglevel=info"]
