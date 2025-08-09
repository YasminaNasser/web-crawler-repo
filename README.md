# Distributed Web Crawler

This project is a fault-tolerant, distributed web crawler designed for scalability, efficient indexing, and advanced search capabilities. It can crawl large sets of URLs, store content for later retrieval, and provide powerful search features such as stemming and boolean queries.

## Features

- **Distributed Architecture** – Multiple workers process tasks in parallel for scalability.  
- **Fault Tolerance** – Heartbeat monitoring, timeout detection, and automatic task re-queuing.  
- **Advanced Indexing** – Supports Elasticsearch or Whoosh with stemming and boolean queries.  
- **Search Interface** – Web-based interface for querying indexed content.  
- **Monitoring Dashboard** – Real-time statistics on crawled URLs, indexing progress, and error rates.  
- **Data Persistence** – Stores crawled data in AWS S3 for durability.  

## Technology Stack

- **Backend:** Python, Flask  
- **Task Queue:** Celery with RabbitMQ  
- **Search Engine:** Elasticsearch or Whoosh  
- **Storage:** AWS S3  
- **Database:** Reddis 
- **Frontend:** HTML, CSS, JavaScript  


