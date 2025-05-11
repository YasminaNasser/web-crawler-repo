from elasticsearch import Elasticsearch
import hashlib
from utils.config import get_config
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import re
from urllib.parse import urljoin

config = get_config()
ps = PorterStemmer()
stops = set(stopwords.words('english'))
# Access 'elasticsearch' section in the config
es_config = config.get('elasticsearch', {})
index_name = config.get('index', 'default_index')  # Default to 'default_index' if not found
custom_index_config = {
    "settings": {
        "analysis": {
            "analyzer": {
                "custom_english": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "stop", "porter_stem"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "url": { "type": "keyword" },
            "title": { "type": "text", "analyzer": "custom_english" },
               "body":  { "type": "text", "analyzer": "custom_english" }
        }
    }
}
# Add error handling for Elasticsearch config
if not es_config.get('host'):
    raise KeyError("Missing 'host' in Elasticsearch configuration")

class ElasticsearchIndexer:
    def __init__(self, config):
        self.es = Elasticsearch(hosts=[es_config['host']],
            verify_certs=False)
        self.index = index_name
        self._ensure_index()

    def _ensure_index(self):
        if not self.es.indices.exists(index=self.index):
            self.es.indices.create(index=self.index, body=custom_index_config)

    def index_document(self, url, title, body):
       clean_body = self.clean_text(body)
       doc = {
           "url": url,
           "title": title,
           "body": clean_body
             }
       self.es.index(index=self.index, body=doc)
       logger.info(f" Indexed: {url}")
    def clean_text(self,text):
         # Remove punctuation and lowercase
        text = re.sub(r'[^\w\s]', '', text.lower())
        words = text.split()
        stemmed = [ps.stem(w) for w in words if w not in stops]
        return ' '.join(stemmed)