import boto3
import hashlib
import os

class S3Handler:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')

    def upload_page(self, url, title, body):
        key = self._generate_key(url)
        content = f"URL: {url}\nTitle: {title}\n\n{body}"
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=content.encode('utf-8'))
        return f"s3://{self.bucket_name}/{key}"

    def _generate_key(self, url):
        hashed = hashlib.sha256(url.encode()).hexdigest()
        return f"pages/{hashed}.txt"
