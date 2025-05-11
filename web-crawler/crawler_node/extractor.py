from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

def extract_urls_and_text(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    links = set()
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if not href.startswith('#'):
            full_url = urljoin(base_url, href)
            if re.match(r'^https?://', full_url):
                links.add(full_url)
    return list(links), text
