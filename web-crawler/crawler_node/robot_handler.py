import urllib.robotparser
from urllib.parse import urlparse

USER_AGENT = "DistributedCrawlerBot"

robot_parsers = {}

def is_allowed(url):
    domain = urlparse(url).netloc
    if domain not in robot_parsers:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"https://{domain}/robots.txt")
        try:
            rp.read()
        except:
            return True  # Assume allowed if can't fetch robots.txt
        robot_parsers[domain] = rp
    return robot_parsers[domain].can_fetch(USER_AGENT, url)

