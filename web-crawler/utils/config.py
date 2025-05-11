import yaml
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/config.yaml')

def get_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)