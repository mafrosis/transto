import functools

import yaml


@functools.lru_cache(maxsize=1)
def load_mapping() -> dict:
    'Load transaction mapping data'
    with open('mapping.yaml', encoding='utf8') as f:
        return yaml.safe_load(f).get('mapping')
