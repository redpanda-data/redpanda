import re
from typing import Optional


def key_to_topic(key: str) -> Optional[str]:
    # Segment objects: <hash>/<ns>/<topic>/<partition>_<revision>/...
    # Manifest objects: <hash>/meta/<ns>/<topic>/<partition>_<revision>/...
    # Topic manifest objects: <hash>/meta/<ns>/<topic>/topic_manifest.json
    m = re.search(".+/(.+)/(.+)/(\d+_\d+/|topic_manifest.json)", key)
    if m is None:
        return None
    else:
        return m.group(2)
