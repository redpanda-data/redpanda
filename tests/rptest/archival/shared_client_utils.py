import re
from typing import Optional

expr: re.Pattern[str] = re.compile(
    r'.+/(.+)/(.+)/(\d+_\d+/|topic_manifest\.json|topic_manifest\.bin)')


def key_to_topic(key: str) -> Optional[str]:
    # Segment objects: <hash>/<ns>/<topic>/<partition>_<revision>/...
    # Manifest objects: <hash>/meta/<ns>/<topic>/<partition>_<revision>/...
    # Topic manifest objects: <hash>/meta/<ns>/<topic>/topic_manifest.json
    if m := expr.search(key):
        return m[2]
