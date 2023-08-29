import requests
from concurrent.futures import ThreadPoolExecutor
from functools import partial


def _rget(uri, headers=None):
    return requests.get(uri, headers=headers)


def _build_instance_meta_tree(uri, path="", headers=None):
    """
    Recoursively calls target host/path for metadata values
    """
    # Call for value
    _r = _rget(f"{uri}{path}", headers=headers)
    # iterate results and call for child values
    if _r.ok:
        _l = []
        # iterate through values
        _items = _r.text.split()
        for item in _items:
            if item[-1] == '/':
                # this is a subpath, call it
                _path = f"{path}{item}"
                _l += _build_instance_meta_tree(uri,
                                                path=_path,
                                                headers=headers)
            else:
                _l.append(f"{path}{item}")
        return _l
    else:
        # on fail, return path and message as a value
        return [f"{path}: {_r.status_code}, '{_r.reason}'"]


def query_instance_meta(baseurl, headers=None):
    """
    Query meta from local node. Universal for AWS/GCP
    """
    # Multiple calls will be required to get all of the metadata
    # Walk the path and build list of values to get
    _path_list = _build_instance_meta_tree(baseurl, headers=headers)

    # flat dict to fill in with values
    # hierachical structure is not required here
    # due to overcomplications in resulting code
    # {
    #   "key-name": ("<path>", "value")
    # }
    _meta = {}
    # Use pool to get all of the metadata
    pool = ThreadPoolExecutor(max_workers=10)
    # Some real python right here
    _uris = list(map(str.__add__, [baseurl] * len(_path_list), _path_list))
    # Use threading pool to get all the values
    _kw = {"headers": headers}
    for r in pool.map(partial(_rget, **_kw), _uris):
        # create keyname
        _u = r.url.replace(baseurl, "")
        _p = "-".join(_u.split('/'))
        # Store value
        _meta[_p] = r.text
    # Kill the threads
    pool.shutdown(wait=False)
    # Return
    return _meta
