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


# This is a duplicate code combined from ec2 and gcp clients in order to
# be able to get meta using cluster.node[?].account.ssh_output
# The reason for this is that EC2 metadata service available only
# within the host and not externally
# TODO: Consolidate code in one place and refactor its use in HTT tests
if __name__ == "__main__":
    import sys
    import json

    # get provider from command line
    provider = None
    if len(sys.argv) < 2:
        sys.stderr.write("ERROR: missing provider type")
        sys.stderr.flush()
        sys.exit(1)
    else:
        provider = sys.argv[1].upper()
    # get metadata according to provider
    if provider == 'AWS':
        _prefix = "http://"
        _suffix = "/latest/meta-data/"
        _target = "169.254.169.254"
        uri = f"{_prefix}{_target}{_suffix}"
        # Get meta
        _meta = query_instance_meta(uri, headers=None)
        # swith macs for device ids
        # filter out keys for network interfaces
        # _old_keys = filter(lambda x: x.startswith('interfaces-macs'), _meta)
        # iterate keys and build new dict
        _new_meta = {}
        _head = "network-interfaces-macs-"
        for k, v in _meta.items():
            if not k.startswith(_head):
                _new_meta[k] = v
            else:
                # current mac
                _tail = k.replace(_head, "")
                _mac = _tail[:_tail.index('-')] if '-' in _tail else ""
                _tail = _tail[_tail.index('-'):] if '-' in _tail else _tail
                # if this is not interface key just copy
                # get id and create new key
                _id = _meta[_head + _mac + "-device-number"]
                _new_meta[_head + _id + _tail] = v
        _meta = _new_meta
    elif provider == 'GCP':
        _prefix = "http://"
        _suffix = "/computeMetadata/v1/instance/"
        _target = "169.254.169.254"
        uri = f"{_prefix}{_target}{_suffix}"
        _meta = query_instance_meta(uri, headers={"Metadata-Flavor": "Google"})
    else:
        assert False, f"bad provider: {provider}"

    sys.stdout.write(json.dumps(_meta))
    sys.stdout.flush()
    sys.exit(0)
