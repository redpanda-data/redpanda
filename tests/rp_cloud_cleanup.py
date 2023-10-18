import json
import logging
import os
import sys
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from rptest.services.redpanda_cloud import CloudClusterConfig

log = logging.getLogger()
gconfig_path = './'


def load_globals(path):
    _gconfig = {}
    _gpath = os.path.join(path, "globals.json")
    try:
        with open(_gpath, "r") as gf:
            _gconfig = json.load(gf)
    except Exception as e:
        log.error(f"# ERROR: globals.json not found; {e}")
    return _gconfig


class CloudCleanup():
    def __init__(self, ducktape_globals, log):
        self.log = log
        # Serialize params
        self.config = CloudClusterConfig(**ducktape_globals['cloud_cluster'])
        # Init the REST client
        self.cloudv2 = RpCloudApiClient(self.config, log)

    def clean_namespaces(self):
        self.log.info("# Cleaning namespaces")
        # TODO: Clean namespaces
        return


# Main script
def cleanup_entrypoint():
    # Try to load cluster config
    gconfig = load_globals(gconfig_path)
    if "cloud_cluster" not in gconfig:
        log.warn("# WARNING: Cloud cluster configuration not present, exiting")
        sys.exit(1)
    # Init the cleaner class
    cleaner = CloudCleanup(gconfig, log)

    # Namespaces
    cleaner.clean_namespaces()


if __name__ == "__main__":
    cleanup_entrypoint()
    sys.exit(0)
