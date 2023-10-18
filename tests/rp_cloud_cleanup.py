import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from rptest.services.redpanda_cloud import CloudClusterConfig

gconfig_path = './'


def setupLogger(level):
    root = logging.getLogger()
    root.setLevel(level)
    # Console
    cli = logging.StreamHandler(sys.stdout)
    cli.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    cli.setFormatter(formatter)
    root.addHandler(cli)

    return root


class CloudCleanup():
    _ns_pattern = "rp-ducktape-ns-"

    def __init__(self, log_level=logging.INFO):
        self.log = setupLogger(log_level)
        # Load config
        ducktape_globals = self.load_globals(gconfig_path)

        # Serialize it to class
        if "cloud_cluster" not in ducktape_globals:
            self.log.warn("# WARNING: Cloud cluster configuration "
                          "not present, exiting")
            sys.exit(1)
        self.config = CloudClusterConfig(**ducktape_globals['cloud_cluster'])

        # Init the REST client
        self.cloudv2 = RpCloudApiClient(self.config, self.log)

    def load_globals(self, path):
        _gconfig = {}
        _gpath = os.path.join(path, "globals.json")
        try:
            with open(_gpath, "r") as gf:
                _gconfig = json.load(gf)
        except Exception as e:
            self.log.error(f"# ERROR: globals.json not found; {e}")
        return _gconfig

    def clean_namespaces(self):
        """
            Function lists non-deleted namespaces and hierachically deletes
            clusters, networks and network-peerings if any
        """
        self.log.info("# Cleaning namespaces")
        # Items to delete
        _queue = []

        # Get namespaces
        ns_list = self.cloudv2.list_namespaces()
        # filter out
        self.log.info(f" {len(ns_list)} total namespaces found. "
                      f"Filtering with '{self._ns_pattern}*'")
        ns_list = [n for n in ns_list
                   if n['name'].startswith(self._ns_pattern)]
        # Check which ones are empty
        # The areas that are checked is clusters, networks and network-peerings
        self.log.info(f" {len(ns_list)} namespaces to process")
        for ns in ns_list:
            clusters = self.cloudv2.list_clusters(ns_uuid=ns['id'])
            networks = self.cloudv2.list_networks(ns_uuid=ns['id'])
            # check if any peerings exists
            peerings = []
            for net in networks:
                peerings += self.cloudv2.list_network_peerings(
                    net['id'], ns_uuid=ns['id'])
            counts = [len(clusters), len(networks), len(peerings)]
            if any(counts):
                self.log.warning(f"# WARNING: Namespace '{ns['name']}' "
                                 f"not empty: {counts[0]} clusters,"
                                 f" {counts[1]} networks,"
                                 f" {counts[2]} peering connections")
                # TODO: Cleanup clusters
                # TODO: Cleanup networks
                # TODO: Cleanup network peerings
                pass
            else:
                # Add delete handle to the list
                _queue += [self.cloudv2.namespace_endpoint(uuid=ns['id'])]
                # ##### debug
                # if len(_queue) > 10:
                #    break
                # ##### debug
        # Use ThreadPool to delete them
        self.log.info(f"# Deleting {len(_queue)} namespaces")
        pool = ThreadPoolExecutor(max_workers=5)
        for r in pool.map(self.cloudv2.delete_resource, _queue):
            if not r['deleted']:
                self.log.warning(f"# Namespace '{r['name']}' not deleted")
        # Cleanup thread resources
        pool.shutdown()

        return


# Main script
def cleanup_entrypoint():
    # Init the cleaner class
    cleaner = CloudCleanup()

    # Namespaces
    cleaner.clean_namespaces()


if __name__ == "__main__":
    cleanup_entrypoint()
    sys.exit(0)
