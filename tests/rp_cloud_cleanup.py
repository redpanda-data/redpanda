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
    # At this point all is disabled except namespaces
    delete_clusters = False
    delete_peerings = False
    delete_networks = False
    delete_namespaces = True

    def __init__(self, log_level=logging.INFO):
        self.log = setupLogger(log_level)
        self.log.info("# CloudV2 resourse cleanup util")
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

    def _delete_peerings(self, pool, queue):
        if not self.delete_peerings:
            self.log.info(f"# {len(queue)} network peerings could be deleted")
        else:
            self.log.info(f"# Deleting {len(queue)} network peerings")
            for r in pool.map(self.cloudv2.delete_resource, queue):
                # TODO: Check on real peering network delete request
                if not r:
                    self.log.warning(f"# Network peering '{r['name']}' "
                                     "not deleted")

    def _delete_clusters(self, pool, queue):
        if not self.delete_clusters:
            self.log.info(f"# {len(queue)} clusters could be deleted")
        else:
            self.log.info(f"# Deleting {len(queue)} clusters")
            for r in pool.map(self.cloudv2.delete_resource, queue):
                if r['state'] != 'deleted':
                    self.log.warning(f"# Cluster '{r['name']}' not deleted")

    def _delete_networks(self, pool, queue):
        if not self.delete_networks:
            self.log.info(f"# {len(queue)} networks could be deleted")
        else:
            self.log.info(f"# Deleting {len(queue)} networks")
            for r in pool.map(self.cloudv2.delete_resource, queue):
                if r['state'] != 'deleted':
                    self.log.warning(f"# Network '{r['name']}' not deleted")

    def _delete_namespaces(self, pool, queue):
        if not self.delete_namespaces:
            self.log.info(f"# {len(queue)} namespaces could be deleted")
        else:
            self.log.info(f"# Deleting {len(queue)} namespaces")
            for r in pool.map(self.cloudv2.delete_resource, queue):
                if not r['deleted']:
                    self.log.warning(f"# Namespace '{r['name']}' not deleted")

    def clean_namespaces(self):
        """
            Function lists non-deleted namespaces and hierachically deletes
            clusters, networks and network-peerings if any
        """
        self.log.info("# Listing namespaces")
        # Items to delete
        cl_queue = []
        net_queue = []
        npr_queue = []
        ns_queue = []

        # Get namespaces
        ns_list = self.cloudv2.list_namespaces()
        # filter out
        self.log.info(f" {len(ns_list)} total namespaces found. "
                      f"Filtering with '{self._ns_pattern}*'")
        ns_list = [
            n for n in ns_list if n['name'].startswith(self._ns_pattern)
        ]
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
                # TODO: Cleanup network peerings
                if counts[2]:
                    for peernet in peerings:
                        npr_queue += [
                            self.cloudv2.network_peering_endpoint(
                                id=peernet['net_id'], peering_id=peernet['id'])
                        ]
                # TODO: Cleanup clusters
                if counts[0]:
                    for cluster in clusters:
                        # TODO: Check creation date
                        # Add cluster to queue
                        cl_queue += [
                            self.cloudv2.cluster_endpoint(cluster['id'])
                        ]
                # TODO: Cleanup networks
                if counts[1]:
                    for net in networks:
                        net_queue += [self.cloudv2.network_endpoint(net['id'])]
            else:
                # Add delete handle to the list
                ns_queue += [self.cloudv2.namespace_endpoint(uuid=ns['id'])]
                # #### debug purposes while in draft mode
                if len(ns_queue) > 10:
                    break
                # #### debug
        # Use ThreadPool
        pool = ThreadPoolExecutor(max_workers=5)
        # Delete network peerings
        self._delete_peerings(pool, npr_queue)
        # Delete clusters
        self._delete_clusters(pool, cl_queue)
        # Delete orphaned networks
        self._delete_networks(pool, net_queue)
        # Delete namespaces
        self._delete_namespaces(pool, ns_queue)
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
