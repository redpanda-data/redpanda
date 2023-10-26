import json
import logging
import re
import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from rptest.services.redpanda_cloud import CloudClusterConfig, ns_name_date_fmt, \
    ns_name_prefix

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
    _ns_pattern = f"{ns_name_prefix}"
    # At this point all is disabled except namespaces
    delete_clusters = False
    delete_peerings = False
    delete_networks = False
    delete_namespaces = True

    ns_regex = re.compile("^(?P<prefix>" + f"{ns_name_prefix}" + ")"
                          "(?P<date>\d{4}-\d{2}-\d{2}-\d{6}-)?"
                          "(?P<id>[a-zA-Z0-9]{8})$")

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
        self.log.info(f"  {len(ns_list)} total namespaces found. "
                      f"Filtering with '{self._ns_pattern}*'")
        # filter namespaces according to 'ns_name_prefix'
        ns_list = [
            n for n in ns_list if n['name'].startswith(self._ns_pattern)
        ]
        # Processing namespaces
        self.log.info(
            f"# Searching for resources in {len(ns_list)} namespaces")
        # Fugure out which time was 36h back
        back_36h = datetime.now() - timedelta(hours=36)
        for ns in ns_list:
            # Filter out according to dates in name
            # Detect date
            ns_match = self.ns_regex.match(ns['name'])
            date = ns_match['date']
            if date is not None:
                # Parse date into datetime object
                ns_creation_date = datetime.strptime(date, ns_name_date_fmt)
                if ns_creation_date > back_36h:
                    self.log.info(f"  skipped '{ns['name']}', "
                                  "36h delay not passed")
                    continue
            # Check which ones are empty
            # The areas that are checked is clusters, networks and network-peerings
            clusters = self.cloudv2.list_clusters(ns_uuid=ns['id'])
            networks = self.cloudv2.list_networks(ns_uuid=ns['id'])
            # check if any peerings exists
            peerings = []
            for net in networks:
                peerings += self.cloudv2.list_network_peerings(
                    net['id'], ns_uuid=ns['id'])
            # Calculate existing resources for this namespace
            counts = [len(clusters), len(networks), len(peerings)]
            if any(counts):
                # Announce counts
                self.log.warning(f"# WARNING: Namespace '{ns['name']}' "
                                 f"not empty: {counts[0]} clusters,"
                                 f" {counts[1]} networks,"
                                 f" {counts[2]} peering connections")
                # TODO: Prepare needed data for peerings
                if counts[2]:
                    for peernet in peerings:
                        # Add peerings delete handle to own list
                        npr_queue += [
                            self.cloudv2.network_peering_endpoint(
                                id=peernet['net_id'], peering_id=peernet['id'])
                        ]
                # TODO: Prepare needed data for clusters
                if counts[0]:
                    for cluster in clusters:
                        # TODO: Check creation date
                        # Add cluster delete handle to own list
                        cl_queue += [
                            self.cloudv2.cluster_endpoint(cluster['id'])
                        ]
                # TODO: Prepare needed data for networks
                if counts[1]:
                    for net in networks:
                        # Add network delete handle to own list
                        net_queue += [self.cloudv2.network_endpoint(net['id'])]
                # At this point, we should not add namespace to cleaning
                # if it has any resources. Just leave it to the next run
                continue
            # Add ns delete handle to the list
            ns_queue += [self.cloudv2.namespace_endpoint(uuid=ns['id'])]
        # Use ThreadPool
        # Deletion can be done only in this order:
        # Peerings - > clusters -> networks -> namespaces
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
