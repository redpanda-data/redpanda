import json
import logging
import re
import os
import sys

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urlparse

from rptest.services.cloud_cluster_utils import CloudClusterUtils
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


class FakeContext():
    def __init__(self, globals):
        self.globals = globals


class CloudCleanup():
    _ns_pattern = f"{ns_name_prefix}"
    # At this point all is disabled except namespaces
    delete_clusters = True
    delete_peerings = True
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

        # Prepare fake context
        _fake_context = FakeContext(ducktape_globals)

        # Serialize it to class
        if "cloud_cluster" not in ducktape_globals:
            self.log.warn("# WARNING: Cloud cluster configuration "
                          "not present, exiting")
            sys.exit(1)
        self.config = CloudClusterConfig(**ducktape_globals['cloud_cluster'])

        # Init the REST client
        self.cloudv2 = RpCloudApiClient(self.config, self.log)

        # Init Cloud Cluster Utils
        o = urlparse(self.config.oauth_url)
        oauth_url_origin = f'{o.scheme}://{o.hostname}'
        self.utils = CloudClusterUtils(_fake_context, self.log,
                                       ducktape_globals['s3_access_key'],
                                       ducktape_globals['s3_secret_key'],
                                       self.config.provider,
                                       self.config.api_url, oauth_url_origin,
                                       self.config.oauth_audience)

    def load_globals(self, path):
        _gconfig = {}
        _gpath = os.path.join(path, "globals.json")
        try:
            with open(_gpath, "r") as gf:
                _gconfig = json.load(gf)
        except Exception as e:
            self.log.error(f"# ERROR: globals.json not found; {e}")
        return _gconfig

    def _delete_cluster(self, handle):
        # Function detects cluster type and deletes it
        _cluster = self.cloudv2.get_resource(handle)
        _id = _cluster['id']
        _type = _cluster['spec']['clusterType']
        _state = _cluster['state']
        if _type in ['FMC']:
            # Just request deletion right away
            return self.cloudv2.delete_resource(handle)
        elif _type in ['BYOC']:
            # Check status and act in steps
            # In most cases cluisters will be in 'deleting_agent' status
            # I.e. past 36h that we skip for namespaces,
            # native CloudV2 cleaning will kick in and delete main cluster.
            # Only agent would be left

            # TODO: Implement deletion on rare non-agent statuses

            if _state in ['deleting_agent']:
                # Login
                try:
                    self.log.info(f"-> {handle} -> cloud login")
                    out = self.utils.rpk_cloud_login(
                        self.config.oauth_client_id,
                        self.config.oauth_client_secret)
                except Exception as e:
                    self.log.error(
                        f"# ERROR: failed to login as '{self.config.oauth_client_id}'"
                    )
                    return False

                # Delete agent
                try:
                    self.log.info(f"-> {handle} -> delete agent")
                    out = self.utils.rpk_cloud_agent_delete(_id)
                except Exception as e:
                    self.log.error(
                        f"# ERROR: failed to delete agent for cluster '{_id}'")
                    return False
                # All good
                return True
            else:
                self.log.warning(
                    f"# WARNING: Cluster deletion with status '{_state}' not yet implemented"
                )
        else:
            self.log.warning(f"# WARNING: Cloud type not supported: '{_type}'")
            return False

    def _pooled_delete(self, pool, resource_name, func, queue):
        _c = len(queue)
        if _c < 1:
            self.log.info(f"# {resource_name}: nothing to delete")
            return
        else:
            self.log.info(f"# {resource_name}: amount to cleanup {_c}")
            for r in pool.map(func, queue):
                if isinstance(r, bool):
                    if not r:
                        self.log.warning(f"# {resource_name}: not deleted")
                else:
                    self.log.debug("  output:\n{r}")
            return

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
        self.log.info("\n\n# Cleaning up")
        # Delete network peerings
        if not self.delete_peerings:
            self.log.info(
                f"# {len(npr_queue)} network peerings could be deleted")
        else:
            self._pooled_delete(pool, "Network peerings",
                                self.cloudv2.delete_resource, npr_queue)

        # Delete clusters
        if not self.delete_clusters:
            self.log.info(f"# {len(cl_queue)} clusters could be deleted")
        else:
            self._pooled_delete(pool, "Clusters", self._delete_cluster,
                                cl_queue)

        # Delete orphaned networks
        if not self.delete_networks:
            self.log.info(f"# {len(net_queue)} networks could be deleted")
        else:
            self._pooled_delete(pool, "Networks", self.cloudv2.delete_resource,
                                net_queue)

        # Delete namespaces
        if not self.delete_namespaces:
            self.log.info(f"# {len(ns_queue)} namespaces could be deleted")
        else:
            self._pooled_delete(pool, "Namespaces",
                                self.cloudv2.delete_resource, ns_queue)

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
