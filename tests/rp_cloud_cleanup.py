import json
import logging
import re
import os
import subprocess
import sys
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urlparse

from rptest.services.cloud_cluster_utils import CloudClusterUtils
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from rptest.services.provider_clients import make_provider_client
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


def shell(cmd):
    # Run single command
    _cmd = cmd.split(" ")
    p = subprocess.Popen(_cmd, stdout=subprocess.PIPE, text=True)
    p.wait()
    # Format outout as String object
    _out = "\n".join(list(p.stdout)).strip()
    # _rcode = p.returncode
    p.kill()
    return _out


class FakeContext():
    def __init__(self, globals):
        # Check if rpk present
        if not os.path.exists(globals['rp_install_path_root']):
            # Get correct path
            _path = shell("which rpk")
            globals['rp_install_path_root'] = _path.replace('/bin/rpk', "")
        self.globals = globals


class CloudCleanup():
    # At this point all is disabled except namespaces
    delete_clusters = True
    delete_peerings = True
    delete_networks = True
    delete_namespaces = True

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
        self.config.provider = self.config.provider.upper()
        if self.config.provider == "AWS":
            _keyId = ducktape_globals['s3_access_key']
            _secret = ducktape_globals['s3_secret_key']
        elif self.config.provider == "GCP":
            _keyId = self.config.gcp_keyfile
            _secret = None
        else:
            self.log.error(f"# ERROR: Provider {self.config.provider} "
                           "not supported")
            sys.exit(1)
        # Initialize clients
        self.provider = make_provider_client(self.config.provider, self.log,
                                             self.config.region, _keyId,
                                             _secret)
        #self.log.info(f"# Provider {self.config.provider} initialized. Running"
        #              f" as '{self.provider.get_caller_identity()['Arn']}'")
        self.utils = CloudClusterUtils(_fake_context, self.log, _keyId,
                                       _secret, self.config.provider,
                                       self.config.api_url, oauth_url_origin,
                                       self.config.oauth_audience)

        self.log.info(f"# Using CloudV2 API at '{self.config.api_url}'")

        # Fugure out which time was 36h back
        self.back_36h = datetime.now() - timedelta(hours=36)
        self.back_8h = datetime.now() - timedelta(hours=8)

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
        def _ensure_date(creation_date):
            # False if it is not old enough
            if creation_date > self.back_36h:
                return False
            else:
                return True

        def _log_skip(_msg):
            _msg += f"| skipped '{_cluster['name']}', 36h delay not passed"
            self.log.info(_msg)

        def _log_deleted(_msg):
            _msg += "| deleted"
            self.log.info(_msg)

        # Function detects cluster type and deletes it
        _cluster = self.cloudv2.get_resource(handle)
        _id = _cluster['id']
        _type = _cluster['spec']['clusterType']
        _state = _cluster['state']
        _createdDate = datetime.strptime(_cluster['createdAt'],
                                         "%Y-%m-%dT%H:%M:%S.%fZ")
        _message = f"-> cluster '{_cluster['name']}', " \
                   f"{_cluster['createdAt']}, {_type} "
        if _type in ['FMC']:
            _message += f"| status: '{_cluster['state']}' "
            if _state in ['ready']:
                if not _ensure_date(_createdDate):
                    _log_skip(_message)
                    return False
                else:
                    # Just request deletion right away
                    out = self.cloudv2.delete_resource(handle)
                    _log_deleted(_message)
                    return out
            else:
                _message += "| skipping non-ready FMC clouds"
                self.log.info(_message)
        elif _type in ['BYOC']:
            # Check if provider is the same
            # This is relevant only for BYOC as
            # rpk will not be able to delete it anyway
            if _cluster['spec']['provider'].lower(
            ) != self.config.provider.lower():
                # Wrong provider, can't delete right now
                _message += "| SKIP: Can't delete " \
                            f"'{_cluster['spec']['provider']}' " \
                            f"cluster using '{self.config.provider}' creds"
                self.log.info(_message)
                return False

            # Check status and act in steps
            # In most cases cluisters will be in 'deleting_agent' status
            # I.e. past 36h that we skip for namespaces,
            # native CloudV2 cleaning will kick in and delete main cluster.
            # Only agent would be left

            # If the cluster in deleleting_agent, we should clean it
            # regardless of time or anything else to clean up quota
            if _cluster['state'] in ['deleting_agent']:
                _message += f"| status: '{_cluster['state']}' "
                # Login
                try:
                    _message += "| cloud login "
                    out = self.utils.rpk_cloud_login(
                        self.config.oauth_client_id,
                        self.config.oauth_client_secret)
                except Exception as e:
                    _message += "| ERROR: failed to login as " \
                                f"'{self.config.oauth_client_id}': {e}"
                    self.log.error(_message)
                    return False

                # Delete agent
                try:
                    _message += "| delete agent "
                    out = self.utils.rpk_cloud_agent_delete(_id)
                except Exception as e:
                    _message += "| ERROR: failed to delete agent for " \
                                f"cluster '{_id}': {e}"
                    self.log.info(_message)
                    return False
                # All good
                _log_deleted(_message)
                return True
            if _state in ['creating_agent']:
                _message += f"| status: '{_cluster['state']}' "
                # Check date, and delete only old ones
                if _ensure_date(_createdDate):
                    _log_skip(_message)
                    return False
                # Just delete the cluster resource
                out = self.cloudv2.delete_resource(handle)
                _log_deleted(_message)
                self.log.debug(f"Returned:\n{out}")
                return True
            # All other states need to wait for 36h
            if _ensure_date(_createdDate):
                _log_skip(_message)
                return False
            # TODO: Implement deletion on rare non-agent statuses
            else:
                _message += "| WARNING: Cluster deletion with status " \
                            f"'{_state}' not yet implemented"
                self.log.warning(_message)
                return False
        else:
            _message += f"| WARNING: Cloud type not supported: '{_type}'"
            self.log.warning(_message)
            return False

    def _pooled_delete(self, pool, resource_name, func, queue):
        _c = len(queue)
        if _c < 1:
            self.log.info(f"# {resource_name}: nothing to delete")
            return
        else:
            self.log.info(f"# {resource_name}: amount to cleanup {_c}")
            _ok = 0
            _failed = 0
            _unsure = 0
            for r in pool.map(func, queue):
                if isinstance(r, bool):
                    if not r:
                        _failed += 1
                    else:
                        _ok += 1
                else:
                    _unsure += 1
                    self.log.debug("  output:\n{r}")
            self.log.info(f"# Clusters: {_ok} deleted, "
                          f"{_failed} failed, {_unsure} other")
            return

    def clean_namespaces(self, pattern, uuid_len):
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
        # Prepare regex for matching namespaces
        ns_regex = re.compile("^(?P<prefix>" + f"{pattern}" + ")"
                              "(?P<date>\d{4}-\d{2}-\d{2}-\d{6}-)?"
                              "(?P<id>[a-zA-Z0-9]{" + f"{uuid_len}" + "})$")

        # Get namespaces
        ns_list = self.cloudv2.list_namespaces()
        self.log.info(f"  {len(ns_list)} total namespaces found. "
                      f"Filtering with '{pattern}*'")
        # filter namespaces according to 'ns_name_prefix'
        ns_list = [n for n in ns_list if n['name'].startswith(pattern)]
        # Processing namespaces
        self.log.info(
            f"# Searching for resources in {len(ns_list)} namespaces")
        for ns in ns_list:
            # Filter out according to dates in name
            # Detect date
            ns_match = ns_regex.match(ns['name'])

            if ns_match is None:
                continue
            date = ns_match['date'] if 'date' in ns_match.groups() else None
            _ns_36h_skip_flag = False
            if date is not None:
                # Parse date into datetime object
                ns_creation_date = datetime.strptime(date, ns_name_date_fmt)
                if ns_creation_date > self.back_36h:
                    _ns_36h_skip_flag = True
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
                                id=peernet['networkId'],
                                peering_id=peernet['id'])
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
            if _ns_36h_skip_flag:
                self.log.info(f"  skipped '{ns['name']}', "
                              "36h delay not passed")
                continue
            else:
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

    def _delete_nat(self, nat):
        """
            Function releases EIP and deletes NAT
        """

        # Delete Nat
        # self.log.info(f"-> deleting NAT: {nat['NatGatewayId']}")
        _start = time.time()
        r = self.provider.delete_nat(nat['NatGatewayId'])
        _now = time.time()
        # 180 sec timeout
        while (_now - _start) < 180:
            r = self.provider.get_nat_gateway(nat['NatGatewayId'])
            if r['State'] == 'deleted':
                break
            time.sleep(10)
        _elapsed = time.time() - _start
        self.log.info(f"-> NAT {nat['NatGatewayId']} deleted, {_elapsed:.2f}s")

        # Release EIP
        for _address in nat['NatGatewayAddresses']:
            # Dissassociation happens on NAT deletion
            # This is just for history
            # r = self.provider.disassociate_address(_address['AssociationId'])
            # self.log.info(f"-> dissassociated IP '{_address['PublicIp']}' from '{_address['NetworkInterfaceId']}'")

            # IP can be release only when NAT has status 'deleted'
            try:
                r = self.provider.release_address(_address['AllocationId'])
                self.log.info(f"-> released IP '{_address['PublicIp']}'")
            except Exception as e:
                self.log.error("ERROR: Failed to release "
                               f"'{_address['PublicIp']}': {e}")

        return r

    def clean_aws_nat(self):
        """
            Function matched networks in CloudV2 
            for clusters that has been deleted and cleans them up

            Steps:
            - List NATs
            - filter them according to current CloudV2 API (uuid used)
            - Extract network id and check if it is exists in CloudV2
            - If not query NAT for cleaning
            - Delete NAT and wait for 'deleted' status
            - Release PublicIP
        """
        def get_net_id_from_nat(nat):
            for tag in nat['Tags']:
                if tag['Key'] == 'Name' and tag['Value'].startswith(
                        'network-'):
                    return tag['Value'].split('-')[1]

        # list AWS networks with specific tag
        _uuids = {
            "https://cloud-api.ppd.cloud.redpanda.com":
            "0a9923e1-8b0f-4110-9fc0-6d3c714cc270",
            "https://cloud-api.ign.cloud.redpanda.com":
            "a845616f-0484-4506-9638-45fe28f34865"
        }
        # List NAT gateways
        _nats = self.provider.list_nat_gateways(tag_key="redpanda-org")
        self.log.info(f"# Found {len(_nats)} NAT Gateways")

        # Filter by CloudV2 API origin
        _uuid = _uuids[self.config.api_url]
        _nat_filtered = []
        for _nat in _nats:
            if _nat['State'] == 'deleted':
                _ips = ", ".join([
                    a['PublicIp'] + " / " + a['AllocationId']
                    for a in _nat['NatGatewayAddresses']
                ])
                self.log.info(f"# Nat {_nat['NatGatewayId']} deleted, "
                              f"IPs: {_ips}'")
                continue
            _date = datetime.strftime(_nat['CreateTime'], "%Y-%m-%d %H:%M:%S")
            for _tag in _nat['Tags']:
                if _tag['Key'] == 'redpanda-org' and _tag['Value'] == _uuid:
                    # This is NAT created by this CloudV2 API
                    _net_id = get_net_id_from_nat(_nat)
                    # Check if network exists in CloudV2
                    try:
                        r = self.cloudv2.get_network(_net_id, quite=True)
                        self.log.warning(f"# {_nat['NatGatewayId']} ({_date}) "
                                         f"skipped: network '{r['id']}' "
                                         "exists")
                    except Exception:
                        # If there is no network, it will generate HTTPError
                        # That means that it should be deleted
                        _nat_filtered.append(_nat)

        self.log.info(f"# {len(_nat_filtered)} orphaned NATs "
                      f"for {self.config.api_url}")

        # Fancy threading
        # Due to RPS limiter on AWS side, please, do not use >5
        pool = ThreadPoolExecutor(max_workers=5)
        self.log.info("\n\n# Cleaning up")
        self._pooled_delete(pool, "NAT Gateways", self._delete_nat,
                            _nat_filtered)

        return

    def clean_buckets(self, mask=None, eight_hours=False):
        """
            Function list buckets on S3 for cloud cluster storage
            and cleans them up if corresponding cluster is deleted/not exists.

            Steps:
            - List buckets with 'redpanda-cloud-storage-*'
              example: 'redpanda-cloud-storage-cl0lhnudg5jkvqfj7vtg'
            - Check if such cluster is deleted
            - Delete bucket
        """
        if self.config.provider == "GCP":
            self.log.warning("Bucket cleaning not yet supported for GCP")
            return False

        if not mask:
            self.log.info("# Bucket mask is empty. "
                          "Will not delete all buckets for account")
            return
        else:
            self.log.info(f"# Listing buckets using '{mask}'")
            buckets = self.provider.list_buckets(mask=mask)
            self.log.info(f"# Found buckets: {len(buckets)}")
            for bucket in buckets:
                self.log.info(f"-> Processing '{bucket['Name']}'")
                # Check creation time
                created = bucket['CreationDate']
                ts = self.back_8h if eight_hours else self.back_36h
                if created.timestamp() > ts.timestamp():
                    _d = created.strftime(ns_name_date_fmt)
                    self.log.info(f"...8h not passed; created at {_d}'; "
                                  "skipped")
                    continue
                # Do not need to check the cluster status in CloudV2 API
                # As we are deleting logs and temp data storages

                # Get first 3000 objects
                bucket_objects = self.provider.list_bucket_objects(
                    bucket['Name'], max_objects=3000)
                _count = len(bucket_objects)
                if _count >= 3000:
                    self.log.info("...more than 3000 objects found. "
                                  "Applying 1 day expiration policy")
                    # Apply LC policy
                    # Once we update the policy, creation date of
                    # the bucket will change as it will be replicated
                    # under the hood and it will be a new bucket
                    # instead of old one
                    self.provider.set_bucket_lifecycle_configuration(
                        bucket['Name'])
                else:
                    # Delete objects
                    if _count > 0:
                        self.log.info(f"...deleting {_count} objects")
                        self.provider.cleanup_bucket(bucket['Name'])
                    iters = 10
                    while iters > 0:
                        count = self.provider.list_bucket_objects(
                            bucket['Name'])
                        count = len(count)
                        if count == 0:
                            break
                        self.log.info("...bucket is not empty, waiting 10 sec")
                        time.sleep(10)
                        iters -= 1
                    if iters == 0:
                        self.log.info("...bucket deletion postponed")
                        continue
                    # Delete bucket
                    self.log.info(f"...deleting bucket '{bucket['Name']}'")
                    self.provider.delete_bucket(bucket['Name'])

        self.log.info(f"# Done cleaning up buckets with mask '{mask}'\n\n")
        return True


# Main script
def cleanup_entrypoint():
    # Init the cleaner class
    cleaner = CloudCleanup()

    # Arguments parsing. Quick and head on. No fancy argparse needed
    clean_namespaces = False
    clean_buckets = False
    clean_nats = False
    # Copy the list
    arguments = sys.argv[:]
    arguments.pop(0)
    if len(arguments) == 0:
        clean_namespaces = True
    else:
        while len(arguments) > 0:
            option = arguments.pop()
            if option == 'ns':
                clean_namespaces = True
            elif option == 'nat':
                clean_nats = True
            elif option == 's3':
                clean_buckets = True
            else:
                cleaner.log.error(
                    f"ERROR: Wrong argument: '{option}'. Accepting "
                    "only 's3'/'nat' to clean buckets or aws NATs")
                sys.exit(1)

    # Namespaces
    if clean_namespaces:
        cleaner.clean_namespaces(ns_name_prefix, 8)

    # NAT Gateways cleaning routine
    if clean_nats:
        cleaner.clean_aws_nat()

    # Clean buckets for deleted clusters and networks
    if clean_buckets:
        cleaner.clean_buckets(mask="panda-bucket-", eight_hours=True)
        cleaner.clean_buckets(mask="redpanda-cloud-storage-")
        cleaner.clean_buckets(mask="redpanda-network-logs-")

    cleaner.log.info("\n# Done.")
    return


if __name__ == "__main__":
    cleanup_entrypoint()
    sys.exit(0)
