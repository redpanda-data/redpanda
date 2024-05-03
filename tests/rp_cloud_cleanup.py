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
    cmd = cmd.split(" ")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    p.wait()
    # Format outout as String object
    out = "\n".join(list(p.stdout)).strip()  # type: ignore
    p.kill()
    return out


class FakeContext():
    def __init__(self, globals):
        # Check if rpk present
        if not os.path.exists(globals['rp_install_path_root']):
            # Get correct path
            rpk_path = shell("which rpk")
            globals['rp_install_path_root'] = rpk_path.replace('/bin/rpk', "")
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
        fake_context = FakeContext(ducktape_globals)

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
            keyId = ducktape_globals['s3_access_key']
            secret = ducktape_globals['s3_secret_key']
        elif self.config.provider == "GCP":
            keyId = self.config.gcp_keyfile
            secret = None
        else:
            self.log.error(f"# ERROR: Provider {self.config.provider} "
                           "not supported")
            sys.exit(1)
        # Initialize clients
        self.provider = make_provider_client(self.config.provider, self.log,
                                             self.config.region, keyId, secret)
        #self.log.info(f"# Provider {self.config.provider} initialized. Running"
        #              f" as '{self.provider.get_caller_identity()['Arn']}'")
        self.utils = CloudClusterUtils(fake_context, self.log, keyId, secret,
                                       self.config.provider,
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

        def _log_skip(msg):
            msg += f"| skipped '{cluster['name']}', 36h delay not passed"
            self.log.info(msg)

        def _log_deleted(msg):
            msg += "| deleted"
            self.log.info(msg)

        # Function detects cluster type and deletes it
        cluster = self.cloudv2.get_resource(handle)
        if cluster is None:
            self.log.warning(f"# Cluster '{handle}' was already deleted")
            return
        id = cluster["id"]
        cluster_type = cluster['spec']['clusterType']
        state = cluster['state']
        createdDate = datetime.strptime(cluster['createdAt'],
                                        "%Y-%m-%dT%H:%M:%S.%fZ")
        message = f"-> cluster '{cluster['name']}', " \
                  f"{cluster['createdAt']}, {cluster_type} "
        if cluster_type in ['FMC']:
            message += f"| status: '{cluster['state']}' "
            if state in ['ready']:
                if not _ensure_date(createdDate):
                    _log_skip(message)
                    return False
                else:
                    # Just request deletion right away
                    out = self.cloudv2.delete_resource(handle)
                    _log_deleted(message)
                    return out
            else:
                message += "| skipping non-ready FMC clouds"
                self.log.info(message)
        elif cluster_type in ['BYOC']:
            # Check if provider is the same
            # This is relevant only for BYOC as
            # rpk will not be able to delete it anyway
            if cluster['spec']['provider'].lower(
            ) != self.config.provider.lower():
                # Wrong provider, can't delete right now
                message += "| SKIP: Can't delete " \
                            f"'{cluster['spec']['provider']}' " \
                            f"cluster using '{self.config.provider}' creds"
                self.log.info(message)
                return False

            # Check status and act in steps
            # In most cases cluisters will be in 'deleting_agent' status
            # I.e. past 36h that we skip for namespaces,
            # native CloudV2 cleaning will kick in and delete main cluster.
            # Only agent would be left

            # If the cluster in deleleting_agent, we should clean it
            # regardless of time or anything else to clean up quota
            message += f"| status: '{cluster['state']}' "
            if cluster['state'] in ['deleting_agent']:
                # Login
                try:
                    message += "| cloud login "
                    out = self.utils.rpk_cloud_login(
                        self.config.oauth_client_id,
                        self.config.oauth_client_secret)
                except Exception as e:
                    message += "| ERROR: failed to login as " \
                                f"'{self.config.oauth_client_id}': {e}"
                    self.log.error(message)
                    return False

                # Delete agent
                try:
                    message += "| delete agent "
                    out = self.utils.rpk_cloud_agent_delete(id)
                except Exception as e:
                    message += "| ERROR: failed to delete agent for " \
                                f"cluster '{id}': {e}"
                    self.log.info(message)
                    return False
                # All good
                _log_deleted(message)
                return True
            if state in ['creating_agent']:
                # Check date, and delete only old ones
                if not _ensure_date(createdDate):
                    _log_skip(message)
                    return False
                # Just delete the cluster resource
                out = self.cloudv2.delete_resource(handle)
                _log_deleted(message)
                self.log.debug(f"Returned:\n{out}")
                return True
            # All other states need to wait for 36h
            if not _ensure_date(createdDate):
                _log_skip(message)
                return False
            # TODO: Implement deletion on rare non-agent statuses
            else:
                message += "| WARNING: Cluster deletion with status " \
                            f"'{state}' not yet implemented"
                self.log.warning(message)
                return False
        else:
            message += f"| WARNING: Cloud type not supported: '{cluster_type}'"
            self.log.warning(message)
            return False

    def _pooled_delete(self, pool, resource_name, func, queue):
        queue_len = len(queue)
        if queue_len < 1:
            self.log.info(f"# {resource_name}: nothing to delete")
            return
        else:
            self.log.info(f"# {resource_name}: amount to cleanup {queue_len}")
            ok = 0
            failed = 0
            unsure = 0
            for r in pool.map(func, queue):
                if isinstance(r, bool):
                    if not r:
                        failed += 1
                    else:
                        ok += 1
                else:
                    unsure += 1
                    self.log.debug("  output:\n{r}")
            self.log.info(f"# Clusters: {ok} deleted, "
                          f"{failed} failed, {unsure} other")
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
        start = time.time()
        r = self.provider.delete_nat(nat['NatGatewayId'])
        now = time.time()
        # 180 sec timeout
        while (now - start) < 180:
            r = self.provider.get_nat_gateway(nat['NatGatewayId'])
            if r['State'] == 'deleted':
                break
            time.sleep(10)
        _elapsed = time.time() - start
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

    def _delete_vpc(self, vpc):
        # Delete VPC
        # self.log.info(f"-> deleting VPC: {vpc['VpcId']}")
        start = time.time()

        # - terminate all instances running in the VPC
        # - delete all security groups associated with the VPC (except the default one)
        # - delete all route tables associated with the VPC (except the default one)

        r = self.provider.delete_vpc(vpc['VpcId'], clean_dependencies=True)
        if not r:
            self.log.info(f"-> VPC {vpc['VpcId']} NOT deleted")
            return r
        now = time.time()
        # 180 sec timeout
        while (now - start) < 180:
            try:
                r = self.provider.get_vpc_by_id(vpc['VpcId'])
                if r['State'] == 'deleted':
                    break
            except:
                break
            time.sleep(10)
        elapsed = time.time() - start
        self.log.info(f"-> VPC {vpc['VpcId']} deleted, {elapsed:.2f}s")

        return r

    def clean_aws_networks(self):
        """
            Function matched networks in CloudV2 
            for clusters that has been deleted and cleans them up

            Steps:
            - List NATs and VPCs
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

        def get_date_from_resource(aws_res):
            if 'CreateTime' in aws_res:
                return datetime.strftime(aws_res['CreateTime'],
                                         "%Y-%m-%d %H:%M:%S")
            else:
                return "Unknown"

        def check_cloud_network(aws_res):
            # This resource is created by CloudV2 API
            net_id = get_net_id_from_nat(aws_res)
            # Check if network exists in CloudV2
            try:
                # 'quite' is passed all the way to depth of requests module
                r = self.cloudv2.get_network(net_id,
                                             quite=True)  # type: ignore
                return r
            except Exception:
                # If there is no network, it will generate HTTPError
                # That means that it should be deleted
                return None

        def filter_nats(nats):
            fnats = []
            for nat in nats:
                if nat['State'] == 'deleted':
                    ips = ", ".join([
                        a['PublicIp'] + " / " + a['AllocationId']
                        for a in nat['NatGatewayAddresses']
                    ])
                    self.log.info(f"# Nat {nat['NatGatewayId']} deleted, "
                                  f"IPs: {ips}'")
                    continue
                resource_date = get_date_from_resource(nat)
                for _tag in nat['Tags']:
                    if _tag['Key'] == 'redpanda-org' and _tag['Value'] == uuid:
                        # Check if network exists in CloudV2
                        cloud_net = check_cloud_network(nat)
                        if cloud_net is None:
                            fnats.append(nat)
                        else:
                            self.log.warning(
                                f"# {nat['NatGatewayId']} (create date: {resource_date}) "
                                f"skipped: network '{cloud_net['id']}' exists")
            return fnats

        def filter_vpcs(vpcs):
            fvpcs = []
            for vpc in vpcs:
                if vpc['State'] == 'deleted':
                    self.log.info(f"# VPC {vpc['VpcId']} deleted")
                    continue
                resource_date = get_date_from_resource(vpc)
                for tag in vpc['Tags']:
                    if tag['Key'] == 'redpanda-org' and tag['Value'] == uuid:
                        # Check if network exists in CloudV2
                        cloud_net = check_cloud_network(vpc)
                        if cloud_net is None:
                            fvpcs.append(vpc)
                        else:
                            self.log.warning(
                                f"# {vpc['VpcId']} ({resource_date}) "
                                f"skipped: network '{cloud_net['id']}' exists")
            return fvpcs

        if self.config.provider == "GCP":
            self.log.warning(
                "Network resources cleaning not yet supported for GCP")
            return False

        # list AWS networks with specific tag
        uuids = {
            "https://cloud-api.ppd.cloud.redpanda.com":
            "0a9923e1-8b0f-4110-9fc0-6d3c714cc270",
            "https://cloud-api.ign.cloud.redpanda.com":
            "a845616f-0484-4506-9638-45fe28f34865"
        }
        # List NAT gateways
        nats = self.provider.list_nat_gateways(tag_key="redpanda-org")
        self.log.info(f"# Found {len(nats)} NAT Gateways")

        # List VPCs
        vpcs = self.provider.list_vpcs(tag_key="redpanda-org")
        self.log.info(f"# Found {len(vpcs)} VPCs")

        # Filter by CloudV2 API origin
        uuid = uuids[self.config.api_url]
        nat_filtered = filter_nats(nats)
        vpc_filtered = filter_vpcs(vpcs)

        self.log.info(f"# {len(nat_filtered)} orphaned NATs "
                      f"for {self.config.api_url}")
        self.log.info(f"# {len(vpc_filtered)} orphaned VPCs "
                      f"for {self.config.api_url}")

        # Fancy threading
        # Due to RPS limiter on AWS side, please, do not use >5
        pool = ThreadPoolExecutor(max_workers=5)
        self.log.info("\n\n# Cleaning up")
        self._pooled_delete(pool, "NAT Gateways", self._delete_nat,
                            nat_filtered)
        # VPC deletion will only work if public IPs are not mapped
        self._pooled_delete(pool, "VPCs", self._delete_vpc, vpc_filtered)

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
        def _delete_bucket(name):
            # Delete bucket
            self.provider.delete_bucket(name)

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
                msg = f"-> '{bucket['Name']}'"
                # Check creation time
                last_modified = self.provider.query_bucket_lastmodified_date(
                    bucket['Name'])
                # last modified date comes from first key/object in the bucket
                # If it is empty - then make sure it is 1h old. I.e. not
                # part of some running test.
                # Assumption here is that RP will write something to
                # the bucket right when it knows it exists
                if last_modified is None:
                    # Bucket is empty
                    # If creation date is >1h, just delete it
                    one_hour = datetime.now() - timedelta(hours=1)
                    creation_date = bucket['CreationDate']
                    if creation_date.timestamp() > one_hour.timestamp():
                        _delete_bucket(bucket['Name'])
                    else:
                        msg += " | empty | 1h not passed"
                        self.log.info(msg)
                        continue
                ts = self.back_8h if eight_hours else self.back_36h
                if last_modified.timestamp() > ts.timestamp():  # type: ignore
                    created_date = last_modified.strftime(
                        ns_name_date_fmt)  # type: ignore
                    msg += f" | created at {created_date} | not empty | 8h not passed"
                    self.log.info(msg)
                    continue
                # Do not need to check the cluster status in CloudV2 API
                # As we are deleting logs and temp data storages

                # Get first 3000 objects
                bucket_objects = self.provider.list_bucket_objects(
                    bucket['Name'], max_objects=3000)
                bucket_count = len(bucket_objects)
                if bucket_count >= 3000:
                    msg += " | >3000 objects | 1 day expiration policy applied"
                    # Apply LC policy
                    # Once we update the policy, creation date of
                    # the bucket will change as it will be replicated
                    # under the hood and it will be a new bucket
                    # instead of old one
                    self.provider.set_bucket_lifecycle_configuration(
                        bucket['Name'])
                    self.log.info(msg)
                else:
                    # Delete objects
                    if bucket_count > 0:
                        msg += f" | {bucket_count} objects"
                        self.provider.cleanup_bucket(bucket['Name'])
                    iters = 10
                    while iters > 0:
                        count = self.provider.list_bucket_objects(
                            bucket['Name'])
                        count = len(count)
                        if count == 0:
                            break
                        time.sleep(10)
                        iters -= 1
                    if iters == 0:
                        msg += " | deletion postponed"
                        self.log.info(msg)
                        continue
                    _delete_bucket(bucket['Name'])
                    msg += " | deleted"
                    self.log.info(msg)

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
                    "only 'ns', 's3' or 'nat' to clean cloud clusters, "
                    "buckets or aws network resources")
                sys.exit(1)

    # Cloud cluster resource cleaning order
    # - Cloud cluster via namespaces/API using rpk
    # - [not covered here] autoscaling groups (will delete instances)
    # - [not covered here] EKS clusters (no access from here)
    # - [not covered here] ELBs (no access from here)
    # - NAT Gateways cleaning
    # - VPC Cleaning (including dependencies)
    # - S3 buckets

    # Namespaces
    if clean_namespaces:
        cleaner.clean_namespaces(ns_name_prefix, 8)

    # NAT Gateways cleaning routine
    if clean_nats:
        cleaner.clean_aws_networks()

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
