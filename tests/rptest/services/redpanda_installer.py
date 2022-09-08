# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import errno
import json
import os
import re
import requests

from ducktape.utils.util import wait_until

# Match any version that may result from a redpanda binary, which may not be a
# released version.
# E.g. "v22.1.1-rc1-1373-g77f868..."
VERSION_RE = re.compile(".*v(\\d+)\\.(\\d+)\\.(\\d+).*")


def wait_for_num_versions(redpanda, num_versions):
    # Use a single node so the metadata about brokers have a consistent source
    # in case we retry.
    node = redpanda.nodes[0]

    def get_unique_versions():
        try:
            brokers_list = \
                json.dumps(redpanda._admin.get_brokers(node=node))
        except Exception as e:
            redpanda.logger.debug(f"Failed to list brokers: {e}")
            raise e
        redpanda.logger.debug(brokers_list)
        version_re = re.compile("v\\d+\\.\\d+\\.\\d+")
        return set(version_re.findall(brokers_list))

    # NOTE: allow retries, as the version may not be available immediately
    # following a restart.
    wait_until(lambda: len(get_unique_versions()) == num_versions,
               timeout_sec=30,
               retry_on_exc=True)
    unique_versions = get_unique_versions()
    assert len(unique_versions) == num_versions, unique_versions
    return unique_versions


def int_tuple(str_tuple):
    """
    Converts
    ("x": string, "y": string, "z": string) => (x: int, y: int, z: int)
    """
    return (int(str_tuple[0]), int(str_tuple[1]), int(str_tuple[2]))


class InstallOptions:
    """
    Options with which to configure the installation of Redpanda in a cluster.
    """
    def __init__(self,
                 install_previous_version=False,
                 install_prev_prev_version=False,
                 version=None,
                 num_to_upgrade=0):
        # If true, install the highest version of the prior feature version
        # before HEAD.
        self.install_previous_version = install_previous_version

        # HACK: this is a part of a backport that requires new test infra to
        # start Redpanda from two versions ago in EndToEndTest. Other test
        # harnesses can just use the RedpandaInstaller directly to deduce and
        # install the proper version.

        # If true, install the highest version of the feature version two
        # versions before HEAD.
        self.install_prev_prev_version = install_prev_prev_version

        # Either RedpandaInstaller.HEAD or a numeric tuple representing the
        # version to install (e.g. (22, 1, 3)).
        self.version = version

        # Number of nodes in a cluster to upgrade to HEAD after starting the
        # cluster on an older version, e.g. to simulate a mixed-version
        # environment.
        self.num_to_upgrade = num_to_upgrade


class RedpandaInstaller:
    """
    Provides mechanisms to install multiple Redpanda binaries on a cluster.

    Each installed version is downloaded and kept around for the lifespan of
    the installer. Thus, once downloaded, switching versions amounts to
    updating a symlink usable by the RedpandaService.

    This only provides methods for installation; restarting nodes is left up to
    callers.
    """
    # Represents the binaries installed at the time of the call to start(). It
    # is expected that this is identical across all nodes initially.
    HEAD = "head"

    # Directory to which binaries are downloaded.
    #
    # In local deployments it is expected that this is shared by all nodes in a
    # cluster, and that directories therein are only ever created (never
    # deleted) during the lifetime of the RedpandaInstaller.
    INSTALLER_ROOT = "/opt/redpanda_installs"
    TGZ_URL_TEMPLATE = "https://dl.redpanda.com/qSZR7V26sJx7tCXe/redpanda/raw/names/redpanda-{arch}/versions/{version}/redpanda-{version}-{arch}.tar.gz"

    # File path to be used for locking to prevent multiple local test processes
    # from operating on the same volume mounts.
    INSTALLER_LOCK_PATH = f"{INSTALLER_ROOT}/install_lock"

    @staticmethod
    def root_for_version(version):
        """
        Returns an appropriate root path for the given version. Expects the
        version to be either a tuple of ints or the string "head".
        """
        if version != RedpandaInstaller.HEAD:
            version = f"v{version[0]}.{version[1]}.{version[2]}"
        return f"{RedpandaInstaller.INSTALLER_ROOT}/{version}"

    @staticmethod
    def wait_for_async_ssh(logger, ssh_out_per_node, log_msg):
        """
        Waits for each SSHOutputIter to complete.
        """
        for node in ssh_out_per_node:
            logger.debug(f"{log_msg} for {node.account.hostname}")
            for l in ssh_out_per_node[node]:
                logger.debug(l)

    def __init__(self, redpanda):
        """
        Constructs an installer for the given RedpandaService.
        """
        self._started = False
        self._released_versions: list[tuple] = []
        self._redpanda = redpanda

        # Keep track if the original install path is /opt/redpanda, as is the
        # case for package-deployed clusters. Since the installer uses this
        # directory, we'll need to be mindful not to mess with the original
        # binaries.
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        self._head_backed_up = rp_install_path_root == "/opt/redpanda"

        # Whether the nodes are expected to share a single mounted volume for
        # their installs. If so, care should be taken to coordinate operations
        # on the installer root.
        self._nodes_share_installs = rp_install_path_root != "/opt/redpanda"

        # File descriptor used to coordinate access to the installer root when
        # multiple test processes are running on the same machine.
        # Must be acquire when operating on the contents of the installer root
        # (i.e. root_for_version(), etc).
        self._install_lock_fd = None

        self._installed_version = self.HEAD

    @property
    def installed_version(self):
        return self._installed_version

    def _acquire_install_lock(self, timeout_sec=600):
        """
        Attempt to take the install lock, preventing other test processes from
        operating an installer.

        Serves to prevent concurrent operations to the same local mountpoint.
        """
        if not self._nodes_share_installs:
            self._redpanda.logger.debug(
                "Nodes don't share installs; no locking needed")
            return

        def _lock():
            try:
                self._redpanda.logger.debug(
                    f"Acquiring install lock {self.INSTALLER_LOCK_PATH}")
                fd = os.open(self.INSTALLER_LOCK_PATH,
                             os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self._install_lock_fd = fd
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                # Another process holds the lock.
                return False
            return True

        wait_until(lambda: _lock(), timeout_sec=timeout_sec)
        self._redpanda.logger.debug(
            f"Acquired install lock {self.INSTALLER_LOCK_PATH}")

    def _release_install_lock(self):
        """
        Releases the install lock, allowing other test processes running
        locally to perform downloads.
        """
        if not self._nodes_share_installs:
            self._redpanda.logger.debug(
                "Nodes don't share installs; no locking needed")
            return

        if not self._install_lock_fd:
            self._redpanda.logger.debug("Installer lock not held")
            return True
        os.close(self._install_lock_fd)
        os.unlink(self.INSTALLER_LOCK_PATH)
        self._redpanda.logger.debug("Released install lock")

    def _setup_head_roots_unlocked(self):
        """
        Sets up the head roots on each node such that they contain or point to
        the original binaries installed at 'rp_install_path_root'.

        Expects that the install lock has been acquired before calling.
        """
        nodes = self._redpanda.nodes
        head_root_path = RedpandaInstaller.root_for_version(
            RedpandaInstaller.HEAD)
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        for node in nodes:
            # Always end up with binaries at 'head_root_path', so we can
            # continue to use root_for_version() to reference the head root.
            cmd = None
            if self._head_backed_up:
                cmd = f"mv /opt/redpanda {head_root_path}"
            elif not node.account.exists(head_root_path):
                cmd = f"ln -s {rp_install_path_root} {head_root_path}"
            if cmd:
                node.account.ssh_output(cmd)

    def start(self):
        """
        Validates that all nodes in the service have installed the same
        version, and initializes test-wide state, like the list of released
        versions.
        """
        if self._started:
            return

        # In case a previous test was aborted, do some cleanup.
        self.reset_current_install(self._redpanda.nodes)

        initial_version = None
        nodes = self._redpanda.nodes

        # Verify that the installations on each node match.
        for node in nodes:
            vers = self._redpanda.get_version(node)
            if initial_version == None:
                initial_version = vers
            assert initial_version == vers, \
                f"Mismatch version {node.account.hostname} has {vers}, {nodes[0].account.hostname} has {initial_version}"
            node.account.ssh_output(f"mkdir -p {self.INSTALLER_ROOT}")

        try:
            self._acquire_install_lock()
            self._setup_head_roots_unlocked()
        finally:
            self._release_install_lock()

        # Start out pointing /opt/redpanda at the current installation.
        ssh_setup_head_per_node = dict()
        head_root_path = self.root_for_version(RedpandaInstaller.HEAD)
        for node in nodes:
            if not node.account.exists("/opt/redpanda"):
                cmd = f"ln -s {head_root_path} /opt/redpanda"
                ssh_setup_head_per_node[node] = node.account.ssh_capture(cmd)
        self.wait_for_async_ssh(self._redpanda.logger, ssh_setup_head_per_node,
                                "Setting up /opt/redpanda")

        # Keep track of the logical version of the head installation so we can
        # use it to get older versions relative to the head version.
        # NOTE: installing this version may not yield the same binaries being
        # as 'head', e.g. if an unreleased source is checked out.
        self._head_version: tuple = int_tuple(
            VERSION_RE.findall(initial_version)[0])

        self._started = True

    def _initialize_released_versions(self):
        if len(self._released_versions) > 0:
            return

        # Initialize and order the releases so we can iterate to previous
        # releases when requested.
        releases_resp = requests.get(
            "https://api.github.com/repos/redpanda-data/redpanda/releases")
        self._released_versions = [
            int_tuple(VERSION_RE.findall(f["tag_name"])[0])
            for f in releases_resp.json()
        ]
        self._released_versions.sort(reverse=True)

    def highest_from_prior_feature_version(self, version):
        """
        Returns the highest version that is of a lower feature version than the
        given version, or None if one does not exist.
        """
        if not self._started:
            self.start()
        if len(self._released_versions) == 0:
            self._initialize_released_versions()

        if version == RedpandaInstaller.HEAD:
            version = self._head_version
        # NOTE: the released versions are sorted highest first.
        for v in self._released_versions:
            if (v[0] == version[0]
                    and v[1] < version[1]) or (v[0] < version[0]):
                return v
        return None

    def install(self, nodes, version):
        """
        Installs the release on the given nodes such that the next time the
        nodes are restarted, they will use the newly installed bits.

        TODO: abstract 'version' into a more generic installation that doesn't
        necessarily correspond to a released version. E.g. a custom build
        packaged in a private repository.
        """
        if not self._started:
            self.start()

        try:
            self._acquire_install_lock()
            self._install_unlocked(nodes, version)
            self._installed_version = version
        finally:
            self._release_install_lock()

    def _install_unlocked(self, nodes, version):
        """
        Like above but expects the install lock to have been taken before
        calling.
        """
        version_root = self.root_for_version(version)

        nodes_to_download = nodes
        if self._nodes_share_installs:
            nodes_to_download = [nodes[0]]

        ssh_download_per_node = dict()
        for node in nodes_to_download:
            if not version == RedpandaInstaller.HEAD and not node.account.exists(
                    version_root):
                ssh_download_per_node[
                    node] = self._async_download_on_node_unlocked(
                        node, version)
        self.wait_for_async_ssh(self._redpanda.logger, ssh_download_per_node,
                                "Finished downloading binaries")

        # Regardless of whether we downloaded anything, adjust the
        # /opt/redpanda link to point to the appropriate version on all nodes.
        relink_cmd = f"unlink /opt/redpanda && ln -s {version_root} /opt/redpanda"
        for node in nodes:
            node.account.ssh_output(relink_cmd)

    def _async_download_on_node_unlocked(self, node, version):
        """
        Asynchonously downloads Redpanda of the given version on the given
        node. Returns an iterator to the results.

        Expects the install lock to have been taken before calling.
        """
        version_root = self.root_for_version(version)
        arch = "amd64"
        uname = str(node.account.ssh_output("uname -m"))
        if "aarch" in uname or "arm" in uname:
            arch = "arm64"
        self._redpanda.logger.debug(
            f"{node.account.hostname} uname output: {uname}")

        url = RedpandaInstaller.TGZ_URL_TEMPLATE.format( \
            arch=arch, version=f"{version[0]}.{version[1]}.{version[2]}")
        tgz = "redpanda.tar.gz"
        cmd = f"curl -fsSL {url} --create-dir --output-dir {version_root} -o {tgz} && gunzip -c {version_root}/{tgz} | tar -xf - -C {version_root} && rm {version_root}/{tgz}"
        return node.account.ssh_capture(cmd)

    def reset_current_install(self, nodes):
        """
        WARNING: should not be used to upgrade to the originally installed
        binaries; use 'install(RedpandaInstaller.HEAD)' for that. This should
        only be used to clean up a node to its expected starting state (the
        state of the world before the first call to 'start()').

        Resets any /opt/redpanda symlink to instead be real binaries if they
        exist. This is a best attempt effort to revert the installs to their
        original state (i.e. the state before installing other versions).

        Upon returning, either:
        - this is a packaged deployment (CDT) and we are left with a real
          /opt/redpanda directory (not a symlink) if possible, or
        - this is a local deployment and we are left with no links to head
          binaries
        """
        head_root_path = self.root_for_version(RedpandaInstaller.HEAD)
        for node in nodes:
            host = node.account.hostname
            if self._head_backed_up:
                assert not self._nodes_share_installs
                # NOTE: no locking required since installs aren't shared.
                head_root_path_exists = node.account.exists(head_root_path)
                opt_redpanda_exists = node.account.exists("/opt/redpanda")
                if opt_redpanda_exists:
                    if not node.account.islink("/opt/redpanda"):
                        assert not head_root_path_exists, \
                            f"{host}: {head_root_path} exists and /opt/redpanda exists but is not a link; unclear which to use"
                        continue
                    node.account.ssh_output("unlink /opt/redpanda",
                                            allow_fail=True)

                assert head_root_path_exists, f"{host}: neither {head_root_path} nor /opt/redpanda exists"
                node.account.ssh_output(f"mv {head_root_path} /opt/redpanda",
                                        allow_fail=True)
            else:
                node.account.ssh_output("unlink /opt/redpanda",
                                        allow_fail=True)
