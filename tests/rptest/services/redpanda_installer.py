# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import requests

# Match any version that may result from a redpanda binary, which may not be a
# released version.
# E.g. "v22.1.1-rc1-1373-g77f868..."
VERSION_RE = re.compile(".*v(\\d+)\\.(\\d+)\\.(\\d+).*")


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
    INSTALLER_ROOT = "/opt/redpanda_installs"
    TGZ_URL_TEMPLATE = "https://packages.vectorized.io/qSZR7V26sJx7tCXe/redpanda/raw/names/redpanda-{arch}/versions/{version}/redpanda-{version}-{arch}.tar.gz"

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
        self._redpanda = redpanda
        self._installed_per_node = dict()

        # Keep track if the original install path is /opt/redpanda is used, as
        # is the case for package-deployed clusters. Since the installer uses
        # this directory, we'll need to be mindful not to mess with the
        # original binaries.
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        self._head_backed_up = rp_install_path_root == "/opt/redpanda"

    def start(self):
        """
        Validates that all nodes in the service have installed the same
        version, and initializes test-wide state, like the list of released
        versions.
        """
        if self._started:
            return

        initial_version = None
        nodes = self._redpanda.nodes

        # Verify that the installations on each node match.
        for node in nodes:
            vers = self._redpanda.get_version(node)
            if initial_version == None:
                initial_version = vers
            assert initial_version == vers, \
                f"Mismatch version {node.account.hostname} has {vers}, {nodes[0].account.hostname} has {initial_version}"

        # Clean up the installer root directory so we start out clean.
        for node in nodes:
            if node.account.exists(RedpandaInstaller.INSTALLER_ROOT):
                node.account.remove(f"{RedpandaInstaller.INSTALLER_ROOT}/*",
                                    allow_fail=True)
            else:
                node.account.mkdir(RedpandaInstaller.INSTALLER_ROOT)

        # Now that we're at a sane starting point, set up our install path for
        # ease of jumping between versions.
        ssh_setup_head_per_node = dict()
        head_root_path = RedpandaInstaller.root_for_version(
            RedpandaInstaller.HEAD)
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        for node in nodes:
            # For simplicity's sake, always end up with binaries at
            # 'head_root_path', so we can continue to use root_for_version() to
            # reference the head root.
            head_cmd = ""
            if self._head_backed_up:
                head_cmd = f"mv /opt/redpanda {head_root_path}"
            else:
                head_cmd = f"ln -s {rp_install_path_root} {head_root_path}"

            cmd = f"{head_cmd} && ln -s {head_root_path} /opt/redpanda"
            ssh_setup_head_per_node[node] = node.account.ssh_capture(cmd)
            self._installed_per_node[node] = set()
        self.wait_for_async_ssh(self._redpanda.logger, ssh_setup_head_per_node,
                                "Setting up head binaries")

        def int_tuple(str_tuple):
            return (int(str_tuple[0]), int(str_tuple[1]), int(str_tuple[2]))

        # Initialize and order the releases so we can iterate to previous
        # releases when requested.
        releases_resp = requests.get(
            "https://api.github.com/repos/redpanda-data/redpanda/releases")
        self._released_versions: list[tuple] = [
            int_tuple(VERSION_RE.findall(f["tag_name"])[0])
            for f in releases_resp.json()
        ]
        self._released_versions.sort(reverse=True)
        self._started = True

    def install(self, nodes, version):
        """
        Installs the release on the given node such that the next time the node
        is restarted, it will use the newly installed bits.

        TODO: abstract 'version' into a more generic installation that doesn't
        necessarily correspond to a released version. E.g. a custom build
        packaged in a private repository.
        """
        if not self._started:
            self.start()
        assert version == RedpandaInstaller.HEAD or version in self._released_versions, \
            f"Can't find installation for {version}"
        ssh_install_per_node = dict()
        for node in nodes:
            # If we already have this version installed, just adjust the
            # symlinks.
            version_root = self.root_for_version(version)
            relink_cmd = f"unlink /opt/redpanda && ln -s {version_root} /opt/redpanda"
            if version == RedpandaInstaller.HEAD or version in self._installed_per_node[
                    node]:
                ssh_install_per_node[node] = node.account.ssh_capture(
                    relink_cmd)
                continue

            arch = "amd64"
            uname = str(node.account.ssh_output("uname -m"))
            if "aarch" in uname or "arm" in uname:
                arch = "arm64"
            self._redpanda.logger.debug(
                f"{node.account.hostname} uname output: {uname}")

            self._installed_per_node[node].add(version)
            url = RedpandaInstaller.TGZ_URL_TEMPLATE.format( \
                arch=arch, version=f"{version[0]}.{version[1]}.{version[2]}")
            tgz = "redpanda.tar.gz"
            cmd = f"curl -fsSL {url} --create-dir --output-dir {version_root} -o {tgz} && gunzip -c {version_root}/{tgz} | tar -xf - -C {version_root} && rm {version_root}/{tgz} && {relink_cmd}"
            ssh_install_per_node[node] = node.account.ssh_capture(cmd)

        self.wait_for_async_ssh(self._redpanda.logger, ssh_install_per_node,
                                "Finished installing binaries")

    def clean(self, node):
        """
        Cleans the node such that only the original installation remains.

        This should only be called once there is no longer a need to run the
        RedpandaService.
        """
        if not self._started:
            self._redpanda.logger.debug(
                "Ignoring cleanup, installer not started")
            return

        # Allow failures so the entire cleanup can proceed even on failure.
        head_root_path = RedpandaInstaller.root_for_version(
            RedpandaInstaller.HEAD)
        if self._head_backed_up:
            cmd = f"unlink /opt/redpanda && mv {head_root_path} /opt/redpanda"
            node.account.ssh(cmd, allow_fail=True)
        else:
            cmd = f"unlink /opt/redpanda && unlink {head_root_path}"
            node.account.ssh(cmd, allow_fail=True)

        # Also clean up all the downloaded published binaries.
        roots_to_rm = [
            RedpandaInstaller.root_for_version(v)
            for v in self._installed_per_node[node]
        ]
        if len(roots_to_rm) == 0:
            return
        node.account.remove(' '.join(roots_to_rm), allow_fail=True)
