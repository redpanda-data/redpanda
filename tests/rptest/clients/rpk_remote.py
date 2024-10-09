# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from rptest.clients.rpk import RpkException


class RpkRemoteTool:
    """
    Wrapper around rpk. Runs commands on the Redpanda cluster nodes. 
    """
    def __init__(self, redpanda, node):
        self._redpanda = redpanda
        self._node = node

    def config_set(self, key, value, format=None, path=None, timeout=30):
        cmd = [
            'set',
            f"'{key}'",
            f"'{value}'",
        ]

        if format is not None:
            cmd += ['--format', format]

        return self._run_config(cmd, path=path, timeout=timeout)

    def debug_bundle(self, output_file):
        # Run the bundle command.  It outputs into pwd, so switch to working dir first
        return self._execute([
            self._rpk_binary(), 'debug', 'bundle', "--output", output_file,
            "--api-urls",
            self._redpanda.admin_endpoints()
        ],
                             timeout=45)

    def cluster_config_force_reset(self, property_name):
        return self._execute([
            self._rpk_binary(), 'cluster', 'config', 'force-reset',
            property_name
        ])

    def cluster_config_lint(self):
        return self._execute([self._rpk_binary(), 'cluster', 'config', 'lint'])

    def tune(self, tuner):
        return self._execute([self._rpk_binary(), 'redpanda', 'tune', tuner])

    def mode_set(self, mode):
        return self._execute([self._rpk_binary(), 'redpanda', 'mode', mode])

    def redpanda_start(self, log_file, additional_args="", env_vars=""):
        return self._execute([
            env_vars,
            self._rpk_binary(), "redpanda", "start", "-v", additional_args,
            ">>", log_file, "2>&1", "&"
        ])

    def _run_config(self, cmd, path=None, timeout=30):
        cmd = [self._rpk_binary(), 'redpanda', 'config'] + cmd

        if path is not None:
            cmd += ['--config', path]

        return self._execute(cmd, timeout=timeout)

    def _run_profile(self, cmd):
        cmd = [self._rpk_binary(), "profile"] + cmd
        return self._execute(cmd)

    def create_profile(self, name):
        cmd = ["create", name]
        return self._run_profile(cmd)

    def create_profile_redpanda(self, name, cfg_location):
        return self._run_profile(
            ['create', name, "--from-redpanda", cfg_location])

    def use_profile(self, name):
        cmd = ["use", name]
        return self._run_profile(cmd)

    def delete_profile(self, name):
        cmd = ["delete", name]
        return self._run_profile(cmd)

    def rename_profile(self, new_name):
        cmd = ["rename-to", new_name]
        return self._run_profile(cmd)

    def set_profile(self, kv):
        cmd = ["set", kv]
        return self._run_profile(cmd)

    def list_profiles(self):
        cmd = ["list"]
        out = self._run_profile(cmd)
        lines = out.splitlines()
        if len(lines) == 1:
            return []

        def profile_line(line):
            parts = line.split()
            # We remove the asterisk that denotes that is the selected profile. Not needed here.
            return parts[0].strip("*")

        for i, line in enumerate(lines):
            if line.split() == ["NAME", "DESCRIPTION"]:
                return list(map(profile_line, lines[i + 1:]))

    def create_topic_no_flags(self, name):
        cmd = [self._rpk_binary(), "topic", "create", name]
        return self._execute(cmd)

    def cloud_login_cc(self, id, secret):

        cmd = [
            self._rpk_binary(), "cloud", "login", "--client-id", id,
            "--client-secret", secret, "--save", "--no-profile"
        ]

        self._redpanda.logger.debug(
            "Executing command: %s cloud login --client-id %s --client-secret [redacted]",
            self._rpk_binary(), id)

        return self._execute(cmd, log_cmd=False)

    def cloud_logout(self, clear_credentials=True):

        cmd = [self._rpk_binary(), "cloud", "logout"]

        if clear_credentials:
            cmd += ["--clear-credentials"]

        return self._execute(cmd)

    def _execute(self, cmd, timeout=30, log_cmd=True):
        if log_cmd:
            self._redpanda.logger.debug("Executing command: %s", cmd)

        return self._node.account.ssh_output(
            ' '.join(cmd),
            timeout_sec=timeout,
        ).decode('utf-8')

    def _rpk_binary(self):
        return self._redpanda.find_binary('rpk')

    def read_timestamps(self, topic: str, partition: int, count: int,
                        timeout_sec: int):
        """
        Read all the timestamps of messages in a partition, as milliseconds
        since epoch.

        :return: generator of 2-tuples like (offset:int, timestamp:int)
        """
        cmd = [
            self._rpk_binary(), "--brokers",
            self._redpanda.brokers(), "topic", "consume", topic, "-f",
            "\"%o %d\\n\"", "--num",
            str(count), "-p",
            str(partition)
        ]
        for line in self._node.account.ssh_capture(' '.join(cmd),
                                                   timeout_sec=timeout_sec):
            try:
                offset, ts = line.split()
            except:
                self._redpanda.logger.error(f"Bad line: '{line}'")
                raise

            yield (int(offset), int(ts))

    def plugin_list(self):
        cmd = [self._rpk_binary(), "plugin", "list", "--local"]

        return self._execute(cmd)

    def _run_connect(self, cmd, timeout=None):
        cmd = [self._rpk_binary(), "connect"] + cmd
        return self._execute(cmd, timeout=timeout)

    def install_connect(self, version="", force=False):
        cmd = ["install"]
        if version != "":
            cmd += ["--connect-version", version]

        if force:
            cmd += ["--force"]

        # This command has a higher timeout than normal rpk
        # commands as it downloads and install the RP Connect
        # binary.
        return self._run_connect(cmd, timeout=60)

    def uninstall_connect(self):
        cmd = ["uninstall"]

        return self._run_connect(cmd)

    def upgrade_connect(self):
        cmd = ["upgrade", "--no-confirm"]
        return self._run_connect(cmd)

    def connect_version(self):
        cmd = ["--version"]
        out = self._run_connect(cmd)
        """
        Example output of rpk connect --version:
        Version: 4.33.0
        Date: 2024-08-13T21:40:38Z
        """
        pattern = r"Version:\s*([\d.]+)"
        match = re.search(pattern, out)
        if match:
            return match.group(1)  # The version.
        else:
            raise RpkException(f"unable to parse connect version from {out}")

    def run_connect_arbitrary(self, cmd):
        return self._run_connect(cmd)
