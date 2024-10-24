# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.redpanda import RedpandaService


class ConsumerOffsetsRecovery:
    """
    Wrap tools/consumer_offsets_recovery for use in tests
    """
    def __init__(self, redpanda: RedpandaService):
        self._redpanda = redpanda
        self._cfg_path = '/tmp/cgr.properties'
        self._offsets_cache_path = '/tmp/offsets'

    def _cmd(self, partition_count, dry_run):
        cmd = "python3 /opt/scripts/consumer_offsets_recovery/main.py"

        cmd += f" --cfg {self._cfg_path}"
        cmd += f" --offsets {self._offsets_cache_path}"
        cmd += f" -p {partition_count}"
        if not dry_run:
            cmd += " --execute"
        return cmd

    def _render_config(self, node):
        properties = {"bootstrap_servers": self._redpanda.brokers()}
        content = ""
        for k, v in properties.items():
            content += f"{k}={v}\n"

        node.account.create_file(self._cfg_path, content)

    def change_partition_count(self, partition_count, node, dry_run):
        node.account.remove(self._offsets_cache_path, allow_fail=True)
        node.account.mkdir(self._offsets_cache_path)
        self._render_config(node)
        cmd = self._cmd(partition_count, dry_run)
        output = node.account.ssh_output(cmd, combine_stderr=True)
        self._redpanda.logger.info(f"out: {output}")
