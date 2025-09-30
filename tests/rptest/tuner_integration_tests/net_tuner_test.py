# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.tests.test import TestContext
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk_remote import RpkRemoteTool


class NetTunerTest(RedpandaTest):
    def __init__(self, ctx: TestContext):
        super().__init__(test_context=ctx)

    @staticmethod
    def parse_matching_interrupt_num_from_interrupt_file(
        interrupt_file: str, matcher: str
    ) -> list[int]:
        ret: list[int] = []
        for line in interrupt_file.split("\n"):
            if matcher in line:
                ret.append(int(line.split(":")[0]))

        return ret

    @cluster(num_nodes=1)
    def test_tune_net(self):
        # TODO: generalize, only works on 4 core AWS for now.

        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.config_set("rpk.tune_network", "true")

        rpk.tune("net")

        # test irqbalance
        irqbalance_output = node.account.ssh_output(
            "systemctl status irqbalance"
        ).decode("utf-8")
        assert "--banirq" in irqbalance_output, (
            f"irqbalance is not configured correctly, got {irqbalance_output}"
        )

        # test interrupts
        interrupts_file = node.account.ssh_output("cat /proc/interrupts").decode(
            "utf-8"
        )
        interrupt_ids = self.parse_matching_interrupt_num_from_interrupt_file(
            interrupts_file, "ens5"
        )
        target_mq_setup = (
            node.account.ssh_output(
                "/opt/redpanda/bin/hwloc-distrib-redpanda 4 --single --restrict 0xf"
            )
            .decode("utf-8")
            .strip()
        )

        for interrupt_id, target_mask in zip(interrupt_ids, target_mq_setup.split()):
            cpu_affinity = (
                node.account.ssh_output(f"cat /proc/irq/{interrupt_id}/smp_affinity")
                .decode("utf-8")
                .strip()
            )
            assert int(cpu_affinity, 16) == int(target_mask, 16), (
                f"IRQ {interrupt_id} smp_affinity is not set correctly, got {cpu_affinity} expected {target_mask}"
            )

        # test RPS
        for i in range(4):
            rps_cpu = (
                node.account.ssh_output(
                    f"cat /sys/class/net/ens5/queues/rx-{i}/rps_cpus"
                )
                .decode("utf-8")
                .strip()
            )
            assert rps_cpu == "0", (
                f"rps_cpus for queue {i} is not set correctly, got {rps_cpu}"
            )

        # test RFS
        targetRFSTableSize = 32768

        total_rps_sock_flow_entries = int(
            node.account.ssh_output("cat /proc/sys/net/core/rps_sock_flow_entries")
            .decode("utf-8")
            .strip()
        )
        assert total_rps_sock_flow_entries == targetRFSTableSize, (
            f"rps_sock_flow_entries is not set correctly, got {total_rps_sock_flow_entries}"
        )

        for i in range(4):
            per_queue_rps_flow_count = int(
                node.account.ssh_output(
                    f"cat /sys/class/net/ens5/queues/rx-{i}/rps_flow_cnt"
                )
                .decode("utf-8")
                .strip()
            )
            assert per_queue_rps_flow_count == 0, (
                f"rps_flow_cnt for queue {i} is not set correctly, got {per_queue_rps_flow_count}"
            )

        # confirm that we also check cleanly
        check_output = rpk.check().split()

        for line in check_output:
            if line.startswith("NIC ens5"):
                assert line.endswith("true"), f"NIC check failed: {line}"
