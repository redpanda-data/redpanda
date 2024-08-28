# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import dataclasses
import sys
from time import sleep
import random
from collections.abc import Callable

from ducktape.mark import matrix

from rptest.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector
from rptest.services.cluster import cluster
from rptest.services.chaos.types import NoProgressError
import rptest.services.chaos.workloads.all as workloads
import rptest.services.chaos.faults.all as faults


@dataclasses.dataclass
class TimingConfig:
    wait_progress_timeout_s: int = 20

    warmup_s: int = 20

    no_fault_steady_s: int = 180

    recoverable_fault_steady_s: int = 60
    recoverable_fault_impact_s: int = 60
    recoverable_fault_recovery_s: int = 60

    oneoff_fault_steady_s: int = 60
    oneoff_fault_recovery_s: int = 120


@dataclasses.dataclass
class CheckProgressDuringFaultConfig:
    min_delta: int = 100
    selector: str = "all"


class SingleFaultTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self,
            workload: workloads.WorkloadServiceBase,
            fault: faults.FaultBase | None,
            timings: TimingConfig = TimingConfig(),
            check_progress_during_fault: CheckProgressDuringFaultConfig
            | None = None):
        try:
            self._do_run(workload, fault, timings, check_progress_during_fault)
        finally:
            workload.stop_and_validate()

            if sys.exc_info()[0] is None:
                # check crashes only if the test passed,
                # if it failed, the check will be performed automatically
                self.redpanda.raise_on_crash()

    def _transfer_leadership(self,
                             topic,
                             target_id,
                             partition=0,
                             namespace="kafka",
                             current_leader_id=None,
                             admin=None,
                             timeout_s=10):
        self.logger.info(
            f"transferring {namespace}/{topic}/{partition} leader to {target_id}"
        )

        if admin is None:
            admin = Admin(self.redpanda)

        def transfer_successful():
            nonlocal current_leader_id
            if current_leader_id is None:
                current_leader_id = admin.await_stable_leader(
                    namespace=namespace, topic=topic, partition=partition)
            if current_leader_id == target_id:
                return True

            admin.transfer_leadership_to(namespace=namespace,
                                         topic=topic,
                                         partition=partition,
                                         leader_id=current_leader_id,
                                         target_id=target_id)
            current_leader_id = None

        wait_until(transfer_successful,
                   timeout_sec=timeout_s,
                   backoff_sec=1,
                   err_msg="failed to transfer leadership",
                   retry_on_exc=True)

    def _do_run(self, workload: workloads.WorkloadServiceBase,
                fault: faults.FaultBase | None, timings: TimingConfig,
                check_progress_during_fault: CheckProgressDuringFaultConfig):
        if timings.warmup_s > 0:
            self.logger.info(f"warming up for {timings.warmup_s}s")
            sleep(timings.warmup_s)

        self.logger.info(f"start measuring")
        for node in workload.nodes:
            workload.emit_event(node, "measure")

        if fault == None:
            if timings.no_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.no_fault_steady_s} seconds "
                    f"to record steady state")
                sleep(timings.no_fault_steady_s)
        elif isinstance(fault, faults.RecoverableFault):
            if timings.recoverable_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.recoverable_fault_steady_s} seconds "
                    f" to record steady state")
                sleep(timings.recoverable_fault_steady_s)

            for node in workload.nodes:
                workload.emit_event(node, "injecting")
            self.logger.info(f"injecting '{fault.name}'")
            fault.inject()
            self.logger.info(f"injected '{fault.name}'")

            try:
                for node in workload.nodes:
                    workload.emit_event(node, "injected")

                after_fault_info = {}
                for node in workload.nodes:
                    after_fault_info[node.name] = workload.info(node)

                if timings.recoverable_fault_impact_s > 0:
                    self.logger.info(
                        f"wait for {timings.recoverable_fault_impact_s} seconds "
                        "to record impacted state")
                    sleep(timings.recoverable_fault_impact_s)
                self.logger.info(
                    f"done waiting for "
                    f"{timings.recoverable_fault_impact_s} seconds")

                before_heal_info = {}
                for node in workload.nodes:
                    before_heal_info[node.name] = workload.info(node)

                if check_progress_during_fault is not None:
                    results = []
                    for node_name in before_heal_info.keys():
                        delta = (before_heal_info[node_name].succeeded_ops -
                                 after_fault_info[node_name].succeeded_ops)
                        self.logger.debug(
                            f"progress during fault for client node "
                            f"{node_name}: {delta}")
                        results.append(
                            delta >= check_progress_during_fault.min_delta)
                    selector = check_progress_during_fault.selector
                    if selector not in ("all", "any"):
                        raise Exception(f"unknown selector: {selector}")
                    if selector == "all" and not all(results):
                        raise NoProgressError("no progress during fault")
                    elif selector == "any" and not any(results):
                        raise NoProgressError("no progress during fault")

                for node in workload.nodes:
                    workload.emit_event(node, "healing")
            finally:
                self.logger.info(f"healing '{fault.name}'")
                fault.heal()
                self.logger.info(f"healed '{fault.name}'")
                for node in workload.nodes:
                    workload.emit_event(node, "healed")

            if timings.recoverable_fault_recovery_s > 0:
                self.logger.info(
                    f"wait for {timings.recoverable_fault_recovery_s} seconds "
                    f"to record recovering state")
                sleep(timings.recoverable_fault_recovery_s)
        elif isinstance(fault, faults.OneoffFault):
            if timings.oneoff_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.oneoff_fault_steady_s} seconds "
                    f"to record steady state")
                sleep(timings.oneoff_fault_steady_s)

            for node in workload.nodes:
                workload.emit_event(node, "injecting")
            self.logger.info(f"injecting '{fault.name}'")
            fault.execute()
            self.logger.info(f"injected '{fault.name}'")
            for node in workload.nodes:
                workload.emit_event(node, "injected")

            if timings.oneoff_fault_recovery_s > 0:
                self.logger.info(
                    f"wait for {timings.oneoff_fault_recovery_s} seconds "
                    f"to record recovering / impacted state")
                sleep(timings.oneoff_fault_recovery_s)
        else:
            raise Exception(f"Unknown fault type {type(fault)}")

    def tearDown(self):
        FailureInjector(self.redpanda)._heal_all()
        super().tearDown()


class SingleTopicTest(SingleFaultTestBase):
    @dataclasses.dataclass
    class ListOffsetsCase:
        make_fault: Callable[[SingleFaultTestBase],
                             faults.FaultBase] | None = None
        check_progress_during_fault: CheckProgressDuringFaultConfig | None = None

    LIST_OFFSETS_CASES = {
        "baseline":
        ListOffsetsCase(),
        "isolate_leader":
        ListOffsetsCase(
            lambda test: faults.IsolateLeaderFault(test.redpanda,
                                                   topic=test.topic),
            CheckProgressDuringFaultConfig(),
        )
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)
        self.topics = [
            TopicSpec(name="topic1", replication_factor=3, partition_count=1)
        ]

    def prepare(self, workload: workloads.WorkloadServiceBase,
                timings: TimingConfig):
        admin = Admin(self.redpanda)

        # wait for the topic to come online
        admin.await_stable_leader(topic=self.topic, timeout_s=20)

        workload.start()
        workload.start_workload()

        topic_leader_id = admin.await_stable_leader(self.topic)
        self.logger.debug(f"leader id of '{self.topic}': {topic_leader_id}")

        controller_leader_id = admin.await_stable_leader("controller",
                                                         namespace="redpanda")
        self.logger.debug(f"controller leader id: {controller_leader_id}")

        if topic_leader_id == controller_leader_id:
            other_ids = [
                self.redpanda.node_id(n) for n in self.redpanda.nodes
                if self.redpanda.node_id(n) != controller_leader_id
            ]
            self._transfer_leadership(namespace="redpanda",
                                      topic="controller",
                                      partition=0,
                                      current_leader_id=controller_leader_id,
                                      target_id=random.choice(other_ids))

        self.logger.info(f"waiting for progress")

        workload.wait_progress(timeout_sec=timings.wait_progress_timeout_s)

    @cluster(num_nodes=4)
    @matrix(case_id=LIST_OFFSETS_CASES.keys())
    def test_list_offsets(self, case_id):
        case = self.LIST_OFFSETS_CASES[case_id]
        workload = workloads.ListOffsetsWorkload(self.test_context,
                                                 self.redpanda.brokers(),
                                                 self.topic)
        fault = None
        if case.make_fault:
            fault = case.make_fault(self)
        timings = TimingConfig()

        self.prepare(workload, timings)
        self.run(workload, fault, timings, case.check_progress_during_fault)
