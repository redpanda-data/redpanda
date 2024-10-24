# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import traceback
from typing import Any, Optional
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.services.redpanda import SISettings, CloudStorageType, get_cloud_storage_type
from rptest.services.redpanda_installer import RedpandaInstaller, RedpandaVersion, RedpandaVersionTriple
from rptest.services.workload_protocol import PWorkload
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.workload_producer_consumer import ProducerConsumerWorkload
from rptest.tests.workload_dummy import DummyWorkload, MinimalWorkload
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.workload_license import LicenseWorkload
from rptest.tests.workload_upgrade_config_defaults import SetLogSegmentMsMinConfig
from rptest.utils.mode_checks import skip_debug_mode, skip_fips_mode
from ducktape.mark import matrix


def expand_version(
        installer: RedpandaInstaller,
        version: Optional[RedpandaVersion]) -> RedpandaVersionTriple:
    if version is None:
        # return latest unsupported version
        return installer.latest_for_line(
            installer.latest_unsupported_line())[0]

    if version == RedpandaInstaller.HEAD:
        return installer.head_version()

    if len(version) == 3:
        return version

    # version is a release line, get latest minor for it
    return installer.latest_for_line(version)[0]


class WorkloadAdapter(PWorkload):
    """
    WorkloadAdapter is a wrapper around a PWorkload that keeps track of the state and save the error if one occurs.
    """
    NOT_STARTED = "not_started"
    STARTED = "started"
    STOPPED = "stopped"
    STOPPED_WITH_ERROR = "stopped_with_error"

    def __init__(self, workload: PWorkload, ctx: RedpandaTest,
                 installer: RedpandaInstaller) -> None:
        self.workload = workload
        self.ctx = ctx
        self.installer = installer
        self.state = WorkloadAdapter.NOT_STARTED
        self.error: Optional[Exception] = None
        self.earliest_v: Optional[tuple[int, int, int]] = None
        self.latest_v: Optional[tuple[int, int, int]] = None
        self.last_method_execution: dict[str, float] = {}

    def get_earliest_applicable_release(self) -> tuple[int, int, int]:
        if self.earliest_v is None:
            self.earliest_v = expand_version(
                self.installer,
                self.workload.get_earliest_applicable_release())

        return self.earliest_v

    def get_latest_applicable_release(self) -> tuple[int, int, int]:
        if self.latest_v is None:
            self.latest_v = expand_version(
                self.installer, self.workload.get_latest_applicable_release())
        return self.latest_v

    def _exec_workload_method(self, final_state: str, method_name: str, *args):
        """
        Executes a workload method and updates the state accordingly.
        Exceptions are saved and will prevent future execution of any method
        Execution is throttled to once per second
        """
        if self.state == WorkloadAdapter.STOPPED_WITH_ERROR:
            return None

        if method_name in self.last_method_execution and (
                self.last_method_execution[method_name] + 1) > time.time():
            # throttle execution: do not execute more than once per second
            return PWorkload.NOT_DONE

        try:
            result = getattr(self.workload, method_name)(*args)
            self.state = final_state
            if result == PWorkload.DONE and method_name in self.last_method_execution:
                # reset execution time for next round
                self.last_method_execution.pop(method_name)
            else:
                # keep track of execution time
                self.last_method_execution[method_name] = time.time()
            return result
        except Exception as e:
            self.ctx.logger.error(
                f"{self.workload.get_workload_name()} Exception in {method_name}(): {traceback.format_exception(e)}"
            )
            # the stacktrace is captured and saved in the trace variable
            # so that it can be used in the error message
            # along with time of failure
            self.time_of_failure = time.time()
            self.error = e
            self.state = WorkloadAdapter.STOPPED_WITH_ERROR
            try:
                # attempt at cleanup anyway
                self.workload.end()
            except:
                pass
            return None

    def begin(self):
        self._exec_workload_method(WorkloadAdapter.STARTED,
                                   PWorkload.begin.__name__)

    def end(self):
        self._exec_workload_method(WorkloadAdapter.STOPPED,
                                   PWorkload.end.__name__)

    def on_partial_cluster_upgrade(self,
                                   versions: dict[Any, RedpandaVersionTriple]):
        res = self._exec_workload_method(
            self.state, PWorkload.on_partial_cluster_upgrade.__name__,
            versions)
        return res if res is not None else PWorkload.DONE

    def get_workload_name(self):
        return self.workload.get_workload_name()

    def on_cluster_upgraded(self, version: RedpandaVersionTriple):
        res = self._exec_workload_method(
            self.state, PWorkload.on_cluster_upgraded.__name__, version)
        return res if res is not None else PWorkload.DONE


class RedpandaUpgradeTest(PreallocNodesTest):
    def __init__(self, test_context):
        # si_settings are needed for LicenseWorkload
        super().__init__(test_context=test_context,
                         num_brokers=3,
                         si_settings=SISettings(test_context),
                         node_prealloc_count=1,
                         node_ready_timeout_s=60)
        # it is expected that older versions of redpanda will generate this kind of errors, at least while we keep testing from v23.x
        self.redpanda.si_settings.set_expected_damage({
            'ntr_no_topic_manifest',
            'ntpr_no_manifest',
            'unknown_keys',
            'missing_segments',
        })

        self.installer = self.redpanda._installer

        # workloads that will be executed during this test
        workloads: list[PWorkload] = [
            DummyWorkload(self),
            MinimalWorkload(self),
            ProducerConsumerWorkload(self),
            SetLogSegmentMsMinConfig(self),
            # NOTE: due to issue/13180 the next workload is temporarily disabled
            # LicenseWorkload(self),
        ]

        # setup self as context for the workloads
        self.adapted_workloads: list[WorkloadAdapter] = [
            WorkloadAdapter(workload=w, ctx=self, installer=self.installer)
            for w in workloads
        ]

        self.upgrade_steps: list[RedpandaVersionTriple] = []

    def setUp(self):
        # at the end of setUp, self.upgrade_steps will look like this:
        # [(22, 1, 11), (22, 1, 10), (22, 1, 11),
        #  (22, 2, 11), (22, 2, 10), (22, 2, 11),
        #  (22, 3, 16), (22, 3, 15), (22, 3, 16),
        #  (23, 1, 7), (23, 1, 6), (23, 1, 7),
        #  (23, 2, 0)]

        # compute the upgrade steps, merging the upgrade steps of each workload
        workloads_steps = [
            self.load_version_range(w.get_earliest_applicable_release())
            for w in self.adapted_workloads
        ]

        latest_unsupported_line = self.installer.latest_unsupported_line()
        # keeping only releases older than latest EOL.
        forward_upgrade_steps = [
            v for v in sorted(set(sum(workloads_steps, start=[])))
            if v >= latest_unsupported_line
        ]

        # for each version, include a downgrade step to previous patch, then go to latest patch
        self.upgrade_steps: list[RedpandaVersionTriple] = []
        prev = forward_upgrade_steps[0]
        for v in forward_upgrade_steps:
            if v[0:2] != prev[0:2] and prev[2] > 1:
                # if the line has changed, add previous patch and again latest for line
                previous_patch = (prev[0], prev[1], prev[2] - 1)
                self.upgrade_steps.extend([previous_patch, prev])
            self.upgrade_steps.append(v)
            # update the latest_current_line
            prev = v

        self.logger.info(f"going through these versions: {self.upgrade_steps}")

    def _check_workload_list(self,
                             to_check_list: list[WorkloadAdapter],
                             version_param: RedpandaVersionTriple
                             | dict[Any, RedpandaVersionTriple],
                             partial_update: bool = False):
        # run checks on all the workloads in the to_check_list
        # each check could take multiple runs, so loop on a list of it until exhaustion

        str_update_kind = "progress check done" if not partial_update else "partial progress check done"
        progress_lambda = lambda w, v_param: w.on_cluster_upgraded(v_param) if not partial_update \
                                                            else w.on_partial_cluster_upgrade(v_param)

        while len(to_check_list) > 0:
            start_time = time.time()
            self.logger.info(
                f"checking { str_update_kind }progress for {[w.get_workload_name() for w in to_check_list]}"
            )
            # check progress of each workload in the to_check_list
            # and if a workload is done, remove it from the list
            status_progress = {
                w: progress_lambda(w, version_param)
                for w in to_check_list
            }
            for w, state in status_progress.items():
                if state == PWorkload.DONE:
                    self.logger.info(
                        f"{w.get_workload_name()} {str_update_kind}")
                    to_check_list.remove(w)

            if delay := 1 - (time.time() - start_time) > 0:
                # ensure that checks are not performed too fast, by requesting a delay of 1 second
                time.sleep(delay)

    def cluster_version(self) -> int:
        return Admin(self.redpanda).get_features()['cluster_version']

    # before v24.2, dns query to s3 endpoint do not include the bucketname, which is required for AWS S3 fips endpoints
    @skip_fips_mode
    @skip_debug_mode
    @cluster(num_nodes=4)
    # TODO(vlad): Allow this test on ABS once we have at least two versions
    # of Redpanda that support Azure Hierarchical Namespaces.
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_workloads_through_releases(self, cloud_storage_type):
        # this callback will be called between each upgrade, in a mixed version state
        def mid_upgrade_check(raw_versions: dict[Any, RedpandaVersion]):
            rp_versions = {
                k: expand_version(self.installer, v)
                for k, v in raw_versions.items()
            }
            next_version = max(rp_versions.values())
            # check only workload that are active and that can operate with next_version
            to_check_workloads = [
                w for w in self.adapted_workloads
                if w.state == WorkloadAdapter.STARTED
                and next_version <= w.get_latest_applicable_release()
            ]
            self._check_workload_list(to_check_list=to_check_workloads,
                                      version_param=rp_versions,
                                      partial_update=True)

        # upgrade loop: for each version
        for current_version in self.upgrade_through_versions(
                self.upgrade_steps,
                already_running=False,
                mid_upgrade_check=mid_upgrade_check,
                license_required=True):
            current_version = expand_version(self.installer, current_version)
            # setup workload that could start at current_version
            for w in self.adapted_workloads:
                if w.state == WorkloadAdapter.NOT_STARTED and current_version >= w.get_earliest_applicable_release(
                ):
                    self.logger.info(f"setup {w.get_workload_name()}")
                    w.begin()  # this will set in a STARTED state

            # run checks on all the started workload.
            # each check could take multiple runs, so loop on a list of it until exhaustion
            self._check_workload_list(to_check_list= \
                                      [w for w in self.adapted_workloads if w.state == WorkloadAdapter.STARTED],
                                      version_param=current_version)

            # stop workload that can't operate with next_version
            for w in self.adapted_workloads:
                if w.state == WorkloadAdapter.STARTED and current_version == w.get_latest_applicable_release(
                ):
                    self.logger.info(f"teardown of {w.get_workload_name()}")
                    w.end()

            # quick exit: terminate loop if no workload is active
            if len([
                    w for w in self.adapted_workloads
                    if w.state == WorkloadAdapter.STARTED
                    or w.state == WorkloadAdapter.NOT_STARTED
            ]) == 0:
                self.logger.info(
                    f"terminating upgrade loop at version {current_version}, no workload is active"
                )
                break

        # check workloads stopped with error, and format the exceptions into concat_error
        concat_error: list[str] = []
        for w in self.adapted_workloads:
            if w.state == WorkloadAdapter.STOPPED_WITH_ERROR:
                concat_error.append(
                    f"{w.get_workload_name()} failed at {w.time_of_failure} - {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(w.time_of_failure))}"
                )
                concat_error.extend(traceback.format_exception(w.error))
        # if concat_error is not empty, raise it as an exception
        if len(concat_error) > 0:
            raise Exception("\n".join(concat_error))

        # Validate that the data structures written by a mixture of historical
        # versions remain readable by our current debug tools
        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.nodes:
            controller_records = log_viewer.read_controller(node=node)
            self.logger.info(
                f"Read {len(controller_records)} controller records from node {node.name} successfully"
            )
