# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import functools
from re import Pattern
import time
from typing import Any, Protocol

import psutil
from ducktape.mark._mark import Mark
from ducktape.mark.resource import ClusterUseMetadata
from ducktape.tests.test import TestContext

from rptest.services.redpanda import RedpandaServiceBase, RedpandaService, \
    RedpandaServiceCloud
from rptest.services.redpanda_types import LogAllowList
from rptest.utils.allow_logs_on_predicate import AllowLogsOnPredicate


def cluster(log_allow_list: LogAllowList | None = None,
            check_allowed_error_logs: bool = True,
            check_for_storage_usage_inconsistencies: bool = True,
            **kwargs: Any):
    """
    Drop-in replacement for Ducktape `cluster` that imposes additional
    redpanda-specific checks and defaults.

    These go into a decorator rather than setUp/tearDown methods
    because they may raise errors that we would like to expose
    as test failures.
    """
    def all_redpandas(test):
        """
        Most tests have a single RedpandaService at self.redpanda, but
        it is legal to create multiple instances, e.g. for read replica tests.

        We find all replicas by traversing ducktape's internal service registry.
        """
        rp = test.redpanda
        assert isinstance(rp, RedpandaServiceBase) or isinstance(
            rp, RedpandaServiceCloud)
        yield rp

        for svc in test.test_context.services:
            if isinstance(svc, RedpandaServiceBase) or isinstance(
                    svc, RedpandaServiceCloud) and svc is not test.redpanda:
                yield svc

    def log_local_load(test_name, logger, t_initial, initial_disk_stats):
        """
        Log indicators of system load on the machine running ducktape tests.  When
        running tests in a single-node docker environment, this is useful to help
        diagnose whether slow-running/flaky tests are the victim of an overloaded
        node.
        """
        load = psutil.getloadavg()
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk_stats = psutil.disk_io_counters()
        assert disk_stats is not None, 'psutil.disk_io_counters() failed'
        disk_deltas = {}
        disk_rates = {}
        runtime = time.time() - t_initial
        for f in disk_stats._fields:
            a = getattr(initial_disk_stats, f)
            b = getattr(disk_stats, f)
            disk_deltas[f] = b - a
            disk_rates[f] = (b - a) / runtime

        logger.info(f"Load average after {test_name}: {load}")
        logger.info(f"Memory after {test_name}: {memory}")
        logger.info(f"Swap after {test_name}: {swap}")
        logger.info(f"Disk activity during {test_name}: {disk_deltas}")
        logger.info(f"Disk rates during {test_name}: {disk_rates}")

    def cluster_use_metadata_adder(f):
        Mark.mark(f, ClusterUseMetadata(**kwargs))

        class HasRedpanda(Protocol):
            redpanda: RedpandaServiceBase | RedpandaServiceCloud
            test_context: TestContext

        @functools.wraps(f)
        def wrapped(self: HasRedpanda, *args: Any, **kwargs: Any):
            # This decorator will only work on test classes that have a RedpandaService,
            # such as RedpandaTest subclasses
            assert hasattr(self, 'redpanda')

            # some checks only make sense on "vanilla" Redpanda nodes, i.e., those created on
            # VMs or docker containers inside a ducktape node, where we have ssh access, for
            # other environemnts we will skip those checks

            t_initial = time.time()
            disk_stats_initial = psutil.disk_io_counters()
            if log_allow_list is not None:
                for entry in log_allow_list:
                    if isinstance(entry, AllowLogsOnPredicate):
                        entry.initialize(self)
            try:
                r = f(self, *args, **kwargs)
            except:
                if self.redpanda is None:
                    # We failed so early there isn't even a RedpandaService instantiated
                    raise

                log_local_load(self.test_context.test_name,
                               self.redpanda.logger, t_initial,
                               disk_stats_initial)

                for redpanda in all_redpandas(self):
                    redpanda.logger.exception(
                        f"Test failed, doing failure checks on {redpanda.who_am_i()}..."
                    )

                    # Disabled to avoid addr2line hangs
                    # (https://github.com/redpanda-data/redpanda/issues/5004)
                    # self.redpanda.decode_backtraces()

                    if isinstance(redpanda, RedpandaServiceBase):
                        redpanda.cloud_storage_diagnostics()
                    if isinstance(redpanda,
                                  RedpandaService | RedpandaServiceCloud):
                        redpanda.raise_on_crash(log_allow_list=log_allow_list)

                raise
            else:
                if not isinstance(self.redpanda,
                                  RedpandaServiceBase | RedpandaServiceCloud):
                    # If None we passed without instantiating a RedpandaService, for example
                    # in a skipped test.
                    # Also skip if we are running against the cloud
                    return r

                if isinstance(self.redpanda, RedpandaServiceCloud):
                    # Call copy logs function for RedpandaServiceCloud
                    # It can't be called from usual ducktape routing due
                    # to different inheritance structure
                    self.redpanda.copy_cloud_logs(t_initial)

                log_local_load(self.test_context.test_name,
                               self.redpanda.logger, t_initial,
                               disk_stats_initial)

                # In debug mode, any test writing too much traffic will impose too much
                # load on the system and destabilize other tests.  Detect this with a
                # post-test check for total bytes written.
                debug_mode_data_limit = 64 * 1024 * 1024
                if getattr(self, 'debug_mode', False) is True:
                    bytes_written = self.redpanda.estimate_bytes_written()
                    if bytes_written is not None:
                        self.redpanda.logger.info(
                            f"Estimated bytes written: {bytes_written}")
                        if bytes_written > debug_mode_data_limit:
                            self.redpanda.logger.error(
                                f"Debug-mode test wrote too much data ({int(bytes_written) // (1024 * 1024)}MiB)"
                            )

                for redpanda in all_redpandas(self):
                    redpanda.logger.info(
                        f"Test passed, doing log checks on {redpanda.who_am_i()}..."
                    )

                    # the following checks don't work on cloud instances but we don't expect
                    # any of those as we already early-outed above when Redpanda is not
                    # RSB (technically, it might be that self.redpanda is RSB but some
                    # additional redpanda in the registry is, but that situation never arises
                    # in practice in our current tests)
                    assert isinstance(
                        redpanda, RedpandaServiceBase | RedpandaServiceCloud)

                    if check_allowed_error_logs:
                        # Only do log inspections on tests that are otherwise
                        # successful.  This executes *before* the end-of-test
                        # shutdown, thereby avoiding having to add the various
                        # gate_closed etc errors to our allow list.
                        # TODO: extend this to cover shutdown logging too, and
                        # clean up redpanda to not log so many errors on shutdown.
                        try:
                            # We need test start time for RedpandaServiceCloud
                            if isinstance(redpanda, RedpandaServiceCloud):
                                redpanda.raise_on_bad_logs(
                                    allow_list=log_allow_list,
                                    test_start_time=t_initial)
                            else:
                                redpanda.raise_on_bad_logs(
                                    allow_list=log_allow_list)
                        except:
                            # Perform diagnostics only for Local run
                            if isinstance(redpanda, RedpandaServiceBase):
                                redpanda.cloud_storage_diagnostics()
                            raise

                    # Do a check if this is the cloud
                    # since the rest not applies to RedpandaServiceCloud class
                    if isinstance(redpanda, RedpandaServiceCloud):
                        return r

                    if check_for_storage_usage_inconsistencies:
                        try:
                            redpanda.raise_on_storage_usage_inconsistency()
                        except:
                            redpanda.cloud_storage_diagnostics()
                            raise

                self.redpanda.validate_controller_log()

                if self.redpanda._si_settings is not None and not self.redpanda.si_settings.skip_end_of_test_scrubbing:
                    try:
                        self.redpanda.maybe_do_internal_scrub()
                        self.redpanda.stop_and_scrub_object_storage()
                    except:
                        self.redpanda.cloud_storage_diagnostics()
                        raise
                else:
                    # stop here explicitly to fail if stop times out, otherwise ducktape won't catch it
                    self.redpanda.stop()

                # Finally, if the test passed and all post-test checks
                # also passed, we may trim the logs to INFO level to
                # save space.
                for redpanda in all_redpandas(self):
                    redpanda.trim_logs()

                return r

        # Propagate ducktape markers (e.g. parameterize) to our function
        # wrapper
        wrapped.marks = f.marks
        wrapped.mark_names = f.mark_names

        return wrapped

    return cluster_use_metadata_adder
