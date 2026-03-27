# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import dataclasses
import functools
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol

import psutil
from ducktape.mark._mark import Mark
from ducktape.mark.resource import ClusterUseMetadata
from ducktape.tests.test import TestContext

from rptest.services.redpanda import (
    RedpandaService,
    RedpandaServiceCloud,
)
from rptest.services.redpanda_types import LogAllowList
from rptest.utils.allow_logs_on_predicate import AllowLogsOnPredicate


class HasRedpanda(Protocol):
    redpanda: RedpandaService | RedpandaServiceCloud | None
    test_context: TestContext


def cluster(
    log_allow_list: LogAllowList = (),
    check_allowed_error_logs: bool = True,
    check_for_storage_usage_inconsistencies: bool = True,
    **kwargs: Any,
):
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
        assert isinstance(rp, (RedpandaService, RedpandaServiceCloud))
        yield rp

        for svc in test.test_context.services:
            if (
                isinstance(svc, (RedpandaService, RedpandaServiceCloud))
                and svc is not test.redpanda
            ):
                yield svc

    def log_local_load(
        test_name: str, logger: Any, t_initial: float, initial_disk_stats: Any
    ):
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
        assert disk_stats is not None, "psutil.disk_io_counters() failed"
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

    @dataclass
    class test_state:
        t_initial: float
        disk_stats_initial: Any

    def _do_post_test_checks(
        self: HasRedpanda, test_failed: bool, state: test_state
    ) -> dict[str, Any]:
        extra_results: dict[str, Any] = {}

        if self.redpanda is None:
            # We failed so early there isn't even a RedpandaService instantiated, or
            # we passed without instantiating a RedpandaService, for example
            # in a skipped test.
            return extra_results

        status_str = "failed" if test_failed else "passed"

        log_local_load(
            self.test_context.test_name,
            self.redpanda.logger,
            state.t_initial,
            state.disk_stats_initial,
        )

        # In debug mode, any test writing too much traffic will impose too much
        # load on the system and destabilize other tests.  Detect this with a
        # post-test check for total bytes written.
        debug_mode_data_limit = 64 * 1024 * 1024
        if not test_failed and getattr(self, "debug_mode", False) is True:
            assert isinstance(self.redpanda, RedpandaService)
            bytes_written = self.redpanda.estimate_bytes_written()
            if bytes_written is not None:
                self.redpanda.logger.info(f"Estimated bytes written: {bytes_written}")
                if bytes_written > debug_mode_data_limit:
                    self.redpanda.logger.error(
                        f"Debug-mode test wrote too much data ({int(bytes_written) // (1024 * 1024)}MiB)"
                    )

        for redpanda in all_redpandas(self):
            redpanda.logger.exception(
                f"Test {status_str}, doing failure checks on {redpanda.who_am_i()}..."
            )

            redpanda.export_cluster_config()

            if not test_failed and check_allowed_error_logs:
                # Only do log inspections on tests that are otherwise
                # successful.  This executes *before* the end-of-test
                # shutdown, thereby avoiding having to add the various
                # gate_closed etc errors to our allow list.
                # TODO: extend this to cover shutdown logging too, and
                # clean up redpanda to not log so many errors on shutdown.
                try:
                    redpanda.raise_on_bad_logs(
                        allow_list=log_allow_list, test_start_time=state.t_initial
                    )
                except:
                    # Perform diagnostics only for Local run
                    if isinstance(redpanda, RedpandaService):
                        redpanda.cloud_storage_diagnostics()
                    raise

            if isinstance(redpanda, RedpandaService):
                if test_failed:
                    redpanda.cloud_storage_diagnostics()
                elif check_for_storage_usage_inconsistencies:
                    try:
                        redpanda.raise_on_storage_usage_inconsistency()
                    except:
                        redpanda.cloud_storage_diagnostics()
                        raise

            if isinstance(redpanda, RedpandaServiceCloud):
                return extra_results

            if test_failed:
                redpanda.raise_on_crash(log_allow_list=log_allow_list)

        assert isinstance(
            self.redpanda, RedpandaService
        )  # we already returned above for cloud case

        stopped = False

        self.redpanda.validate_controller_log()

        if (
            self.redpanda._si_settings is not None
            and not self.redpanda.si_settings.skip_end_of_test_scrubbing
            and not test_failed
        ):
            try:
                self.redpanda.validate_metastore()
                self.redpanda.maybe_do_internal_scrub()
                usage = self.redpanda.stop_and_scrub_object_storage()
                stopped = True
                extra_results["object_storage_usage"] = dataclasses.asdict(usage)
            except:
                self.redpanda.cloud_storage_diagnostics()
                raise

        # stop the cluster before decoding the logs so the decode process doesn't
        # complete with the Redpanda nodes (failed case) and so that we catch stop
        # timeouts (passed case)
        if not stopped:
            try:
                self.redpanda.logger.info(
                    f"While finishing {status_str} test, stopping Redpanda service {self.redpanda.who_am_i()}..."
                )
                self.redpanda.stop()
            except Exception as e:
                # ignore this since we want to propagate the original exception
                self.redpanda.logger.info(
                    f"While finishing {status_str} test, failed to stop Redpanda service: {e}"
                )
                if not test_failed:
                    raise

        # Finally, if the test passed and all post-test checks
        # also passed, we may trim the logs to INFO level to
        # save space.
        if not test_failed:
            for redpanda in all_redpandas(self):
                assert isinstance(redpanda, RedpandaService)
                redpanda.trim_logs()

        return extra_results

    @staticmethod
    def _check_injected_args_roundtrip(args: Any):
        """Check that the injected args roundtrip through a JSON encode/decode, if
        they do not, the test cannot be correctly parameterized on the command line."""

        # In ducktape, "injected args", i.e. those passes with @matrix or
        # @parametrize, annotations, should always use "json roundtrip-able"
        # types, otherwise they cannot be re-invoked on the command line (or
        # using a test suite file), as DT can only parse JSON arguments. Among
        # other things, this means that PT retry will always fail to re-invoke
        # test during retry and these tests will simply fail if they fail in their
        # initial invocation.
        #
        # This means that you can only use types that are JSON de/serializable, such
        # as: str, int, float, bool, None, list, dict.  You should not use any
        # user defined types, or tuples, sets, enums, etc, as these do no roundtrip
        # correctly. As a workaround, for tuples pass lists, for sets pass lists,
        # and for any more complicated type, you can just pass a string identifier
        # and look up the real type inside the test from a dict[str, MyComplexType].

        try:
            roundtripped = json.loads(json.dumps(args))
        except Exception as e:
            raise ValueError(
                "Injected args cannot be serialized to JSON "
                f"or do not roundtrip correctly: {args}"
            ) from e

        if roundtripped != args:
            raise RuntimeError(  # see source (here) line for details
                "Injected args do not roundtrip through JSON: "
                f"original={args}, roundtripped={roundtripped}"
            )

    def cluster_use_metadata_adder(f: Callable[..., Any]):
        Mark.mark(f, ClusterUseMetadata(**kwargs))

        @functools.wraps(f)
        def wrapped(self: HasRedpanda, *args: Any, **kwargs: Any):
            # This decorator will only work on test classes that have a RedpandaService,
            # such as RedpandaTest subclasses
            assert hasattr(self, "redpanda")
            assert hasattr(self, "test_context")

            _check_injected_args_roundtrip(self.test_context.injected_args)

            # some checks only make sense on "vanilla" Redpanda nodes, i.e., those created on
            # VMs or docker containers inside a ducktape node, where we have ssh access, for
            # other environemnts we will skip those checks

            test_state_initial = test_state(
                t_initial=time.time(), disk_stats_initial=psutil.disk_io_counters()
            )

            for entry in log_allow_list:
                if isinstance(entry, AllowLogsOnPredicate):
                    entry.initialize(self)

            try:
                try:
                    r = f(self, *args, **kwargs)
                    test_results: dict[str, Any] = {"result": r}
                except:
                    _do_post_test_checks(self, True, test_state_initial)
                    raise

                test_results |= _do_post_test_checks(self, False, test_state_initial)
                test_results["usage_stats"] = {
                    r.who_am_i(): r.usage_stats_dict for r in all_redpandas(self)
                }
            except:
                # for any failure, we decode backtraces
                if isinstance(self.redpanda, RedpandaService):
                    self.redpanda.decode_backtraces()
                raise

            return test_results

        # Propagate ducktape markers (e.g. parameterize) to our function
        # wrapper
        wrapped.marks = f.marks  # type: ignore
        wrapped.mark_names = f.mark_names  # type: ignore

        return wrapped

    return cluster_use_metadata_adder
