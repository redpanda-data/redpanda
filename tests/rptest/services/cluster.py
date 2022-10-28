# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import functools

from ducktape.mark.resource import ClusterUseMetadata
from ducktape.mark._mark import Mark


def cluster(log_allow_list=None, check_allowed_error_logs=True, **kwargs):
    """
    Drop-in replacement for Ducktape `cluster` that imposes additional
    redpanda-specific checks and defaults.

    These go into a decorator rather than setUp/tearDown methods
    because they may raise errors that we would like to expose
    as test failures.
    """
    def cluster_use_metadata_adder(f):
        Mark.mark(f, ClusterUseMetadata(**kwargs))

        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            # This decorator will only work on test classes that have a RedpandaService,
            # such as RedpandaTest subclasses
            assert hasattr(self, 'redpanda')

            try:
                r = f(self, *args, **kwargs)
            except:
                if not hasattr(self, 'redpanda') or self.redpanda is None:
                    # We failed so early there isn't even a RedpandaService instantiated
                    raise

                self.redpanda.logger.exception(
                    "Test failed, doing failure checks...")

                # Disabled to avoid addr2line hangs
                # (https://github.com/redpanda-data/redpanda/issues/5004)
                # self.redpanda.decode_backtraces()

                self.redpanda.raise_on_crash()

                self.redpanda.cloud_storage_diagnostics()

                raise
            else:
                if not hasattr(self, 'redpanda') or self.redpanda is None:
                    # We passed without instantiating a RedpandaService, for example
                    # in a skipped test
                    return r

                self.redpanda.logger.info("Test passed, doing log checks...")
                if check_allowed_error_logs:
                    # Only do log inspections on tests that are otherwise
                    # successful.  This executes *before* the end-of-test
                    # shutdown, thereby avoiding having to add the various
                    # gate_closed etc errors to our allow list.
                    # TODO: extend this to cover shutdown logging too, and
                    # clean up redpanda to not log so many errors on shutdown.
                    self.redpanda.raise_on_bad_logs(allow_list=log_allow_list)
                self.redpanda.trim_logs()
                return r

        # Propagate ducktape markers (e.g. parameterize) to our function
        # wrapper
        wrapped.marks = f.marks
        wrapped.mark_names = f.mark_names

        return wrapped

    return cluster_use_metadata_adder
