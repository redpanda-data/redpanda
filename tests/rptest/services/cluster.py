# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import functools

from rptest.services.redpanda import RedpandaService
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
    def all_redpandas(test):
        """
        Most tests have a single RedpandaService at self.redpanda, but
        it is legal to create multiple instances, e.g. for read replica tests.
        
        We find all replicas by traversing ducktape's internal service registry.
        """
        yield test.redpanda

        for svc in test.test_context.services:
            if isinstance(svc, RedpandaService) and svc is not test.redpanda:
                yield svc

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

                for redpanda in all_redpandas(self):
                    redpanda.logger.exception(
                        f"Test failed, doing failure checks on {redpanda.who_am_i()}..."
                    )

                    # Disabled to avoid addr2line hangs
                    # (https://github.com/redpanda-data/redpanda/issues/5004)
                    # self.redpanda.decode_backtraces()

                    redpanda.cloud_storage_diagnostics()

                    redpanda.raise_on_crash()

                raise
            else:
                if not hasattr(self, 'redpanda') or self.redpanda is None:
                    # We passed without instantiating a RedpandaService, for example
                    # in a skipped test
                    return r

                for redpanda in all_redpandas(self):
                    redpanda.logger.info(
                        f"Test passed, doing log checks on {redpanda.who_am_i()}..."
                    )
                    if check_allowed_error_logs:
                        # Only do log inspections on tests that are otherwise
                        # successful.  This executes *before* the end-of-test
                        # shutdown, thereby avoiding having to add the various
                        # gate_closed etc errors to our allow list.
                        # TODO: extend this to cover shutdown logging too, and
                        # clean up redpanda to not log so many errors on shutdown.
                        try:
                            redpanda.raise_on_bad_logs(
                                allow_list=log_allow_list)
                        except:
                            redpanda.cloud_storage_diagnostics()
                            raise

                if self.redpanda.si_settings is not None:
                    try:
                        self.redpanda.stop_and_scrub_object_storage()
                    except:
                        self.redpanda.cloud_storage_diagnostics()
                        raise

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
