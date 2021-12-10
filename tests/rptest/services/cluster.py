# Copyright 2021 Vectorized, Inc.
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


def cluster(**kwargs):
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
            f(self, *args, **kwargs)

        wrapped.marks = f.marks
        wrapped.mark_names = f.mark_names

        return wrapped

    return cluster_use_metadata_adder
