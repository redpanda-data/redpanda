# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Utilities for checking the environment of a test

import os


def sample_license(assert_exists=False):
    """
    Returns the sample license from the env if it exists, asserts if its
    missing and the environment is CI
    """
    license = os.environ.get("REDPANDA_SAMPLE_LICENSE", None)
    if license is None:
        is_ci = os.environ.get("CI", "false")
        assert is_ci == "false"
        assert not assert_exists, (
            "No enterprise license found in the environment variable. "
            "Please follow these instructions to get a sample license for local development: "
            "https://redpandadata.atlassian.net/l/cp/4eeNEgZW")
        return None
    return license


class IsCIOrNotEmpty:
    """
    Comparison with this object is true if the environemnt is CI, or if the
    other value is not empty.

    Useful as a markup on a test that must run in CI or when the env is present:

    @env(REQUIRED_ENV=IsCIOrNotEmpty())
    """
    def __init__(self):
        self.is_ci = os.environ.get('CI', 'false') != 'false'

    def __eq__(self, other: str) -> bool:
        return bool(other) or self.is_ci
