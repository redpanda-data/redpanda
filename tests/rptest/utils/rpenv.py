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


def sample_license():
    """
    Returns the sample license from the env if it exists, asserts if its
    missing and the environment is CI
    """
    license = os.environ.get("REDPANDA_SAMPLE_LICENSE", None)
    if license is None:
        is_ci = os.environ.get("CI", "false")
        assert is_ci == "false"
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


def skip_if_cloud_type_is(disallowed_values):
    """
    Skip a test if the CLOUD_TYPE environment variable matches one of the disallowed values.

    :param disallowed_values: List of string values. The test is skipped if CLOUD_TYPE's value is in this list.
    
    Example: @skip_if_cloud_type_is('CLOUD_TYPE_FMC')
    """
    def decorator(test_function):
        # Directly check the 'CLOUD_TYPE' environment variable
        if os.getenv('CLOUD_TYPE') in disallowed_values:
            return ignore(test_function)
        return test_function

    return decorator
