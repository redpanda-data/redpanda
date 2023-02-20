# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from functools import reduce


def flat_map(fn, input_list):
    """
    Expect a function `fn` that accepts 1 argument and returns a list.

    Returns the appended results of all of the invocations of fn(x) against
    each argument in `input_list`.
    """
    return reduce(lambda acc, x: acc + fn(x), input_list, [])
