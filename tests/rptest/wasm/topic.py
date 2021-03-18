# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re


def construct_materialized_topic(source, destination):
    return f"{source}.${destination}$"


def get_source_topic(materialized_topic):
    val = re.search("(.*)\.\$(.*)\$", materialized_topic)
    return None if val is None else val[1]
