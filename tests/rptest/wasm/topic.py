# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

# Regex for materialized topics
mt_rgx = re.compile("(.*)\\.\_(.*)\_")

# Regex for normal kafka topics
topic_rgx = re.compile("^([a-zA-Z0-9\\.\\_\\-])*$")


def get_source_topic(materialized_topic):
    val = mt_rgx.match(materialized_topic)
    return None if val is None else val[1]


def get_dest_topic(materialized_topic):
    val = mt_rgx.match(materialized_topic)
    return None if val is None else val[2]


def is_materialized_topic(materialized_topic):
    return mt_rgx.match(materialized_topic) is not None


def construct_materialized_topic(source, destination):
    def is_valid_kafka_topic(topic):
        if topic == "":
            return False
        elif topic == "." or topic == "..":
            return False
        elif len(topic) > 249:
            return False
        elif topic_rgx.match(topic) is None:
            return False
        return True

    return (f"{source}._{destination}_" if is_valid_kafka_topic(source)
            and is_valid_kafka_topic(destination) else None)
