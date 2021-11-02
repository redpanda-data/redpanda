# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .sarama_examples import SaramaFactory
from .franzgo_examples import FranzGoFactory


# A factory method to create the necessary
# kafka client factory
def create_example(context, redpanda, topic, extra_conf):
    if "sarama" in context.function_name:
        factory = SaramaFactory(context, redpanda, topic, extra_conf)
        return factory.create_sarama_examples()
    elif "franzgo" in context.function_name:
        factory = FranzGoFactory(context, redpanda, topic, extra_conf)
        return factory.create_franzgo_examples()
    else:
        raise RuntimeError("create_example failed: Invalid function name")
