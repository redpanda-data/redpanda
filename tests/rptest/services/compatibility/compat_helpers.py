# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .sarama_helpers import SaramaHelperFactory
from .franzgo_helpers import FranzGoHelperFactory


# A factory method to create the necessary
# kafka client factory
def create_helper(func_name, redpanda, topic, extra_conf):
    if "sarama" in func_name:
        factory = SaramaHelperFactory(func_name, redpanda, topic)
        return factory.create_sarama_helpers()
    elif "franzgo" in func_name:
        factory = FranzGoHelperFactory(func_name, redpanda, topic, extra_conf)
        return factory.create_franzgo_helpers()
    else:
        raise RuntimeError("create_helper failed: Invalid function name")
