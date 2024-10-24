# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

BASE32_ALPHABET = "0123456789abcdefghijklmnopqrstuv"


def random_xid_string():
    xid = ''.join([random.choice(BASE32_ALPHABET) for _ in range(19)])

    return xid + random.choice(['0', 'g'])
