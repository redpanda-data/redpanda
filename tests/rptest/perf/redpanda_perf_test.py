# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from rptest.tests.redpanda_test import RedpandaTest


class RedpandaPerfTest(RedpandaTest):
    def __init__(self, *args: Any, **kwargs: Any):
        extra_rp_conf: dict[str, Any] = kwargs.get("extra_rp_conf") or {}
        kwargs["extra_rp_conf"] = extra_rp_conf
        if "leader_balancer_mode" not in extra_rp_conf:
            extra_rp_conf["leader_balancer_mode"] = "greedy"
        super().__init__(*args, **kwargs)
