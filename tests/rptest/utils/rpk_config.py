# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
import yaml

from rptest.services.redpanda import RedpandaService

# This lives in the runner
RUNNER_RPK_CONFIG_FILE = "/root/.config/rpk/rpk.yaml"


def clean_runner_rpk_cfg():
    if os.path.exists(RUNNER_RPK_CONFIG_FILE):
        os.remove(RUNNER_RPK_CONFIG_FILE)


def read_rpk_cfg():
    with open(RUNNER_RPK_CONFIG_FILE) as f:
        return yaml.full_load(f.read())


def read_node_rpk_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(RedpandaService.RPK_CONFIG_FILE, d)
        with open(os.path.join(d, 'rpk.yaml')) as f:
            return yaml.full_load(f.read())


def read_redpanda_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
        with open(os.path.join(d, 'redpanda.yaml')) as f:
            return yaml.full_load(f.read())
