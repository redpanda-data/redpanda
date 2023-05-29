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


def read_rpk_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(RedpandaService.RPK_CONFIG_FILE, d)
        with open(os.path.join(d, 'rpk.yaml')) as f:
            return yaml.full_load(f.read())


def read_redpanda_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
        with open(os.path.join(d, 'redpanda.yaml')) as f:
            return yaml.full_load(f.read())
