# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import os
import re
import sys
import time
import traceback
from collections import namedtuple, defaultdict
from typing import DefaultDict, List, Optional

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, SISettings, CloudStorageTypeAndUrlStyle, get_cloud_storage_type, get_cloud_storage_type_and_url_style
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.node_operations import verify_offset_translator_state_consistent


class OffsetTranslatorConsistencyTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(OffsetTranslatorConsistencyTest, self).__init__(
            test_ctx,
            si_settings=SISettings(test_ctx,
                                   log_segment_size=1024 * 1024,
                                   fast_uploads=True),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=3)
    def test_offset_translator_state_consistent(self):
        cli = KafkaCliTools(self.redpanda)
        topic = TopicSpec(partition_count=3, replication_factor=3)

        cli.create_topic(topic)
        cli.produce(topic.name, 1000, 100)
        verify_offset_translator_state_consistent(self.redpanda)
