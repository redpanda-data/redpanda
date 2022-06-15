# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from rptest.services.cluster import cluster

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest

usable_api_re = re.compile('(.*)\((\d*)\): (\d*) to (\d*) \[usable: (\d*)\]')
unusable_api_re = re.compile('(.*)\((\d*)\): UNSUPPORTED')


def parse_api_versions_response(line):
    """
    Parses the output of running BrokerApiVersionsCommand
    Eg: 'ApiVersions(18): 0 to 3 [usable: 3]'
    """
    match = usable_api_re.match(line)
    if match is None:
        match = unusable_api_re.match(line)
        if match is None:
            return None
    return match.groups()


class ApiVersionResponseParser:
    def __init__(self, match_grps):
        assert len(match_grps) == 2 or len(match_grps) == 5
        self.api_name = match_grps[0]
        self.api_key = int(match_grps[1])
        self.is_unsupported = True
        if len(match_grps) == 5:
            self.is_unsupported = False
            self.min_supported = int(match_grps[2])
            self.max_supported = int(match_grps[3])
            self.version_used = int(match_grps[4])

    def __repr__(self):
        if self.is_unsupported:
            return str((self.api_name, self.api_key))
        return str((self.api_name, self.api_key, self.min_supported,
                    self.max_supported, self.version_used))


class FlexibleCompatTest(RedpandaTest):
    def __init__(self, test_context):
        super(FlexibleCompatTest, self).__init__(test_context=test_context)
        self._client = KafkaCliTools(self.redpanda)

    def _query_api_versions(self):
        output = self._client.get_api_versions()
        self.redpanda.logger.info(output)

        # sanitize output
        def trim_and_parse(x):
            line = parse_api_versions_response(x.removesuffix(',').strip())
            return None if line is None else ApiVersionResponseParser(line)

        output = [trim_and_parse(x) for x in output.split('\n')]
        return [x for x in output if x is not None]

    @cluster(num_nodes=3)
    def test_api_versions_flexible(self):
        parsed = self._query_api_versions()
        results = [x for x in parsed if x.api_key == 18]

        # 3 because theres one response for each node in cluster
        if len(results) != 3:
            raise Exception(f'ApiVersionsRequest failed: {parsed}')

        for r in results:
            assert r.is_unsupported is False and r.min_supported == 0 \
                and r.max_supported >= 3 and r.version_used >= 3
