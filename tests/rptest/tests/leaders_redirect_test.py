#Copyright 2022 Redpanda Data, Inc.
#
#Use of this software is governed by the Business Source License
#included in the file licenses / BSL.md
#
#As of the Change Date specified in that file, in accordance with
#the Business Source License, use of this software will be governed
#by the Apache License, Version 2.0

from urllib.parse import urlparse
from urllib.parse import parse_qs

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest


class LeadersRedirectTest(RedpandaTest):
    """
    Asserts that Location and Retry-After headers are being correctly appended
    to requests made to non-leaders upon redirect.
    """
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(LeadersRedirectTest, self).__init__(test_context=test_context,
                                                  num_brokers=3)

        self.port = 9644
        self.admin = Admin(self.redpanda)

    def make_request(self, url):
        kwargs = {'allow_redirects': False}
        return self.admin._session.request(method='GET', url=url, **kwargs)

    def check_redirect(self,
                       r,
                       leader,
                       num_redirects=1,
                       expect_retry_after=False):
        #Assert 307 response code
        assert r.status_code == 307

        #Possibly expect Retry-After header.
        if expect_retry_after:
            assert 'Retry-After' in r.headers

        #Assert there is a redirect location.
        assert 'Location' in r.headers
        location_url = r.headers['Location']

        #Assert parsed net location is to the leader node, with port.
        parsed = urlparse(location_url)
        expected_net_loc = f'{self.redpanda.nodes[leader - 1].name}:{self.port}'
        assert parsed.netloc == expected_net_loc

        #Assert the number of redirects is what is expected.
        parsed_query_params = parse_qs(parsed.query)
        assert 'redirect' in parsed_query_params
        assert parsed_query_params['redirect'][0] == f"{num_redirects}"

    @cluster(num_nodes=3)
    def redirected_request_test(self):
        topic = self.topics[0]
        partition = 0
        path = f"cloud_storage/manifest/{topic}/{partition}"
        for node in self.redpanda.nodes:
            leader = self.admin.get_partition_leader(namespace="kafka",
                                                     topic=topic,
                                                     partition=partition)
            if self.redpanda.idx(node) != leader:
                url = self.admin._url(node, path)
                r = self.make_request(url)
                self.check_redirect(r,
                                    leader,
                                    num_redirects=1,
                                    expect_retry_after=False)

                #Attempt to make the request again- but to the original non-leader node, to get multiple redirects.
                location_url = r.headers['Location']
                parsed = urlparse(location_url)
                retry_url = parsed._replace(
                    netloc=f'{node.name}:{self.port}').geturl()
                r = self.make_request(retry_url)
                self.check_redirect(r,
                                    leader,
                                    num_redirects=2,
                                    expect_retry_after=True)
