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

from ducktape.utils.util import wait_until
from ducktape.mark import parametrize


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

    def make_request(self, url, method='GET'):
        kwargs = {'allow_redirects': False}
        return self.admin._session.request(method=method, url=url, **kwargs)

    def check_redirect(self,
                       r,
                       leader,
                       num_redirects=1,
                       expect_retry_after=False,
                       subdomain=""):
        #Assert 307 response code
        assert r.status_code == 307, f"Unexpected code {r.status_code}"

        #Possibly expect Retry-After header.
        if expect_retry_after:
            assert 'Retry-After' in r.headers

        #Assert there is a redirect location.
        assert 'Location' in r.headers
        location_url = r.headers['Location']

        #Assert parsed net location is to the leader node, with port.
        parsed = urlparse(location_url)
        expected_net_loc = f'{subdomain}{self.redpanda.nodes[leader - 1].name}:{self.port}'
        assert parsed.netloc == expected_net_loc, \
            f"Enexpected location: {parsed.netloc} != {expected_net_loc}"

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

    @cluster(num_nodes=3)
    @parametrize(subdomain="broker.")
    @parametrize(subdomain="")
    def test_subdomain_redirect(self, subdomain):
        '''
        Tests code paths in the Admin API which perform loose matching on
        incoming requests host headers, redirecting to the hostname of a
        matching advertised kafka listener.

        Also verifies that original exact matching still works.
        '''
        path = f"security/users/cc-baxter"

        def make_new_address(node, port, sub=""):
            return dict(address=f"{sub}{node.name}", port=port)

        self.logger.debug(
            "Reconfigure advertised kafka API to some other DNS name & port.")
        # NOTE(oren): port choice is arbitrary, but _changing_ it makes debugging
        # the test a bit easier.
        base_port = 10091
        for i in range(len(self.redpanda.nodes)):
            node = self.redpanda.nodes[i]
            port = base_port + i
            self.redpanda.restart_nodes(
                [node],
                override_cfg_params=dict(
                    kafka_api=make_new_address(node, port),
                    advertised_kafka_api=make_new_address(
                        node, port, subdomain),
                ),
            )

        wait_until(lambda: self.redpanda.healthy(),
                   timeout_sec=30,
                   backoff_sec=2)

        leader = self.admin.await_stable_leader(topic="controller",
                                                partition=0,
                                                namespace="redpanda",
                                                timeout_s=30,
                                                backoff_s=1)

        self.logger.debug(
            "Redirects should point to the new advertised address")
        for node in self.redpanda.nodes:
            if self.redpanda.idx(node) == leader:
                continue
            self.logger.debug(
                f"leader: {leader}, dest: {self.redpanda.idx(node)}")
            url = self.admin._url(node, path)
            r = self.make_request(url, 'DELETE')
            self.check_redirect(r, leader, subdomain=subdomain)
