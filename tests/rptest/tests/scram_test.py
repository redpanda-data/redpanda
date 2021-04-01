# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import string
import requests
from ducktape.mark import ignore
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from ducktape.tests.test import Test
from rptest.services.redpanda import NoSaslRedpandaService
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


# test that starts a redpanda service without using clients that would otherwise
# require sasl auth support
class NoSaslRedpandaTest(Test):
    def __init__(self,
                 test_context,
                 num_brokers=3,
                 extra_rp_conf=dict(),
                 log_level='info'):
        super(NoSaslRedpandaTest, self).__init__(test_context)

        self.redpanda = NoSaslRedpandaService(test_context,
                                              num_brokers,
                                              KafkaCliTools,
                                              extra_rp_conf=extra_rp_conf,
                                              log_level=log_level)

    def setUp(self):
        self.redpanda.start()


class ScramTest(NoSaslRedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = dict(
            developer_mode=True,
            enable_sasl=True,
        )
        super(ScramTest, self).__init__(test_context=test_context,
                                        extra_rp_conf=extra_rp_conf)

    def update_user(self, username):
        def gen(length):
            return "".join(
                random.choice(string.ascii_letters) for _ in range(length))

        password = gen(20)

        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users/{username}"
        data = dict(
            username=username,
            password=password,
            algorithm="SCRAM-SHA-256",
        )
        res = requests.put(url, json=data)

        assert res.status_code == 200
        return password

    def delete_user(self, username):
        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users/{username}"
        res = requests.delete(url)
        assert res.status_code == 200

    def create_user(self):
        def gen(length):
            return "".join(
                random.choice(string.ascii_letters) for _ in range(length))

        username = gen(15)
        password = gen(15)

        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users"
        data = dict(
            username=username,
            password=password,
            algorithm="SCRAM-SHA-256",
        )
        res = requests.post(url, json=data)
        assert res.status_code == 200

        return username, password

    @cluster(num_nodes=3)
    # TODO: this test is ignored until we will have support for creating propert
    #       set of ACLs
    @ignore
    def test_scram(self):
        username, password = self.create_user()

        topic = TopicSpec()

        # create topic with correct username/password
        client = KafkaCliTools(self.redpanda, user=username, passwd=password)
        client.create_topic(topic)

        try:
            # with incorrect password
            client = KafkaCliTools(self.redpanda, user=username, passwd="xxx")
            client.list_topics()
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but it works with correct password
        client = KafkaCliTools(self.redpanda, user=username, passwd=password)
        topics = client.list_topics()
        print(topics)
        assert topic.name in topics

        # still works
        client = KafkaCliTools(self.redpanda, user=username, passwd=password)
        topics = client.list_topics()
        print(topics)
        assert topic.name in topics

        self.delete_user(username)

        try:
            # now listing should fail because the user has been deleted
            client = KafkaCliTools(self.redpanda,
                                   user=username,
                                   passwd=password)
            client.list_topics()
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # recreate user
        username, password = self.create_user()

        # works ok
        client = KafkaCliTools(self.redpanda, user=username, passwd=password)
        topics = client.list_topics()
        print(topics)
        assert topic.name in topics

        # update password
        new_password = self.update_user(username)

        try:
            # now listing should fail because the password is different
            client = KafkaCliTools(self.redpanda,
                                   user=username,
                                   passwd=password)
            client.list_topics()
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but works ok with new password
        client = KafkaCliTools(self.redpanda,
                               user=username,
                               passwd=new_password)
        topics = client.list_topics()
        print(topics)
        assert topic.name in topics
