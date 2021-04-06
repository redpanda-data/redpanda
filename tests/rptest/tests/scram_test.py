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
import time
from ducktape.mark import ignore
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from ducktape.tests.test import Test
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.python_librdkafka import PythonLibrdkafka


class ScramTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = dict(
            developer_mode=True,
            enable_sasl=True,
        )
        super(ScramTest, self).__init__(test_context,
                                        num_brokers=3,
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

    def list_users(self):
        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users"
        res = requests.get(url)
        assert res.status_code == 200
        return res.json()

    def create_user(self, username, algorithm):
        def gen(length):
            return "".join(
                random.choice(string.ascii_letters) for _ in range(length))

        password = gen(15)

        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users"
        data = dict(
            username=username,
            password=password,
            algorithm=algorithm,
        )
        res = requests.post(url, json=data)
        assert res.status_code == 200

        return password

    def make_superuser_client(self, password_override=None):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        password = password_override or password
        return PythonLibrdkafka(self.redpanda,
                                username=username,
                                password=password,
                                algorithm=algorithm)

    @cluster(num_nodes=3)
    def test_scram(self):
        topic = TopicSpec()

        client = self.make_superuser_client()
        client.create_topic(topic)

        try:
            # with incorrect password
            client = self.make_superuser_client("xxx")
            client.topics()
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but it works with correct password
        client = self.make_superuser_client()
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        # again!
        client = self.make_superuser_client()
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        username = self.redpanda.SUPERUSER_CREDENTIALS[0]
        self.delete_user(username)

        try:
            # now listing should fail because the user has been deleted. add
            # some delay to give user deletion time to propogate
            for _ in range(5):
                client = self.make_superuser_client()
                topics = client.topics()
                time.sleep(1)
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # recreate user
        algorithm = self.redpanda.SUPERUSER_CREDENTIALS[2]
        password = self.create_user(username, algorithm)

        # works ok again
        client = self.make_superuser_client(password_override=password)
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        # update password
        new_password = self.update_user(username)

        try:
            # now listing should fail because the password is different
            for _ in range(5):
                client = self.make_superuser_client(password_override=password)
                client.topics()
                time.sleep(1)
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but works ok with new password
        client = self.make_superuser_client(password_override=new_password)
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        users = self.list_users()
        assert username in users
