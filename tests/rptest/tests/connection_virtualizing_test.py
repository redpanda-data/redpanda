# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
import random
from types import MethodType
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from kafka.protocol.fetch import FetchRequest
from ducktape.mark import matrix

from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until
from kafka.protocol.admin import ApiVersionRequest
from kafka.structs import TopicPartition
from kafka.record.memory_records import MemoryRecordsBuilder

from kafka import KafkaClient, KafkaConsumer
from kafka.protocol.produce import ProduceRequest

from rptest.utils.xid_utils import random_xid_string


@dataclass
class PartitionInfo:
    leader_id: int
    end_offset: int


class MpxMockClient():
    '''
    Client hacking Kafka Python client internals to mimic the MPX behavior
    '''
    def __init__(self, redpanda):
        self.redpanda = redpanda
        self.initial_client_id = "__redpanda_mpx"
        self.client = KafkaClient(bootstrap_servers=self.redpanda.brokers(),
                                  client_id=self.initial_client_id)

    def _all_connections_ready(self):
        ready = all([
            self.client.ready(self.redpanda.node_id(n))
            for n in self.redpanda.nodes
        ])
        if not ready:
            self.client.poll()
        return ready

    def _api_versions_request(self):
        return ApiVersionRequest[1]()

    def _create_record_batch(self):
        # we do not need any special type of batch, using single record batch without compression
        b = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=1)
        b.append(timestamp=None,
                 key=b"test-key",
                 value=b"test-value",
                 headers=[])
        b.close()
        return b.buffer()

    def start(self):
        # wait for broker connections to be ready
        wait_until(
            lambda: self._all_connections_ready(),
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg="Timeout waiting for all brokers connection to be ready")
        # send api versions request to each broker to initialize connection, we expect
        # that the first request sent in the connection will have a special
        # client_id set indicating that MPX is connected to Redpanda
        for n in self.redpanda.nodes:
            id = self.redpanda.node_id(n)
            f = self.client.send(id, request=self._api_versions_request())
            self.client.poll(future=f)

    def set_client_id(self, client_id: str):
        # hack kafka python internals to override client id set in request header
        for conn in self.client._conns.values():
            conn._protocol._client_id = client_id

    def get_partition_info(self, topic: str, partition: int):
        c = KafkaConsumer(bootstrap_servers=self.redpanda.brokers())
        tp = TopicPartition(topic=topic, partition=partition)
        end_o = c.end_offsets([tp])
        leader = c._client.cluster.leader_for_partition(tp)

        return PartitionInfo(leader_id=leader, end_offset=end_o[tp])

    def create_produce_request(self, topic: str, partition: int):
        # basic produce request with one partition only
        return ProduceRequest[7](required_acks=-1,
                                 timeout=10,
                                 topics=[(topic, [
                                     (partition, self._create_record_batch())
                                 ])])

    def send(self, node_id: int, request):
        self.redpanda.logger.info(f"sending request: {request} to {node_id}")
        return self.client.send(node_id=node_id, request=request)

    def poll(self, future):
        return self.client.poll(future=future)

    def create_fetch_request(self, topic: str, partition: int,
                             fetch_offset: int):
        # a fetch request with wait time set to wait for the data before returning broker response
        return FetchRequest[7](replica_id=-1,
                               max_wait_time=10000,
                               min_bytes=10,
                               max_bytes=10 * 1024,
                               isolation_level=0,
                               session_id=-1,
                               session_epoch=-1,
                               topics=[(topic, [(partition, fetch_offset, 0,
                                                 10 * 1024)])],
                               forgotten_topics_data=[])

    def close(self):
        self.client.close()


import kafka.errors as Errors
from kafka.protocol.types import Int32


def no_validation_process_response(self, read_buffer):
    recv_correlation_id = Int32.decode(read_buffer)

    if not self.in_flight_requests:
        raise Errors.CorrelationIdError(
            'No in-flight-request found for server response'
            ' with correlation ID %d' % (recv_correlation_id, ))

    found_req = None
    idx = 0
    # special piece of logic allowing kafka-python protocol to tolerate unordered
    # responses, here we look for an inflight request that correlation id matches
    # the received response
    for i, tuple in enumerate(self.in_flight_requests):
        correlation_id, request = tuple
        if correlation_id == recv_correlation_id:
            idx = i
            found_req = request
            break

    del self.in_flight_requests[idx]

    # decode response
    try:
        response = found_req.RESPONSE_TYPE.decode(read_buffer)
    except ValueError:
        raise Errors.KafkaProtocolError('Unable to decode response')

    return (recv_correlation_id, response)


def create_client_id(vcluster_id: str, connection_id: int, client_id: str):
    return f"{vcluster_id}{connection_id:08x}{client_id}"


class TestVirtualConnections(RedpandaTest):
    def __init__(self, test_context):
        super(TestVirtualConnections, self).__init__(
            test_context=test_context,
            num_brokers=3,
            # disable leader balancer so that we do not have to retry requests sent during the test
            extra_rp_conf={"enable_leader_balancer": False})

    def _fetch_and_produce(self, client: MpxMockClient, topic: str,
                           partition: int, fetch_client_id: str,
                           produce_client_id: str):
        partition_info = client.get_partition_info(topic, partition)
        self.logger.info(
            f"partition {topic}/{partition} end offset: {partition_info.end_offset}, leader: {partition_info.leader_id}"
        )
        client.set_client_id(fetch_client_id)
        fetch_fut = client.send(node_id=partition_info.leader_id,
                                request=client.create_fetch_request(
                                    topic=topic,
                                    partition=partition,
                                    fetch_offset=partition_info.end_offset))

        client.set_client_id(produce_client_id)
        produce_fut = client.send(node_id=partition_info.leader_id,
                                  request=client.create_produce_request(
                                      topic=topic, partition=partition))

        return (fetch_fut, produce_fut)

    @cluster(num_nodes=3)
    @matrix(different_clusters=[True, False],
            different_connections=[True, False])
    def test_no_head_of_line_blocking(self, different_clusters,
                                      different_connections):

        # create topic with single partition
        spec = TopicSpec(partition_count=1, replication_factor=3)
        self.client().create_topic(spec)

        mpx_client = MpxMockClient(self.redpanda)
        mpx_client.start()
        v_cluster_1 = random_xid_string()
        v_cluster_2 = random_xid_string()

        fetch_client = create_client_id(v_cluster_1, 0, "client-fetch")
        produce_client = create_client_id(
            v_cluster_1 if not different_clusters else v_cluster_2,
            0 if not different_connections else 1, "client-produce")
        # validate that fetch request is blocking produce request first as mpx extensions are disabled
        (fetch_fut, produce_fut) = self._fetch_and_produce(
            client=mpx_client,
            topic=spec.name,
            partition=0,
            fetch_client_id=fetch_client,
            produce_client_id=produce_client)

        mpx_client.poll(produce_fut)
        assert produce_fut.is_done and produce_fut.succeeded
        # fetch was sent before produce so having produce future
        # ready implies that the fetch one is ready as well
        assert fetch_fut.is_done
        f_resp = fetch_fut.value
        # fetch response contains only one topic and partition,
        # check if response is empty, produce was executed after the fetch returned.

        assert f_resp.topics[0][1][0][
            6] == b'', "Fetch should be executed before the produce finishes"

        mpx_client.close()

        # enable MPX extensions
        self.redpanda.set_cluster_config({"enable_mpx_extensions": True})

        # reinitialize the client
        mpx_client = MpxMockClient(self.redpanda)
        mpx_client.start()
        partition_info = mpx_client.get_partition_info(spec.name, 0)

        # execute fetch and produce once again, now the fetch should not block the produce request as it will be processed in a virtual connection
        (fetch_fut, produce_fut) = self._fetch_and_produce(
            client=mpx_client,
            topic=spec.name,
            partition=0,
            fetch_client_id=fetch_client,
            produce_client_id=produce_client)

        for connection in mpx_client.client._conns.values():
            if len(connection._protocol.in_flight_requests) == 2:
                # replace process response method of the protocol
                connection._protocol._process_response = MethodType(
                    no_validation_process_response, connection._protocol)

        # wait for fetch as it will be released after produce finishes
        should_interleave_requests = different_clusters or different_connections

        def _produce_is_ready():
            mpx_client.poll(
                fetch_fut if should_interleave_requests else produce_fut)
            return produce_fut.is_done

        wait_until(
            _produce_is_ready,
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg=
            "Produce should allow fetch to finish so at this point it should be ready"
        )

        fetch_fut.add_callback(
            lambda e: self.logger.info(f"fetch success: {e}"))
        fetch_fut.add_errback(lambda e: self.logger.info(f"fetch error: {e}"))

        f_resp = fetch_fut.value

        if should_interleave_requests:
            assert f_resp.topics[0][1][0][
                6] != b'', "Fetch should be unblocked by produce from another virtual connection"
        else:
            assert f_resp.topics[0][1][0][
                6] == b'', "Fetch should be executed before the produce finishes"
        mpx_client.close()

    @cluster(num_nodes=3)
    def test_handling_invalid_ids(self):
        self.redpanda.set_cluster_config({"enable_mpx_extensions": True})
        # create topic with single partition
        spec = TopicSpec(partition_count=1, replication_factor=3)
        topic = spec.name
        self.client().create_topic(spec)

        def produce_with_client(client_id: str):
            mpx_client = MpxMockClient(self.redpanda)
            mpx_client.start()
            partition_info = mpx_client.get_partition_info(topic, 0)
            mpx_client.set_client_id(client_id)
            produce_fut = mpx_client.send(
                node_id=partition_info.leader_id,
                request=mpx_client.create_produce_request(topic=topic,
                                                          partition=0))

            mpx_client.poll(produce_fut)
            assert produce_fut.is_done and produce_fut.succeeded
            pi = mpx_client.get_partition_info(topic, 0)
            mpx_client.close()
            return pi

        v_cluster = random_xid_string()
        valid_client_id = create_client_id(v_cluster, 0, "client-fetch")
        p_info = produce_with_client(valid_client_id)

        assert p_info.end_offset > 0, "Produce request should be successful"
        starting_end_offset = p_info.end_offset
        invalid_xid_id = create_client_id("zzzzzzzzzzzzzzzzzzzz", 0,
                                          "client-fetch")

        p_info = produce_with_client(invalid_xid_id)

        assert starting_end_offset == p_info.end_offset, "Produce request with invalid client id should fail"

        invalid_connection_id = f"{v_cluster}00blob00client"

        p_info = produce_with_client(invalid_connection_id)

        assert starting_end_offset == p_info.end_offset, "Produce request with invalid client id should fail"

        valid_client_id_empty = create_client_id(v_cluster, 0, "")
        p_info = produce_with_client(valid_client_id_empty)

        assert starting_end_offset < p_info.end_offset, "Produce request with valid client id should succeed "
