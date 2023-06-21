# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import os
import typing

import requests
import threading

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from enum import Enum
from typing import Optional, List

TESTS_DIR = os.path.join("/usr", "local", "bin")

REMOTE_PORT_BASE = 8080


class DeleteRecordPosition(str, Enum):
    """
    Relative position to delete records
    """
    All = "all"
    """
    Delete all records
    """
    Halfway = "halfway"
    """
    Delete all records before the halfway point between the LWM and HWM
    """
    Single = "single"
    """
    Delete a single record after the LWM
    """


class CompressionType(str, Enum):
    NoCompression = "none"
    GZip = "gzip"
    LZ4 = "lz4"
    Snappy = "snappy"
    ZStd = "zstd"


class DeleteSpecificRecords:
    """Used to specify, and then json-ify specific records to delete"""
    class DeleteSpecificRecordsEncoder(json.JSONEncoder):
        """Used by json to encode the contents of DeleteSpecificRecords"""
        def default(self, o):
            res = {'records': []}
            for k, v in o.offsets.items():
                res['records'].append({'partition': k, 'offset': v})
            return res

    def __init__(self, offsets: typing.Dict[int, int]):
        """
        Parameters
        ----------

        offsets: Dict[int, int]
            The [partition, offset] to delete
        """
        self._offsets = offsets

    @property
    def offsets(self):
        return self._offsets

    def to_json(self):
        """JSON-ify the data"""
        return json.dumps(
            self, cls=DeleteSpecificRecords.DeleteSpecificRecordsEncoder)


class AutoDeleteRecordsSettings:
    """Settings used to setup automatic delete records"""
    def __init__(self, delete_record_position: DeleteRecordPosition,
                 auto_delete_interval_sec: int):
        """
        Parameters
        ----------
        delete_record_position: DeleteRecordPosition
            The relative position to delete at an interval

        auto_delete_interval_sec: int
            The interval, in seconds, to delete records, automatically
        """
        self.delete_record_position = delete_record_position
        self.auto_delete_interval_sec = auto_delete_interval_sec


class DeleteRecordsService(Service):
    """This class execute the delete-records-test"""
    def __init__(
            self,
            context,
            redpanda,
            custom_node,
            topic: str,
            api_port: int = REMOTE_PORT_BASE,
            compression: Optional[CompressionType] = None,
            min_record_size: Optional[int] = None,
            max_record_size: Optional[int] = None,
            keys: Optional[int] = None,
            auto_delete_records: Optional[AutoDeleteRecordsSettings] = None,
            num_producers: Optional[int] = None,
            produce_throughput_bps: Optional[int] = None,
            produce_timeout_ms: Optional[int] = None,
            producer_properties: List[str] = None,
            num_consumers: Optional[int] = None,
            consumer_throughput_mbps: Optional[int] = None,
            consumer_properties: List[str] = None,
            rand: bool = False,
            group_name: Optional[str] = None,
            debug_logs: bool = False,
            trace_logs: bool = False):
        """
        Parameters
        ----------
        context
            The test context
        redpanda
            The Redpanda service
        custom_node : optional
            Node to use or if none provide, allocate one
        topic: str
            Name of the topic to use
        api_port: int
            The TCP port to host the API
        compression: CompressionType, optional
            If provided, generate compressible payload and compress with
            the specified compression algorithm
        min_record_size: int, optional
            Minimum record size, if not provided will use maximum
            record size
        max_record_size: int, optional
            Maximum record size
        keys: int, optional
            The number of unique keys to use
        auto_delete_records: AutoDeleteRecordsSettings, optional
            If provided, the test will automatically delete records
            at an interval provided, in seconds, at the relative
            position specified
        num_producers: int, optional
            Number of producers to use, defaults to 1
        produce_throughput_bps: int, optional
            If provided, limit produce throughput by this in Bps
        producer_properties: List[str], optional
            Additional properties for the producer, in the format of key=value
        num_consumers: int, optional
            Number of consumers to use, defaults to 1
        consumer_throughput_mbps: int, optional
            Limit consumer throughput to MBps
        consumer_properties: List[str], optional
            Additional properties for the consumers in the format of key=value
        rand: bool
            If true, consume randomly
            TODO: Make this work
            NOTE: this option is not currently honored
        group_name: str, optional
            The name of the consumer group to use
        debug_logs: bool
            If true, log at debug level
        trace_logs: bool
            If true, log at trace level.  Overrides debug_logs
        """
        if consumer_properties is None:
            consumer_properties = []
        self.use_custom_node = custom_node is not None

        # We should pass num_nodes to allocate for our service in BackgroundThreadService,
        # but if user allocate node by themself, BackgroundThreadService should not allocate any nodes
        nodes_for_allocate = 1
        if self.use_custom_node:
            nodes_for_allocate = 0

        super(DeleteRecordsService,
              self).__init__(context, num_nodes=nodes_for_allocate)

        if self.use_custom_node:
            assert not self.nodes
            self.nodes = custom_node

        self._redpanda = redpanda
        self._topic = topic
        self._api_port = api_port
        self._compression: Optional[CompressionType] = compression
        self._min_record_size: Optional[int] = min_record_size
        self._max_record_size: Optional[int] = max_record_size
        self._keys: Optional[int] = keys
        self._auto_delete_records: Optional[
            AutoDeleteRecordsSettings] = auto_delete_records
        self._num_producers: Optional[int] = num_producers
        self._produce_throughput_bps: Optional[int] = produce_throughput_bps
        self._produce_timeout_ms: Optional[int] = produce_timeout_ms
        self._producer_properties: List[str] = producer_properties if producer_properties is not None else []
        self._num_consumers: Optional[int] = num_consumers
        self._consumer_throughput_mbps: Optional[
            int] = consumer_throughput_mbps
        self._consumer_properties: List[str] = consumer_properties if consumer_properties is not None else []
        self._rand = rand
        self._group_name: Optional[str] = group_name
        self._logging_level = "INFO"
        if trace_logs:
            self._logging_level = "TRACE"
        elif debug_logs:
            self._logging_level = "DEBUG"

        self._pid = None
        self._status_thread = None
        self._status = DeleteRecordsStatus()

    def allocate_nodes(self):
        """
        Allocates a new node for the delete records service
        """
        if self.use_custom_node:
            return
        else:
            return super(DeleteRecordsService, self).allocate_nodes()

    def clean_node(self, node, **kwargs):
        """
        Cleans up the node
        """
        self.logger.info(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process("delete-records-test", clean_shutdown=False)
        node.account.remove(f"/tmp/{self.__class__.__name__}*")

    def free(self):
        """
        Frees allocated node
        """
        if self.use_custom_node:
            return
        else:
            return super(DeleteRecordsService, self).free()

    @property
    def log_path(self):
        return f"/tmp/{self.who_am_i()}.log"

    @property
    def logs(self):
        """Tells Ducktape where to grab logs from"""
        return {
            "delete_records_service_output": {
                "path": self.log_path,
                "collect_default": True
            }
        }

    def stop_node(self, node, **kwargs):
        """
        Halts execution of the service by sending a `DELETE` to the /service endpoint
        """
        if self._status_thread:
            self._status_thread.stop()
            self._status_thread = None

        if self._pid is None:
            return

        self.logger.info(f"{self.__class__.__name__}.stop")
        self.logger.debug(f"Killing pid {self._pid}")
        try:
            node.account.signal(self._pid, 2, allow_fail=False)
        except RemoteCommandError as e:
            if b"No such process" not in e.msg:
                raise

    def _remote_url(self, node, path):
        """
        Generates a new remote url

        Parameters
        ----------
        node
            The node to communicate with
        path: str
            The path to query
        """
        assert self._api_port is not None
        return f"http://{node.account.hostname}:{self._api_port}/{path}"

    def start_node(self, node, clean=False, **kwargs):
        """
        Starts the execution of the test
        """
        if clean:
            self.clean_node(node)

        cmd = f"{TESTS_DIR}/delete-records-test --brokers {self._redpanda.brokers()} --topic {self._topic} --port {self._api_port}"

        if self._compression is not None:
            cmd += f" --compressible-payload --compression-type {self._compression}"

        if self._min_record_size is not None:
            cmd += f" --min-record-size {self._min_record_size}"

        if self._max_record_size is not None:
            cmd += f" --max-record-size {self._max_record_size}"

        if self._keys is not None:
            cmd += f" --keys {self._keys}"

        if self._auto_delete_records is not None:
            cmd += f" --delete-record-position {self._auto_delete_records.delete_record_position} --delete-record-period-sec {self._auto_delete_records.auto_delete_interval_sec}"

        if self._num_producers is not None:
            cmd += f" --num-producers {self._num_producers}"

        if self._produce_throughput_bps is not None:
            cmd += f" --produce-throughput-bps {self._produce_throughput_bps}"

        if self._produce_timeout_ms is not None:
            cmd += f" --timeout-ms {self._produce_timeout_ms}"

        for producer_property in self._producer_properties:
            cmd += f" --producer-properties {producer_property}"

        if self._num_consumers is not None:
            cmd += f" --num-consumers {self._num_producers}"

        if self._consumer_throughput_mbps is not None:
            cmd += f" --consumer-throughput-mbps {self._consumer_throughput_mbps}"

        for consumer_property in self._consumer_properties:
            cmd += f" --consumer-properties {consumer_property}"

        if self._rand:
            cmd += f" --rand"

        if self._group_name is not None:
            cmd += f" --group-name {self._group_name}"

        wrapped_cmd = f"RUST_LOG={self._logging_level} nohup {cmd} >> {self.log_path} 2>&1 & echo $!"
        self.logger.debug(f"spawn {self.who_am_i()}: {wrapped_cmd}")
        pid_str = node.account.ssh_output(wrapped_cmd, timeout_sec=10)
        self.logger.debug(
            f"spawned {self.who_am_i()} node={node.name} pid={pid_str}")
        pid = int(pid_str.strip())
        self._pid = pid

        self._status_thread = DeleteRecordsStatusThread(self, node)
        self._status_thread.start()

    @property
    def status(self):
        return self._status

    def delete_records_relative(self, position: DeleteRecordPosition):
        """
        Attempts to delete all records at a relative position

        Parameters
        ----------

        position: DeleteRecordPosition
            The position to delete
        """
        self.logger.debug(
            f'Attempting to perform delete records at relative position {position}'
        )
        r = requests.delete(self._remote_url(self.nodes[0], "record"),
                            params={'position': f'{position}'},
                            timeout=5)
        self.logger.debug(f'Sent delete to {r.url}')
        r.raise_for_status()

    def delete_records_specific(self, records: DeleteSpecificRecords):
        """
        Attempts to delete specific offsets at specific partitions

        Parameters
        ----------

        records: DeleteSpecificRecords
            The records to delete
        """
        payload = records.to_json()
        self.logger.debug(f'Attempting to delete specific records: {payload}')
        r = requests.delete(self._remote_url(self.nodes[0], "record"),
                            params={'position': 'specific'},
                            timeout=5,
                            data=payload,
                            headers={'Content-Type': 'application/json'})
        self.logger.debug(f'Sent delete to {r.url}')
        r.raise_for_status()


class DeleteRecordsStatus:
    """Decoded from the status returned from the HTTP API endpoint"""
    def __init__(self,
                 updated_at=None,
                 tp_info=None,
                 errors=None,
                 last_removed_offsets=None):
        self.updated_at = updated_at
        if tp_info is None:
            tp_info = {}
        self.tp_info = tp_info
        if errors is None:
            errors = []
        self.errors = errors
        if last_removed_offsets is None:
            last_removed_offsets = {}
        self.last_removed_offsets = last_removed_offsets

    def __str__(self):
        return f'DeleteRecordsStatus<{self.updated_at} {self.tp_info} {self.errors} {self.last_removed_offsets}'


class DeleteRecordsStatusThread(threading.Thread):
    """Status thread class used to query the status of the delete records app"""
    def __init__(self,
                 parent: Service,
                 node,
                 poll_interval_sec: float = 5.0,
                 *args,
                 **kwargs):
        """
        Parameters
        ----------
        parent: Service
            The parent service utilizing this thread class
        node:
            The node being executed on
        poll_interval_sec: float, default=5
            How often to poll the status
        """
        super().__init__(*args, **kwargs)
        self.daemon = True

        self._parent = parent
        self._node = node
        self._poll_interval_sec = poll_interval_sec
        self._ex = None
        self._shutdown_requested = threading.Event()
        self._stop_requested = threading.Event()
        self._ready = False

    def await_ready(self):
        if self._ready:
            return

        wait_until(
            lambda: self._is_ready() or self._stop_requested.is_set(),
            timeout_sec=5,
            backoff_sec=0.5,
            err_msg=
            f'Timed out waiting for status endpoint {self.who_am_i} to be available'
        )
        self._ready = True

    @property
    def errored(self):
        return self._ex is not None

    def _is_ready(self):
        try:
            r = requests.get(self._parent._remote_url(self._node, "status"),
                             timeout=10)
        except Exception as e:
            self.logger.debug(
                f'Status endpoint {self.who_am_i} not ready: {e}')
            return False
        else:
            return r.status_code == 200

    def join_with_timeout(self):
        """
        Join the thread with a modest timeout and raise an exception if
        we are not successful.
        """
        self.join(timeout=10)
        if self.is_alive():
            msg = f"Failed to join thread for {self.who_am_i}"
            self.logger.error(msg)
            raise RuntimeError(msg)

    @property
    def logger(self):
        return self._parent.logger

    def poll_status(self):
        while not self._stop_requested.is_set():
            drop_out = self._shutdown_requested.is_set()

            url = self._parent._remote_url(self._node, "status")
            self.logger.debug(f'Requesting status at endpoint {url}')

            r = requests.get(url, timeout=5)

            r.raise_for_status()
            worker_status = r.json()
            self._parent._status = DeleteRecordsStatus(**worker_status)
            self.logger.debug(f"worker_status: {worker_status}")

            if drop_out:
                return
            else:
                self._stop_requested.wait(self._poll_interval_sec)

    def raise_on_error(self):
        if self._ex is not None:
            raise self._ex

    def run(self):
        try:
            self.await_ready()
            self.poll_status()
        except Exception as ex:
            self._ex = ex
            self.logger.exception(
                f"Error reading status from {self.who_am_i} on {self._node.name}"
            )

    def stop(self):
        """
        Drop out of poll loop as soon as possible
        """
        self._stop_requested.set()
        self.join_with_timeout()

    def shutdown(self):
        """
        Read status one more time, then drop out of poll loop and join
        """
        self._shutdown_requested.set()
        self.join_with_timeout()

    @property
    def who_am_i(self):
        return self._parent.who_am_i()
