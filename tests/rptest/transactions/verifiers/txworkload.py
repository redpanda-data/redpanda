#!/usr/bin/env python3

# make sure to `pip install flask confluent_kafka`

import os
import random
import sys
import time
import uuid

from enum import Enum, auto
from threading import Thread, RLock, local as tlocal
from typing import Optional
from concurrent.futures import Future, ThreadPoolExecutor

from confluent_kafka import Producer, Consumer, TopicPartition
from flask import Flask, abort, jsonify, request


class State(Enum):
    INITIAL = auto()
    PRODUCING = auto()
    DRAINING = auto()
    DRAINED = auto()
    CONSUMING = auto()
    FINAL = auto()


class WriteInfo:
    id: int
    key: str
    partition: int
    offset: int

    def __init__(self, id, key, partition, offset):
        self.id = id
        self.key = key
        self.partition = partition
        self.offset = offset


class Value:
    id: int
    offset: int

    def __init__(self, id, offset):
        self.id = id
        self.offset = offset


class LatestValue:
    confirmed: Optional[WriteInfo]
    attempts: list[WriteInfo]

    def __init__(self):
        self.confirmed = None
        self.attempts = []


class GatedWorkload:
    LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    _count: int = 0
    _state: State = State.INITIAL

    _started: float
    _write_threads: list[Future[None]]
    _read_threads: list[Future[None]]
    _locks: dict[str, RLock] = {
        'count': RLock(),
        'start_producer': RLock(),
        'stop_producer': RLock(),
        'wait_producer': RLock(),
        'start_consumer': RLock(),
        'wait_consumer': RLock(),
        'write_process': RLock(),
        'read_process': RLock()
    }

    def random_string(self, length: int):
        return ''.join(random.choice(self.LETTERS) for _ in range(length))

    def log(self, partition: int, msg: str):
        ts = (time.time() - self._started) / 1000
        print(f'{self._count}\t{ts}\t{partition}\t{msg}\n')
        with self._locks['count']:
            self._count += 1

    def violation(self, partition: int, msg: str):
        self.log(partition, f'violation\t{msg}')
        sys.exit(1)

    def get_metrics(self):
        raise NotImplementedError

    def start_producer(self):
        with self._locks['start_producer']:
            if self._state != State.INITIAL:
                self.violation(
                    -1, 'producer can be started only from the initial state')
            self._state = State.PRODUCING
        self._started = time.time()

    def stop_producer(self):
        with self._locks['stop_producer']:
            if self._state != State.PRODUCING:
                self.violation(
                    -1,
                    'producer can be stopped only from the producing state')
            self._state = State.DRAINING

    def wait_producer(self):
        with self._locks['wait_producer']:
            match self._state:
                case State.DRAINED:
                    return
                case State.DRAINING:
                    [f.result() for f in self._write_threads]
                    self.log(-1, 'writers stopped')
                    self._state = State.DRAINED
                case _:
                    self.violation(-1, "producer isn't active")

    def start_consumer(self):
        with self._locks['start_consumer']:
            if self._state != State.DRAINED:
                self.violation(
                    -1, 'consumer can be started only from the drained state')
            self._state = State.CONSUMING

    def wait_consumer(self):
        with self._locks['wait_consumer']:
            match self._state:
                case State.FINAL:
                    return
                case State.CONSUMING:
                    [f.result() for f in self._read_threads]
                    self.log(-1, 'readers stopped')
                    self._state = State.FINAL
                case _:
                    self.violation(-1,
                                   "can't wait consumer before starting it")


class TxWorkload(GatedWorkload):
    def __init__(self, args):
        super().__init__()
        self.args = {
            'threads': 4,
            'batch_size': 10
        } | args
        self.partitions = {}
        self.last_offset = -1

    def init_partitions(self):
        for pid in range(self.args['partitions']):
            partition = TopicPartition(f'topic-{pid}', pid)
            self.partitions[pid] = partition

    def start_producer(self):
        super().start_producer()
        print(
            '#logical_time\tseconds_since_start\tpartition\tlog_type\tdetails\n'
        )
        self.log(-1, 'start\tproducer')
        self._write_threads = []
        self.init_partitions()

        def try_write_process(id):
            try:
                self.write_process(id)
            except:
                with self._locks['start_producer']:
                    print('=== write process error\n')
                    print(repr(sys.exception()))
                    os._exit(1)

        with ThreadPoolExecutor(max_workers=self.args['threads']) as exe:
            for partition in range(self.args['partitions']):
                self._write_threads.append(
                    exe.submit(try_write_process, partition))

    def start_consumer(self):
        super().start_consumer()
        self._read_threads = []

        def try_read_process(id):
            try:
                self.read_process(id)
            except:
                with self._locks['start_consumer']:
                    print('=== read process error\n')
                    print(repr(sys.exception()))
                    os._exit(1)

        with ThreadPoolExecutor(max_workers=self.args['threads']) as exe:
            for partition in range(self.args['partitions']):
                self._read_threads.append(
                    exe.submit(try_read_process, partition))

    def write_process(self, pid):
        # Create a dictionary to hold the producer configuration
        config = tlocal()
        config.__dict__.update({
            'bootstrap.servers': self.args['brokers'],
            'enable.idempotence': True,
            'linger.ms': 0,
            'transactional.id': str(uuid.uuid4())
        })

        batch_size = self.args['batch_size']
        abort_probability = self.args['abort_probability']

        id = 0
        last_offset = -1
        should_reset = True
        producer: Optional[Producer] = None

        while self._state == State.PRODUCING:
            if should_reset:
                if producer is not None:
                    producer.close()
                producer = None
                should_reset = False
                time.sleep(1.0)

            if producer is None:
                # Create a Kafka producer
                producer = Producer(config.__dict__)
                try:
                    self.log(pid, 'init')
                    producer.init_transactions()
                except:
                    with self._locks['write_process']:
                        self.log(pid, 'err\tinit')
                        print('=== Error on initTransactions\n')
                        print(repr(sys.exception()))
                    should_reset = True
                    continue

            producer.begin_transaction()
            should_abort = random.random() < abort_probability
            batch: dict[str, WriteInfo] = {}

            def cb(err, msg):
                if err is not None:
                    self.log(-1, f'produce delivery error: {err}')
                    os._exit(1)
                if msg.error() is not None:
                    self.log(-1, f'produce delivery msg error: {msg.error()}')
                    os._exit(1)
                with self._locks['write_process']:
                    key = msg.key()
                    partition = msg.partition()
                    last_offset = batch[key].offset = msg.offset()
                    self.log(partition, f'last_offset\t{last_offset}')

            for _ in range(batch_size):
                op = WriteInfo(id, f'key{id}', pid, -1)
                batch[op.key] = op
                if should_abort:
                    self.log(pid, f"a\t{op.id}")
                else:
                    self.log(pid, f"w\t{op.id}")

                value = f'{op.id}\t{"a" if should_abort else "c"}\t{self.random_string(1024)}'

                producer.produce(self.args['topic'], value, op.key, pid, cb)
                # this doesn't work - producer.produce returns None
                with self._locks['write_process']:
                    id += 1

            if should_abort:
                producer.flush()
                producer.abort_transaction()
            else:
                producer.commit_transaction()

    def read_process(self, pid):
        config = tlocal()
        config.__dict__.update({
            'bootstrap.servers': self.args['brokers'],
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
            'auto.offset.reset': 'earliest',
            'group.id': 'my-group'
        })
        consumer = Consumer(config.__dict__)
        partition = self.partitions[pid]
        # TODO: finish porting this over from Java

    def get_metrics(self):
        # TODO: implement metrics
        return {}


def main():
    app = Flask(__name__)
    workload: Optional[GatedWorkload] = None

    @app.route('/ping', methods=['GET'])
    def ping():
        return ''

    @app.route('/info', methods=['GET'])
    def info():
        if workload is None:
            abort(409)
        return workload.get_metrics()

    # curl -vX POST http://127.0.0.1:8080/start-producer -H 'Content-Type:
    # application/json' -d '{"abort_probability":0.1,"batch_size":10,
    # "brokers":"127.0.0.1:9092","partitions":1,"workload":"TX"}'
    @app.route('/start-producer', methods=['POST'])
    def start_producer():
        params = request.get_json()
        match params['workload']:
            case 'TX':
                workload = TxWorkload(params)
            case _:
                print(f'unknown workload: "{params["workload"]}"')
                raise NotImplementedError
        workload.start_producer()
        return ''

    @app.route('/stop-producer', methods=['POST'])
    def stop_producer():
        if workload is None:
            abort(409)
        workload.stop_producer()
        return ''

    @app.route('/wait-producer', methods=['POST'])
    def wait_producer():
        if workload is None:
            abort(409)
        workload.wait_producer()
        return ''

    @app.route('/start-consumer', methods=['POST'])
    def start_consumer():
        if workload is None:
            abort(409)
        workload.start_consumer()
        return ''

    @app.route('/wait-consumer', methods=['POST'])
    def wait_consumer():
        if workload is None:
            abort(409)
        workload.wait_consumer()
        return ''

    def run_flask(*args):
        port = int(os.getenv('PORT', '8080'))
        app.run(port=port, debug=False)

    main_thread = Thread(target=run_flask)
    main_thread.start()
    main_thread.join()


if __name__ == '__main__':
    main()
