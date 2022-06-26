from asyncio import threads
import sys

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka import TopicPartition, OFFSET_BEGINNING
from flask import Flask, request
from threading import Thread
import logging
import time


class SyncProducer:
    def __init__(self, bootstrap):
        self.bootstrap = bootstrap
        self.producer = None
        self.last_msg = None

    def init(self):
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap,
            "enable.idempotence": True,
            "linger.ms": 0
        })

    def on_delivery(self, err, msg):
        if err is not None:
            raise KafkaException(err)
        self.last_msg = msg

    def produce(self, topic, key, value):
        self.last_msg = None
        self.producer.produce(topic,
                              key=key,
                              value=value,
                              callback=lambda e, m: self.on_delivery(e, m))
        self.producer.flush()
        msg = self.last_msg
        self.last_msg = None
        if msg.error() != None:
            raise KafkaException(msg.error())
        if msg.offset() == None:
            raise Exception("offset() of a successful produce can't be None")
        return {"offset": msg.offset(), "partition": msg.partition()}


class LogChecker:
    def __init__(self, bootstrap, topic):
        self.bootstrap = bootstrap
        self.topic = topic

    def check(self, history, timeout_s):
        for partition in history.keys():
            logging.debug(f"validating {self.topic}/{partition}")
            begin = time.time()
            consumer = Consumer({
                "bootstrap.servers": self.bootstrap,
                "group.id": "group-kaf",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed"
            })
            consumer.assign(
                [TopicPartition(self.topic, partition, OFFSET_BEGINNING)])

            i = 0
            while True:
                if i == len(history[partition]):
                    break
                if time.time() - begin > timeout_s:
                    raise Exception("timeout")
                msgs = consumer.consume(timeout=10)
                for msg in msgs:
                    if msg.error() != None:
                        logging.error(msg.error())
                    offset = msg.offset()
                    key = msg.key().decode('utf-8')
                    value = msg.value().decode('utf-8')
                    written = history[partition][i]
                    if offset != written[0]:
                        raise Exception("mismatched offset")
                    if key != written[1]:
                        raise Exception("mismatched key")
                    if value != written[2]:
                        raise Exception("mismatched value")
                    i += 1


class ProducingThread:
    def __init__(self):
        self.is_active = False
        self.is_broken = False
        self.thread = None
        self.topic = None
        self.count = -1
        self.connection = None
        self.keep_history = False
        self.history = dict()


class Service:
    def __init__(self):
        self.threads = dict()

    def validate(self, name, timeout_s):
        if name not in self.threads:
            raise Exception(f"unknown name {name}")
        thread = self.threads[name]
        if thread.is_broken:
            raise Exception("broken")
        if thread.is_active:
            raise Exception("active")
        if not (thread.keep_history):
            raise Exception("w/o history")
        checker = LogChecker(thread.connection, thread.topic)
        checker.check(thread.history, timeout_s)

    def process(self, thread):
        try:
            client = SyncProducer(thread.connection)
            client.init()

            for i in range(0, thread.count):
                key = f"key-{str(i).zfill(8)}"
                value = f"record-{str(i).zfill(8)}"
                info = client.produce(thread.topic, key.encode('utf-8'),
                                      value.encode('utf-8'))
                if thread.keep_history:
                    if info["partition"] not in thread.history:
                        thread.history[info["partition"]] = []
                    thread.history[info["partition"]].append(
                        (info["offset"], key, value))
        except:
            logging.exception("something is wrong")
            thread.is_broken = True
        finally:
            thread.is_active = False

    def start(self, name, connection, topic, count, keep_history):
        thread = ProducingThread()
        thread.is_active = True
        thread.is_broken = False
        thread.topic = topic
        thread.count = count
        thread.connection = connection
        thread.keep_history = keep_history
        thread.thread = Thread(target=lambda: self.process(thread))
        self.threads[name] = thread
        self.threads[name].thread.start()

    def wait(self, name):
        if name not in self.threads:
            raise Exception(f"unknown name {name}")
        if self.threads[name].is_broken:
            raise Exception("broken")
        if not (self.threads[name].is_active):
            return
        self.threads[name].thread.join()
        if self.threads[name].is_broken:
            raise Exception("broken")


app = Flask(__name__)
service = Service()


@app.route('/ping', methods=['GET'])
def ping():
    return ""


@app.route('/start', methods=['POST'])
def start():
    body = request.get_json(force=True)
    name = body["name"]
    connection = body["connection"]
    topic = body["topic"]
    count = body["count"]
    keep_history = body["keep_history"]
    service.start(name, connection, topic, count, keep_history)
    return ""


@app.route('/validate', methods=['POST'])
def validate():
    body = request.get_json(force=True)
    name = body["name"]
    timeout_s = body["timeout_s"]
    service.validate(name, timeout_s)
    return ""


@app.route('/wait', methods=['GET'])
def wait():
    body = request.get_json(force=True)
    name = body["name"]
    service.wait(name)
    return ""


app.run(host='0.0.0.0', port=8080, use_reloader=False, threaded=True)
