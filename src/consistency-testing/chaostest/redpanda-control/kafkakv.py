#!/usr/bin/env python3

# pip3 install kafka-python flask
# rpk api topic create -p 1 -r 3 topic3
# python3 kafkakv.py --topic topic3 --log kafka1.log --port 9891 --broker 172.31.38.96:9092 --broker 172.31.37.22:9092 --broker 172.31.35.89:9092

from kafka import KafkaProducer, TopicPartition, KafkaConsumer
from kafka.errors import (KafkaError, KafkaTimeoutError,
                          NotLeaderForPartitionError, NoBrokersAvailable,
                          KafkaConnectionError, UnknownTopicOrPartitionError,
                          RequestTimedOutError)
import sys
from time import sleep
import time
import threading
import logging
import logging.handlers
import argparse
import json
import copy
import uuid
import traceback
from flask import Flask, request

##############################################################

kafkakv_log = logging.getLogger("kafkakv_log")
kafkakv_stdout = logging.getLogger("kafkakv_stdout")


class m:
    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs
        if message is not None:
            self.kwargs["message"] = message

    def with_time(self):
        self.kwargs["time_ms"] = int(time.time() * 1000)
        return self

    def __str__(self):
        return json.dumps(self.kwargs)


##############################################################


class RequestTimedout(Exception):
    pass


class RequestCanceled(Exception):
    pass


class UnknownTopic(Exception):
    pass


class KafkaKV:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000,  #default 30000
            max_block_ms=10000,  # default 60000
            metadata_max_age_ms=30000,  #default 300000
            acks=-1)
        self.offset = None
        self.state = dict()
        self.consumers = []
        self.n_consumers = 0

    def catchup(self, state, from_offset, to_offset, cmd, metrics):
        consumer = None
        tps = None
        init_started = time.time()

        if len(self.consumers) > 0:
            consumer, tps, cid = self.consumers.pop(0)
        else:
            try:
                consumer = KafkaConsumer(
                    client_id=uuid.uuid4(),
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=10000,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest")
            except ValueError as e:
                kafkakv_log.info(
                    m("Error on creating consumer",
                      type=str(type(e)),
                      msg=str(e),
                      stacktrace=traceback.format_exc()).with_time())
                kafkakv_stdout.info("Error on creating consumer")
                raise RequestTimedout()
            ps = consumer.partitions_for_topic(self.topic)
            if ps is None:
                raise UnknownTopic()
            tps = [TopicPartition(self.topic, p) for p in ps]
            if len(tps) != 1:
                kafkakv_log.info(
                    m("Topic " + str(self.topic) +
                      " should have a single partition but got: " +
                      str(len(tps))).with_time())
                kafkakv_stdout.info(
                    "Topic " + str(self.topic) +
                    " should have a single partition but got: " +
                    str(len(tps)))
                raise RequestTimedout()
            consumer.assign(tps)
            cid = self.n_consumers
            self.n_consumers += 1

        metrics["init_us"] = int((time.time() - init_started) * 1000000)
        catchup_started = time.time()
        if from_offset is None:
            consumer.seek_to_beginning(tps[0])
        else:
            consumer.seek(tps[0], from_offset + 1)
        processed = 0
        while consumer.position(tps[0]) <= to_offset:
            rs = consumer.poll()
            if tps[0] in rs:
                for record in rs[tps[0]]:
                    if record.offset <= to_offset:
                        data = json.loads(record.value.decode("utf-8"))
                        processed += 1
                        if "writeID" in data:
                            if "prevWriteID" in data:
                                if data["key"] in state:
                                    if state[data["key"]]["writeID"] == data[
                                            "prevWriteID"]:
                                        state[data["key"]] = {
                                            "value": data["value"],
                                            "writeID": data["writeID"]
                                        }
                            else:
                                state[data["key"]] = {
                                    "value": data["value"],
                                    "writeID": data["writeID"]
                                }

        result = None
        if cmd["key"] in state:
            result = state[cmd["key"]]

        kafkakv_log.info(
            m("caught",
              cmd=cmd,
              result=result,
              base_offset=from_offset,
              sent_offset=to_offset,
              processed=processed,
              cid=cid).with_time())
        metrics["catchup_us"] = int((time.time() - catchup_started) * 1000000)
        self.consumers.append((consumer, tps, cid))
        return state

    def execute(self, cmd, metrics):
        msg = json.dumps(cmd).encode("utf-8")

        offset = self.offset
        state = copy.deepcopy(self.state)

        kafkakv_log.info(
            m("executing", cmd=cmd, base_offset=offset).with_time())

        send_started = time.time()
        written = None
        try:
            future = self.producer.send(self.topic, msg)
            written = future.get(timeout=10)
        except UnknownTopicOrPartitionError:
            # well that's (phantom) data loss
            # how to repro:
            #   topic has replication factor 3
            #   for each node there is k clients which specifies only it as a bootstrap_servers
            #   start workload
            #   wait ~20 seconds, kill leader
            #   wait 5 seconds, restart former leader
            #   observe UnknownTopicOrPartitionError
            raise RequestTimedout()
        except KafkaConnectionError:
            raise RequestTimedout()
        except KafkaTimeoutError:
            raise RequestTimedout()
        except NotLeaderForPartitionError:
            raise RequestCanceled()
        except RequestTimedOutError:
            raise RequestTimedout()
        except KafkaError as e:
            kafkakv_log.info(
                m("Run into an unexpected Kafka error on sending",
                  type=str(type(e)),
                  msg=str(e),
                  stacktrace=traceback.format_exc()).with_time())
            kafkakv_stdout.info("Run into an unexpected Kafka error " +
                                str(type(e)) + ": " + str(e) + " on sending")
            raise RequestTimedout()
        except:
            e, v, t = sys.exc_info()
            kafkakv_log.info(
                m("Run into an unexpected error on sending",
                  type=str(e),
                  msg=str(v),
                  stacktrace=traceback.format_exc()).with_time())
            kafkakv_stdout.info("Run into an unexpected error " + str(e) +
                                ": " + str(v) + " @ " + str(t) + " on sending")
            raise

        metrics["send_us"] = int((time.time() - send_started) * 1000000)

        kafkakv_log.info(
            m("sent", cmd=cmd, base_offset=offset,
              sent_offset=written.offset).with_time())

        try:
            state = self.catchup(state, offset, written.offset, cmd, metrics)
        except NoBrokersAvailable:
            raise RequestTimedout()
        except UnknownTopic:
            raise RequestTimedout()
        except RequestTimedout:
            raise
        except:
            e, v, t = sys.exc_info()
            kafkakv_log.info(
                m("Run into an unexpected error on catching up",
                  type=str(e),
                  msg=str(v),
                  stacktrace=traceback.format_exc()).with_time())
            kafkakv_stdout.info("Run into an unexpected error " + str(e) +
                                ": " + str(v) + " @ " + str(t) +
                                " on catching up")
            raise RequestTimedout()

        if self.offset is None or self.offset < written.offset:
            base_offset = self.offset
            self.state = state
            self.offset = written.offset
            kafkakv_log.info(
                m("updated",
                  cmd=cmd,
                  base_offset=offset,
                  root_offset=base_offset,
                  sent_offset=written.offset).with_time())

        return state

    def write(self, key, value, write_id, metrics):
        state = self.execute({
            "key": key,
            "value": value,
            "writeID": write_id
        }, metrics)
        return state[key]

    def read(self, key, metrics):
        state = self.execute({"key": key}, metrics)
        return state[key] if key in state else None

    def cas(self, key, prev_write_id, value, write_id, metrics):
        state = self.execute(
            {
                "key": key,
                "prevWriteID": prev_write_id,
                "value": value,
                "writeID": write_id
            }, metrics)
        return state[key] if key in state else None


#################################################################################################

parser = argparse.ArgumentParser(description='kafka-kvelldb')
parser.add_argument('--log', required=True)
parser.add_argument('--topic', required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--broker', action='append', required=True)
args = parser.parse_args()

kafkakv_log.setLevel(logging.INFO)
kafkakv_stdout.setLevel(logging.INFO)

kafkakv_file_handler = logging.handlers.RotatingFileHandler(args.log,
                                                            maxBytes=10 *
                                                            1024 * 1024,
                                                            backupCount=5,
                                                            mode='w')
kafkakv_file_handler.setFormatter(logging.Formatter("%(message)s"))
kafkakv_log.addHandler(kafkakv_file_handler)

# adding console handler
kafkakv_stdout_handler = logging.StreamHandler()
kafkakv_stdout_handler.setLevel(logging.INFO)
kafkakv_stdout_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(message)s"))
kafkakv_stdout.addHandler(kafkakv_stdout_handler)

kafkakv = KafkaKV(args.broker, args.topic)

app = Flask(__name__)


@app.route('/read', methods=['GET'])
def read():
    metrics = {}
    key = request.args.get("key")

    try:
        data = kafkakv.read(key, metrics)
        if data is None:
            return {"status": "ok", "hasData": False, "metrics": metrics}
        else:
            return {
                "status": "ok",
                "hasData": True,
                "writeID": data["writeID"],
                "value": data["value"],
                "metrics": metrics
            }
    except RequestTimedout:
        return {"status": "unknown", "metrics": metrics}
    except RequestCanceled:
        return {"status": "fail", "metrics": metrics}
    except:
        # TODO: log error
        return {"status": "unknown", "metrics": metrics}


@app.route('/write', methods=['POST'])
def write():
    metrics = {}
    body = request.get_json(force=True)
    try:
        data = kafkakv.write(body["key"], body["value"], body["writeID"],
                             metrics)
        if data is None:
            return {"status": "ok", "hasData": False, "metrics": metrics}
        else:
            return {
                "status": "ok",
                "hasData": True,
                "writeID": data["writeID"],
                "value": data["value"],
                "metrics": metrics
            }
    except RequestTimedout:
        return {"status": "unknown", "metrics": metrics}
    except RequestCanceled:
        return {"status": "fail", "metrics": metrics}
    except:
        # TODO: log error
        return {"status": "unknown", "metrics": metrics}


@app.route('/cas', methods=['POST'])
def cas():
    metrics = {}
    body = request.get_json(force=True)

    try:
        data = kafkakv.cas(body["key"], body["prevWriteID"], body["value"],
                           body["writeID"], metrics)
        if data is None:
            return {"status": "ok", "hasData": False, "metrics": metrics}
        else:
            return {
                "status": "ok",
                "hasData": True,
                "writeID": data["writeID"],
                "value": data["value"],
                "metrics": metrics
            }
    except RequestTimedout:
        return {"status": "unknown", "metrics": metrics}
    except RequestCanceled:
        return {"status": "fail", "metrics": metrics}
    except:
        # TODO: log error
        return {"status": "unknown", "metrics": metrics}


app.run(host='0.0.0.0', port=args.port, use_reloader=False, threaded=True)
