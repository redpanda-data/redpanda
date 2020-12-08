#!/usr/bin/env python3
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

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
from threading import Lock
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
kafkakv_err = logging.getLogger("kafkakv_err")
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
    def __init__(self, check_history, inflight_limit, bootstrap_servers, topic,
                 acks):
        self.topic = topic
        self.acks = acks
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=1000,  #default 30000
            max_block_ms=10000,  # default 60000
            metadata_max_age_ms=5000,  #default 300000
            acks=acks)
        self.offset = None
        self.state = dict()
        self.consumers = []
        self.n_consumers = 0
        self.check_history = check_history
        self.inflight_limit = inflight_limit
        self.inflight_requests = 0
        self.has_accessed = False
        self.has_data_loss = False
        self.data_loss_info = None
        self.mutex = Lock()

    def start_history_check_thread(self):
        consumer_tps = None
        while not self.has_data_loss:
            with self.mutex:
                offset = self.offset
                state = copy.deepcopy(self.state)
            try:
                replay = dict()
                consumer_tps = self.catchup_beginning(consumer_tps, replay,
                                                      offset,
                                                      {"key": "replay"}, {})
                missing = []
                mismatch = []
                extra = []
                for key in state.keys():
                    if key not in replay:
                        missing.append({"key": key, "extected": state[key]})
                    elif state[key]["writeID"] != replay[key][
                            "writeID"] or state[key]["value"] != replay[key][
                                "value"]:
                        mismatch.append({
                            "key": key,
                            "extected": state[key],
                            "reconstructed": replay[key]
                        })
                for key in replay.keys():
                    if key not in state:
                        extra.append({
                            "key": key,
                            "reconstructed": replay[key]
                        })
                if len(missing) > 0 or len(mismatch) > 0 or len(extra) > 0:
                    with self.mutex:
                        self.data_loss_info = {
                            "missing": missing,
                            "mismatch": mismatch,
                            "extra": extra
                        }
                        self.has_data_loss = True
            except:
                consumer_tps = None
                pass
            time.sleep(1)

    def catchup(self, state, from_offset, to_offset, cmd, metrics):
        consumer = None
        tps = None
        cid = None
        init_started = time.clock_gettime(time.CLOCK_MONOTONIC)

        if len(self.consumers) > 0:
            consumer, tps, cid = self.consumers.pop(0)
        else:
            try:
                consumer = KafkaConsumer(
                    client_id=uuid.uuid4(),
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=1000,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest")
            except ValueError as e:
                msg = m("Error on creating consumer",
                        type=str(type(e)),
                        msg=str(e),
                        stacktrace=traceback.format_exc()).with_time()
                kafkakv_log.info(msg)
                kafkakv_err.info(msg)
                kafkakv_stdout.info("Error on creating consumer")
                raise RequestTimedout()
            tps = [TopicPartition(self.topic, 0)]
            consumer.assign(tps)
            with self.mutex:
                cid = self.n_consumers
                self.n_consumers += 1

        try:
            metrics["init_us"] = int(
                (time.clock_gettime(time.CLOCK_MONOTONIC) - init_started) *
                1000000)
            catchup_started = time.clock_gettime(time.CLOCK_MONOTONIC)
            if from_offset is None:
                consumer.seek_to_beginning(tps[0])
            else:
                consumer.seek(tps[0], from_offset + 1)
            processed = 0

            while consumer.position(tps[0]) <= to_offset:
                rs = consumer.poll()

                if tps[0] not in rs:
                    continue

                for record in rs[tps[0]]:
                    if record.offset > to_offset:
                        break

                    data = json.loads(record.value.decode("utf-8"))
                    processed += 1

                    if "writeID" not in data:
                        continue

                    if "prevWriteID" in data:
                        if data["key"] not in state:
                            continue

                        current = state[data["key"]]
                        if current["writeID"] == data["prevWriteID"]:
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
            metrics["catchup_us"] = int(
                (time.clock_gettime(time.CLOCK_MONOTONIC) - catchup_started) *
                1000000)
            self.consumers.append((consumer, tps, cid))
            return state
        except:
            try:
                consumer.close()
            except:
                pass
            raise

    def catchup_beginning(self, consumer_tps, state, to_offset, cmd, metrics):
        init_started = time.clock_gettime(time.CLOCK_MONOTONIC)

        if consumer_tps == None:
            try:
                consumer = KafkaConsumer(
                    client_id=uuid.uuid4(),
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=1000,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest")
            except ValueError as e:
                msg = m("Error on creating consumer",
                        type=str(type(e)),
                        msg=str(e),
                        stacktrace=traceback.format_exc()).with_time()
                kafkakv_log.info(msg)
                kafkakv_err.info(msg)
                kafkakv_stdout.info("Error on creating consumer")
                raise RequestTimedout()
            tps = [TopicPartition(self.topic, 0)]
            consumer.assign(tps)
            consumer_tps = (consumer, tps)

        consumer, tps = consumer_tps

        try:
            metrics["init_us"] = int(
                (time.clock_gettime(time.CLOCK_MONOTONIC) - init_started) *
                1000000)
            catchup_started = time.clock_gettime(time.CLOCK_MONOTONIC)
            consumer.seek_to_beginning(tps[0])
            processed = 0

            while consumer.position(tps[0]) <= to_offset:
                rs = consumer.poll()

                if tps[0] not in rs:
                    continue

                for record in rs[tps[0]]:
                    if record.offset > to_offset:
                        break

                    data = json.loads(record.value.decode("utf-8"))
                    processed += 1

                    if "writeID" not in data:
                        continue

                    if "prevWriteID" in data:
                        if data["key"] not in state:
                            continue

                        current = state[data["key"]]
                        if current["writeID"] == data["prevWriteID"]:
                            state[data["key"]] = {
                                "value": data["value"],
                                "writeID": data["writeID"]
                            }
                    else:
                        state[data["key"]] = {
                            "value": data["value"],
                            "writeID": data["writeID"]
                        }

            kafkakv_log.info(
                m("caught",
                  cmd=cmd,
                  sent_offset=to_offset,
                  processed=processed).with_time())
            metrics["catchup_us"] = int(
                (time.clock_gettime(time.CLOCK_MONOTONIC) - catchup_started) *
                1000000)
            return consumer_tps
        except:
            try:
                consumer.close()
            except:
                pass
            raise

    def execute(self, payload, cmd, metrics):
        msg = json.dumps(payload).encode("utf-8")

        with self.mutex:
            if not self.has_accessed and self.offset is not None:
                self.has_accessed = True
                if self.check_history:
                    thread = threading.Thread(
                        target=lambda: self.start_history_check_thread())
                    thread.start()

            offset = self.offset
            state = copy.deepcopy(self.state)

        kafkakv_log.info(
            m("executing", cmd=cmd, base_offset=offset).with_time())

        send_started = time.clock_gettime(time.CLOCK_MONOTONIC)
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
            msg = m("Run into an unexpected Kafka error on sending",
                    type=str(type(e)),
                    msg=str(e),
                    stacktrace=traceback.format_exc()).with_time()
            kafkakv_log.info(msg)
            kafkakv_err.info(msg)
            kafkakv_stdout.info("Run into an unexpected Kafka error " +
                                str(type(e)) + ": " + str(e) + " on sending")
            raise RequestTimedout()
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            msg = m("Run into an unexpected error on sending",
                    type=str(e),
                    msg=str(v),
                    stacktrace=stacktrace).with_time()
            kafkakv_log.info(msg)
            kafkakv_err.info(msg)
            kafkakv_stdout.info("Run into an unexpected error " + str(e) +
                                ": " + str(v) + " @ " + stacktrace +
                                " on sending")
            raise

        metrics["send_us"] = int(
            (time.clock_gettime(time.CLOCK_MONOTONIC) - send_started) *
            1000000)

        kafkakv_log.info(
            m("sent", cmd=cmd, base_offset=offset,
              sent_offset=written.offset).with_time())

        with self.mutex:
            if offset != None and written.offset <= offset:
                error = f"Monotonicity violation: written offset ({written.offset}) is behind current offset ({offset})"
                msg = m(error, stacktrace=stacktrace).with_time()
                kafkakv_log.info(msg)
                kafkakv_err.info(msg)
                self.has_data_loss = True
                self.data_loss_info = error
            elif self.offset == written.offset:
                error = f"Conflict write: written offset ({written.offset}) is same as the current offset ({self.offset})"
                msg = m(error, stacktrace=stacktrace).with_time()
                kafkakv_log.info(msg)
                kafkakv_err.info(msg)
                self.has_data_loss = True
                self.data_loss_info = error

        try:
            state = self.catchup(state, offset, written.offset, cmd, metrics)
        except NoBrokersAvailable:
            raise RequestTimedout()
        except UnknownTopic:
            raise RequestTimedout()
        except RequestTimedout:
            raise
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            msg = m("Run into an unexpected error on catching up",
                    type=str(e),
                    msg=str(v),
                    stacktrace=stacktrace).with_time()
            kafkakv_log.info(msg)
            kafkakv_err.info(msg)
            kafkakv_stdout.info("Run into an unexpected error " + str(e) +
                                ": " + str(v) + " @ " + stacktrace +
                                " on catching up")
            raise RequestTimedout()

        with self.mutex:
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
        if self.inflight_limit <= self.inflight_requests:
            raise RequestCanceled()
        else:
            try:
                self.inflight_requests += 1
                cmd = {"key": key, "value": value, "writeID": write_id}
                state = self.execute(cmd, cmd, metrics)
                return state[key]
            finally:
                self.inflight_requests -= 1

    def read(self, key, read_id, metrics):
        if self.inflight_limit <= self.inflight_requests:
            raise RequestCanceled()
        else:
            try:
                self.inflight_requests += 1
                state = self.execute({}, {
                    "key": key,
                    "read_id": read_id
                }, metrics)
                return state[key] if key in state else None
            finally:
                self.inflight_requests -= 1

    def cas(self, key, prev_write_id, value, write_id, metrics):
        if self.inflight_limit <= self.inflight_requests:
            raise RequestCanceled()
        else:
            try:
                self.inflight_requests += 1
                cmd = {
                    "key": key,
                    "prevWriteID": prev_write_id,
                    "value": value,
                    "writeID": write_id
                }
                state = self.execute(cmd, cmd, metrics)
                return state[key] if key in state else None
            finally:
                self.inflight_requests -= 1


#################################################################################################

parser = argparse.ArgumentParser(description='kafka-kvelldb')
parser.add_argument('--check-history', action='store_true')
parser.add_argument('--log', required=True)
parser.add_argument('--err', required=True)
parser.add_argument('--topic', required=True)
parser.add_argument('--acks', required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--broker', action='append', required=True)
parser.add_argument('--inflight-limit', type=int, required=True)
args = parser.parse_args()

kafkakv_log.setLevel(logging.INFO)
kafkakv_err.setLevel(logging.INFO)
kafkakv_stdout.setLevel(logging.INFO)

kafkakv_log_handler = logging.handlers.RotatingFileHandler(args.log,
                                                           maxBytes=5 * 1024 *
                                                           1024,
                                                           backupCount=1,
                                                           mode='w')
kafkakv_log_handler.setFormatter(logging.Formatter("%(message)s"))
kafkakv_log.addHandler(kafkakv_log_handler)

kafkakv_err_handler = logging.handlers.RotatingFileHandler(args.err,
                                                           maxBytes=5 * 1024 *
                                                           1024,
                                                           backupCount=1,
                                                           mode='w')
kafkakv_err_handler.setFormatter(logging.Formatter("%(message)s"))
kafkakv_err.addHandler(kafkakv_err_handler)

# adding console handler
kafkakv_stdout_handler = logging.StreamHandler()
kafkakv_stdout_handler.setLevel(logging.INFO)
kafkakv_stdout_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(message)s"))
kafkakv_stdout.addHandler(kafkakv_stdout_handler)

kafkakv = KafkaKV(args.check_history, args.inflight_limit, args.broker,
                  args.topic, json.loads(args.acks))

app = Flask(__name__)


@app.route('/read', methods=['GET'])
def read():
    metrics = {}
    key = request.args.get("key")
    read_id = request.args.get("read_id")

    if kafkakv.has_data_loss:
        return {"status": "violation", "info": kafkakv.data_loss_info}

    try:
        data = kafkakv.read(key, read_id, metrics)
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

    if kafkakv.has_data_loss:
        return {"status": "violation", "info": kafkakv.data_loss_info}

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

    if kafkakv.has_data_loss:
        return {"status": "violation", "info": kafkakv.data_loss_info}

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


has_errors = False


def started():
    time.sleep(5)
    if not has_errors:
        kafkakv_log.info(m("Successfully started iofaults!"))


thread = threading.Thread(target=started)
thread.start()

try:
    app.run(host='0.0.0.0', port=args.port, use_reloader=False, threaded=True)
except:
    has_errors = True
    raise
