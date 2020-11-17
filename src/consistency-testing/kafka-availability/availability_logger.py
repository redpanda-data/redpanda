#!/usr/bin/env python
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# pip3 install kafka-python
# rpk api topic create -p 1 -r 3 topic1
# python3 availability_logger.py --duration 240 --broker 172.31.33.70:9092 --broker 172.31.46.115:9092 --broker 172.31.38.64:9092 --topic topic1
# cat lat.log | grep ok > lat-ok.log

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NotLeaderForPartitionError
import sys
from time import sleep
import time
import threading
import logging
import logging.handlers
import argparse


def setup_logger(logger_name,
                 log_file,
                 maxBytes,
                 backupCount,
                 level=logging.INFO):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=maxBytes,
                                                       backupCount=backupCount,
                                                       mode='w')
    fileHandler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(fileHandler)
    return logger


def init_logs(latency_file, stat_file):
    setup_logger("latency", latency_file, 10 * 1024 * 1024, 5)
    stat_logger = setup_logger("stat", stat_file, 10 * 1024 * 1024, 5)

    # adding console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    stat_logger.addHandler(ch)


def log_latency(type, time_s, latency_s):
    log = logging.getLogger("latency")
    message = f"{int(time_s)}\t{int(latency_s*1000*1000)}\t{type}"
    log.info(message)


def log_stat(message):
    log = logging.getLogger("stat")
    log.info(message)


class Stat:
    def __init__(self):
        self.counters = dict()
        self.vars = dict()

    def assign(self, key, val):
        self.vars[key] = val

    def inc(self, key):
        if key not in self.counters:
            self.counters[key] = 0
        self.counters[key] += 1

    def reset(self):
        copy = self.counters
        self.counters = dict()
        for key in self.vars:
            copy[key] = self.vars[key]
        return copy


class StatDumper:
    def __init__(self, stat, keys):
        self.stat = stat
        self.keys = keys
        self.is_active = True

    def start(self):
        started = time.time()
        while self.is_active:
            counters = self.stat.reset()
            line = str(int(time.time() - started))
            for key in self.keys:
                if key in counters:
                    line += "\t" + str(counters[key])
                else:
                    line += "\t" + str(0)
            log_stat(line)
            sleep(1)

    def stop(self):
        self.is_active = False


is_active = True


def writer(name, stat, producer, era, topic):
    global is_active
    while is_active:
        started = time.time()
        future = producer.send(topic, b"event1")
        try:
            _ = future.get(timeout=10)
            ended = time.time()
            log_latency("ok", started - era, ended - started)
            stat.inc("all:ok")
            stat.inc(name + ":ok")
        except KafkaTimeoutError as e:
            stat.inc(name + ":out")
            ended = time.time()
            log_latency("out", started - era, ended - started)
        except NotLeaderForPartitionError as e:
            stat.inc(name + ":err")
            ended = time.time()
            log_latency("err", started - era, ended - started)
        except KafkaError as e:
            stat.inc(name + ":err")
            ended = time.time()
            print(e)
            log_latency("err", started - era, ended - started)
        except:
            stat.inc(name + ":err")
            e = sys.exc_info()[0]
            print(e)


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--topic', required=True)
parser.add_argument('--broker', action='append', required=True)
parser.add_argument('--duration',
                    type=int,
                    default=240,
                    required=False,
                    help="duration")

args = parser.parse_args()

init_logs("lat.log", "1s.log")

i = 0
stat_dims = ["all:ok"]
producers = dict()

for broker in args.broker:
    stat_dims.append(f"p{i}:ok")
    stat_dims.append(f"p{i}:out")
    stat_dims.append(f"p{i}:err")
    producers[f"p{i}"] = KafkaProducer(bootstrap_servers=broker, acks=-1)
    i += 1

stat = Stat()
dumper = StatDumper(stat, stat_dims)

threads = []

threads.append(threading.Thread(target=lambda: dumper.start()))

for p in producers.keys():
    for i in range(0, 3):
        threads.append(
            threading.Thread(target=writer,
                             args=(p, stat, producers[p], time.time(),
                                   args.topic)))

for thread in threads:
    thread.start()

sleep(args.duration)

is_active = False
dumper.stop()

for thread in threads:
    thread.join()
