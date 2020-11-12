# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# https://docs.python.org/3/library/time.html
# CLOCK_REALTIME
# CLOCK_MONOTONIC¶

import time
import os
from os import path
import threading
import argparse
import sys
import json
import logging
import logging.handlers
import traceback
from flask import Flask, request

strobe_log = logging.getLogger("strobe_log")

parser = argparse.ArgumentParser(description='strobe')
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--storage', required=True)
parser.add_argument('--log', required=True)


class Injector:
    def __init__(self, storage):
        self.storage = storage
        self.is_injected = False
        self.is_active = False
        self.normal_offset = None
        self.wierd_offset = None
        self.faulting_thread = None

    def keep_faulting(self, period_ms):
        strobe_log.info("keep_faulting")
        try:
            wierd = False

            while self.is_active:
                to = time.clock_gettime(time.CLOCK_MONOTONIC) + (
                    self.wierd_offset if wierd else self.normal_offset)
                strobe_log.info("setting time to: " + str(to))
                time.clock_settime(time.CLOCK_REALTIME, to)
                strobe_log.info("setting time to: " + str(to))
                wierd = not wierd
                time.sleep(float(period_ms) / 1000)
        except:
            e, v = sys.exc_info()[:2]
            print("Run into an unexpected error " + str(e) + ": " + str(v) +
                  " @ " + traceback.format_exc())
            raise

    def inject(self, delta_ms, period_ms):
        if path.exists(self.storage
                       ) or self.is_injected or self.faulting_thread != None:
            return {"status": "failed", "message": "already injected"}

        self.normal_offset = time.clock_gettime(
            time.CLOCK_REALTIME) - time.clock_gettime(time.CLOCK_MONOTONIC)
        self.wierd_offset = self.normal_offset + float(delta_ms) / 1000
        with open(self.storage, 'w') as storage:
            json.dump({"normal_offset": self.normal_offset}, storage)

        self.is_injected = True
        self.is_active = True
        self.faulting_thread = threading.Thread(
            target=lambda: self.keep_faulting(period_ms))
        self.faulting_thread.start()
        return {"status": "ok"}

    def recover(self):
        self.is_active = False
        if self.faulting_thread != None:
            self.faulting_thread.join()
            self.faulting_thread = None
        if path.exists(self.storage):
            normal_offset = 0
            with open(self.storage, 'r') as storage:
                normal_offset = json.load(storage)["normal_offset"]
            time.clock_settime(
                time.CLOCK_REALTIME,
                time.clock_gettime(time.CLOCK_MONOTONIC) + normal_offset)
            os.remove(self.storage)
        self.is_injected = False
        return {"status": "ok"}


args = parser.parse_args()

injector = Injector(args.storage)
app = Flask(__name__)


@app.route('/inject', methods=['GET'])
def inject():
    strobe_log.info("inject")
    delta_ms = int(request.args.get("delta_ms"))
    period_ms = int(request.args.get("period_ms"))
    return injector.inject(delta_ms, period_ms)


@app.route('/recover', methods=['GET'])
def recover():
    strobe_log.info("recover")
    return injector.recover()


strobe_log.setLevel(logging.INFO)
strobe_log.setLevel(logging.INFO)
strobe_log_handler = logging.handlers.RotatingFileHandler(args.log,
                                                          maxBytes=10 * 1024 *
                                                          1024,
                                                          backupCount=5,
                                                          mode='w')
strobe_log_handler.setFormatter(logging.Formatter("%(message)s"))
strobe_log.addHandler(strobe_log_handler)

strobe_log.info("started")

app.run(host='0.0.0.0', port=args.port, use_reloader=False, threaded=True)
