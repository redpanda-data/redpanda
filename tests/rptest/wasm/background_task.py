# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import threading
import traceback


class BackgroundTask:
    """
    Spawn and manages a task in the background. Catches and re-throws any
    exceptions thrown on a background thread to the callers thread.
    """
    def __init__(self):
        self._done = False
        self._error = None
        self._lock = threading.Lock()
        self._worker = threading.Thread(name=self.task_name(),
                                        target=self._run_protected,
                                        daemon=True,
                                        args=())

    def task_name(self):
        raise Exception('Unimplemented method, BackgroundTask::task_name')

    def _run(self):
        raise Exception('Unimplemented method, BackgroundTask::run')

    def _run_protected(self):
        try:
            self._run()
        except BaseException:
            with self._lock:
                tb = traceback.format_exc()
                self._error = f"{threading.currentThread().name}:  {tb}"
            raise
        finally:
            with self._lock:
                self._done = True

    def is_finished(self):
        with self._lock:
            return self._done

    def start(self):
        self._worker.start()

    def stop(self):
        with self._lock:
            self._done = True

    def join(self):
        self._worker.join()
        if self._error is not None:
            raise Exception(self._error)
