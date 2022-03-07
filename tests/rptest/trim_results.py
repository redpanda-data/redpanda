#!/usr/bin/python3

# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import logging
import os
import subprocess
import tempfile
import json
import shutil
import sys
import concurrent.futures

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


class Trimmer:
    """
    Trim the redpanda logs in a ducktape result tree, such that successful
    tests do not retain DEBUG or TRACE logs.
    """
    def __init__(self, result_path):
        self.result_path = result_path

    def get_passed_test_logs(self):
        results = json.load(open(os.path.join(self.result_path,
                                              "report.json")))['results']
        log_paths = []
        for r in results:
            results_dir = os.path.join(self.result_path,
                                       r['relative_results_dir'])

            if r['test_status'] == "PASS":
                for service in r['services']:
                    if service['cls_name'] == "RedpandaService":
                        service_dir = os.path.join(results_dir,
                                                   service['service_id'])
                        for node in service['nodes']:
                            hostname = node.split("@")[-1]
                            log_path = os.path.join(
                                service_dir, f"{hostname}/redpanda.log")
                            if os.path.exists(log_path):
                                log_paths.append(log_path)
                            else:
                                log.debug(f"No service log for {log_path}")
            else:
                log.info(
                    f"Preserving log for non-passing ({r['test_status']}) test {r['relative_results_dir']}"
                )

        return log_paths

    def trim(self):
        log_paths = self.get_passed_test_logs()

        executor = concurrent.futures.ThreadPoolExecutor()

        def trim_one(path):
            with tempfile.NamedTemporaryFile() as tmpfile:
                log.debug(f"Filtering {path}")
                subprocess.check_call(
                    ["grep", "-v", "-e", "TRACE", "-e", "DEBUG", path],
                    stdout=tmpfile)
                shutil.copyfile(tmpfile.name, path)

        futures = []
        for p in log_paths:
            futures.append(executor.submit(trim_one, p))

        for f in futures:
            f.result()

        log.info(f"Filtered {len(log_paths)} logs in {self.result_path}")


if len(sys.argv) > 1:
    Trimmer(sys.argv[1]).trim()
else:
    Trimmer("./").trim()
