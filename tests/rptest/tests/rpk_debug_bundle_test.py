# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import zipfile
import tempfile
import random
import string

from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.cluster import cluster
from rptest.clients.rpk_remote import RpkRemoteTool


class RpkDebugBundleTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkDebugBundleTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_debug_bundle(self):
        # The main RpkTool helper runs rpk on the test runner machine -- debug
        # commands are run on redpanda nodes.
        root_name = "bundle" + ''.join(
            random.choice(string.ascii_letters) for _ in range(5))
        bundle_name = root_name + ".zip"
        working_dir = "/tmp"
        file_path = os.path.join(working_dir, bundle_name)
        node = self.redpanda.nodes[0]

        rpk_remote = RpkRemoteTool(self.redpanda, node)
        output = rpk_remote.debug_bundle(file_path)
        lines = output.split("\n")

        # On error, rpk bundle returns 0 but writes error description to stdout
        output_file = None
        error_lines = []
        any_errs = False
        for l in lines:
            self.logger.info(l)
            if l.strip().startswith("* "):
                error_lines.append(l)
            elif 'errors occurred' in l:
                any_errs = True
            else:
                m = re.match("^Debug bundle saved to '(.+)'$", l)
                if m:
                    output_file = m.group(1)

        # Avoid false passes if our error line scraping gets broken
        # by a format change.
        if any_errs:
            assert error_lines, f"Found error, but unable to parse error lines: {output}"

        filtered_errors = []
        for l in error_lines:
            if "dmidecode" in l:
                # dmidecode doesn't work in ducktape containers, ignore
                # errors about it.
                continue
            if re.match(r".* error querying .*\.ntp\..* i\/o timeout", l):
                self.logger.error(f"Non-fatal transitory NTP error: {l}")
            else:
                self.logger.error(f"Bad output line: {l}")
                filtered_errors.append(l)

        assert not filtered_errors, f"Unexpected errors encountered: {filtered_errors}"
        assert output_file == file_path, f"Expected output file({output_file}) to be in {file_path}"

        node.account.copy_from(output_file, working_dir)

        zf = zipfile.ZipFile(output_file)
        files = zf.namelist()
        assert f'{root_name}/redpanda.yaml' in files, f"redpanda.yaml not found in the zip files: {files}"
        assert f'{root_name}/redpanda.log' in files, f"redpanda.log not found in the zip files: {files}"

        # At least the first controller log is being saved:
        assert f'{root_name}/controller-logs/redpanda/controller/0_0/0-1-v1.log' in files, f"controller log (0-1-v1.log) not found in the zip files: {files}"

        # Cluster admin API calls:
        assert f'{root_name}/admin/brokers.json' in files, f"admin/brokers.json not found in the zip files: {files}"
        assert f'{root_name}/admin/cluster_config.json' in files, f"admin/cluster_config.json not found in the zip files: {files}"
        assert f'{root_name}/admin/health_overview.json' in files, f"admin/health_overview.json not found in the zip files: {files}"

        # Per-node admin API calls:
        for n in self.redpanda.started_nodes():
            # rpk will save 2 snapsots per metrics endpoint:
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t0_metrics.txt' in files, f"/metrics/{n.account.hostname}-9644/t0_metrics.txt not found in the zip files: {files}"
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t1_metrics.txt' in files, f"/metrics/{n.account.hostname}-9644/t1_metrics.txt not found in the zip files: {files}"
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t0_public_metrics.txt' in files, f"/metrics/{n.account.hostname}-9644/t0_public_metrics.txt not found in the zip files: {files}"
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t1_public_metrics.txt' in files, f"/metrics/{n.account.hostname}-9644/t1_public_metrics.txt not found in the zip files: {files}"
            # and 1 cluster_view and node_config per node:
            assert f'{root_name}/admin/cluster_view_{n.account.hostname}-9644.json' in files, f"/admin/cluster_view_{n.account.hostname}-9644.json not found in the zip files: {files}"
            assert f'{root_name}/admin/node_config_{n.account.hostname}-9644.json' in files, f"/admin/node_config_{n.account.hostname}-9644.json not found in the zip files: {files}"
            assert f'{root_name}/admin/cpu_profile_{n.account.hostname}-9644.json' in files, f"/admin/cpu_profile_{n.account.hostname}-9644.json not found in the zip files: {files}"

    @cluster(num_nodes=3)
    def test_remote_debug_bundle_default(self):
        """
        e2e test, different scenarios using rpk default
        values.
        """
        def _get_job_ID(output):
            # By default, rpk creates a random UUID.
            job_id_pattern = r'\b([0-9a-fA-F\-]{36})\b'

            job_ids = re.findall(job_id_pattern, output)
            assert len(job_ids) > 0, f"found no job-ID in: {output}"
            assert len(
                set(job_ids)) == 1, f"Not all job IDs are the same: {job_ids}"
            return job_ids[0]

        out = self._rpk.remote_bundle_start()
        job_id = _get_job_ID(out)
        assert "The debug bundle collection process has started" in out, f"unexpected output: {out}"

        def _bundle_status(expected_status):
            all_status = self._rpk.remote_bundle_status()
            for s in all_status:
                status = s["status"]
                if status == 'error' and expected_status != 'error':
                    raise RpkException(
                        f"found error while creating a remote bundle: {s}")
                if status != expected_status:
                    return False
            return True

        wait_until(
            lambda: _bundle_status("running"),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Timed out waiting for debug bundle process to start")
        # Cancel and test that all status are "error".
        out = self._rpk.remote_bundle_cancel(job_id)
        wait_until(
            lambda: _bundle_status("error"),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Timed out waiting for debug bundle process to be canceled"
        )

        # Start a new one
        out = self._rpk.remote_bundle_start()
        job_id = _get_job_ID(out)
        assert "The debug bundle collection process has started" in out, f"unexpected output: {out}"

        wait_until(
            lambda: _bundle_status("success"),
            timeout_sec=120,
            backoff_sec=1,
            err_msg="Timed out waiting for debug bundle process to finish")

        def _validate_bundle_file(output_file):
            # Open the outer zip file (the main bundle)
            with zipfile.ZipFile(output_file) as zf:
                # Get the list of files inside the outer zip file, one per broker.
                files = zf.namelist()
                assert len(
                    files
                ) == 3, f"Downloaded zip contains less files than the number of brokers: {files}"

                for broker_file in files:
                    with zf.open(broker_file) as inner_file:
                        with zipfile.ZipFile(inner_file) as bzf:
                            filename = os.path.split(bzf.filename)[1]
                            root_name = os.path.splitext(filename)[0]
                            bfiles = bzf.namelist()
                            # We check if a sub-set of files are present:
                            assert f'{root_name}/redpanda.yaml' in bfiles, f"{root_name}/redpanda.yaml not found in zip files: {bfiles}"
                            # At least the first controller log is being saved:
                            assert f'{root_name}/controller-logs/redpanda/controller/0_0/0-1-v1.log' in bfiles, f"{root_name}/controller-logs/redpanda/controller/0_0/0-1-v1.log not found in zip files: {bfiles}"
                            # At least one cluster admin API call:
                            assert f'{root_name}/admin/brokers.json' in bfiles, f"{root_name}/admin/brokers.json not found in zip files: {bfiles}"
                            # Kafka metadata
                            assert f'{root_name}/kafka.json' in bfiles, f"{root_name}/kafka.json not found in zip files: {bfiles}"

        with tempfile.TemporaryDirectory() as td:
            output_file = os.path.join(td, "test.zip")
            out = self._rpk.remote_bundle_download(job_id,
                                                   output_file=output_file)

            _validate_bundle_file(output_file)
