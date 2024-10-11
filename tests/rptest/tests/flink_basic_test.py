# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import os
import csv
import io

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.e2e_tests.workload_manager import WorkloadManager
from rptest.services.cluster import cluster
from rptest.services.flink import FlinkService
from rptest.tests.redpanda_test import RedpandaTest


class FlinkBasicTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        # Init parent
        super(FlinkBasicTests, self).__init__(test_context, log_level="trace")

        # Prepare FlinkService
        self.topic_name = "flink_workload_topic"
        self.topics = [TopicSpec(name=self.topic_name)]
        self.flink = FlinkService(test_context,
                                  self.redpanda.get_node_cpu_count(),
                                  self.redpanda.get_node_memory_mb())
        # Prepare client
        self.kafkacli = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        # Prepare Workloads
        self.workload_manager = WorkloadManager(self.logger)

        return

    def _run_workloads(self, workloads, config, wait_timeout=900):
        """
            Run workloads from the list with supplied config

            Return: list of failed jobs
        """
        # Run produce job
        for workload in workloads:
            # Add script as a job
            self.logger.info(f"Adding {workload['name']} to flink")
            _ids = self.flink.run_flink_job(workload['path'], config)
            if _ids is None:
                raise RuntimeError("Failed to run job on flink for "
                                   f"workload: {workload['name']}")

            self.logger.debug(f"Workload '{workload['name']}' "
                              f"generated {len(_ids)} "
                              f"jobs: {', '.join(_ids)}")

        # Wait for jobs to finish
        if wait_timeout > 0:
            self.flink.wait(timeout_sec=wait_timeout)

        # Collect failed jobs
        _failed = []
        _all = []
        for _id in _ids:
            _job = self.flink.get_job_by_id(_id)
            if _job['state'] == self.flink.STATE_FAILED:
                self.logger.warning(f"Job '{_id}' has failed")
                _failed.append(_job)
            _all.append(_job)

        return _all, _failed

    def _get_workloads_by_tags(self, tags):
        """
            Calls to manager to get workloads using supplied list of tags
            Raises error on nothing found

            return: list of workloads
        """
        workloads = self.workload_manager.get_workloads(tags)
        if len(workloads) < 1:
            raise RuntimeError("No workloads found "
                               f"with tags: {', '.join(tags)}")
        return workloads

    def _serialize_csv_row(self, csv_row, data_types):
        # Check if data_type list provides correct number of types
        # Assume all lines have same number of columns
        if len(csv_row) != len(data_types):
            raise RuntimeError("Data types list does not match column "
                               "quantity in CSV created by Flink "
                               "transaction workload")
        values = []
        # Process data types per item to handle transformations
        for idx in range(len(data_types)):
            # Strip data from whitespace chars
            src = csv_row[idx].strip()
            # transforms if type not equal
            if not isinstance(src, data_types[idx]):
                # try to transform
                try:
                    src = data_types[idx](src)
                except Exception:
                    self.logger.warning(
                        f"Error processing CSV line '{csv_row}' using data "
                        f"type list of '{data_types}'")
            # Save it
            values.append(src)
        # Return row as array of converted values
        return values

    @cluster(num_nodes=4)
    def test_basic_workload(self):
        """
            Test starts produce workload and then consume
            No checks for message counts, just job success
        """

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Add workload config management
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_produce_group",
            "consumer_group": "flink_consume_group",
            "topic_name": self.topic_name,
            "msg_size": 4096,
            "count": 10
        }

        # Run produce basic
        workloads = self._get_workloads_by_tags(['flink', 'produce', 'basic'])
        # Do not need list of jobs, so just discard them
        _, _failed = self._run_workloads(workloads, _workload_config)

        # Assert failed jobs
        assert len(_failed) == 0, \
            f"Flink reports failed jobs for basic produce workloads {_failed}"

        # Run consume basic
        workloads = self._get_workloads_by_tags(['flink', 'consume', 'basic'])
        # Do not need list of jobs, so just discard them
        _, _failed = self._run_workloads(workloads, _workload_config)

        # Assert failed jobs
        assert len(_failed) == 0, \
            f"Flink reports failed jobs for basic consume workload {_failed}"

        # Stop flink
        self.flink.stop()

        return

    @cluster(num_nodes=4)
    def test_transaction_workload(self):
        """
            Test uses same workload with different modes to produce
            and consume/process given number of transactions
        """
        def get_max_data_index(data_path, node):
            # Max index and target column
            index_column = 1
            max_index = 0
            for f in list_files(data_path, node):
                # Read data into memory
                csvdata = node.account.ssh_output(
                    f"cat {os.path.join(data_path, f)}")
                # Using StringsIO will eliminate file copy which proved to be unreliable
                # Also, it will load data into memory only once along with csvreader
                # processing file with csvreader will give most efficient memory
                # usage. Only one row will present in memory at all times
                # newline='' is critical, refer to: https://docs.python.org/3/library/csv.html
                with io.StringIO(csvdata.decode(), newline='') as csvfile:
                    c_reader = csv.reader(csvfile, delimiter=',')
                    for row in c_reader:
                        # Second parameter is the quick and dirty value type map
                        values = self._serialize_csv_row(
                            row, [int, int, int, str, int, str])
                        if max_index < values[index_column]:
                            max_index = values[index_column]
            return max_index

        # Load data output. There should be 1 partition file
        # with offset at the env equals to _workload_config['count']
        # Example filename:
        #     /workloads/data/part-cfbe2720-7117-40fd-8c28-fca31a459ff8-0-0
        # Data sample:
        # ...
        # 542,415,31,"2024-01-09 22:22:43.647",17,TiJJtOuzOkiOsGmws
        # ...
        def list_files(data_path, node):
            files = []
            try:
                # Note that there is no need to check file name as tmp file will
                # have filename started with '.part' and it will not be listed by
                # bare 'ls -1' command
                files = node.account.ssh_output(f"ls -1 {data_path}")
                files = files.decode().splitlines()
            except RemoteCommandError:
                # Ignore ssh_output command errors
                # as folder will not be existent just yet
                pass
            return files

        # Tunables
        random_words_dict_size = 64
        batch_size = 64
        total_events = 256

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Add workload config management
        data_path = "/workloads/data"
        # Currently, each INSERT operator will generate 1 subjob
        # So this config will generate 256 / 64 jobs
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "data_path": "file://" + data_path,
            "producer_group": "flink_group",
            "consumer_group": "flink_group",
            "topic_name": self.topic_name,
            "mode": "produce",
            "word_size": random_words_dict_size,
            "batch_size": batch_size,
            "count": total_events
        }

        # Get workload
        workloads = self._get_workloads_by_tags(
            ['flink', 'table', 'transactions', 'basic'])
        # Run produce part
        jobs, _failed = self._run_workloads(workloads, _workload_config)

        # Assert failed jobs
        _desc = [f"{j['name']} ({j['jid']}): {j['state']}" for j in _failed]
        _desc = "\n".join(_desc)
        assert len(_failed) == 0, \
            "Flink reports failed produce job for " \
            f"transaction workload:\n{_desc}"

        # Run workload in consume mode
        # Workload will run continuously, we will stop it once file is produced
        # this is why wait_timeout is 0, i.e. do not wait
        _workload_config['mode'] = 'consume'
        jobs, _failed = self._run_workloads(workloads,
                                            _workload_config,
                                            wait_timeout=0)

        # Assert failed jobs
        assert len(_failed) == 0, \
            f"Flink reports failed consume job for transaction workload {_failed}"

        # Wait for file, timeout 10 min (20 sec * 30 iterations)
        # This will make sure that consumer started working
        self.logger.info("Waiting for consumer to start writing data")
        wait_until(lambda: len(list_files(data_path, self.flink.node)) > 0,
                   timeout_sec=600,
                   backoff_sec=10,
                   err_msg="Flink transaction workload produced "
                   "no data files after 5 min")

        # make sure that there is no active jobs with 20 min timeout.
        # This big value is for safety due to overloads on docker env at
        # CDT run. Under the hood, 'flink.wait_node' which is used
        # by 'service.wait' has 'detect_idle_jobs' bool var that will check if
        # job has been completely idle for 30 sec and skip it if so.
        # To access that var, use 'wait_node' directly instead
        self.flink.wait(timeout_sec=1200)

        # Since there is a possibility that after job is finished
        # data is still be written from buffers, make sure that
        # desired index is reached. I.e. index in data files
        # has to reach target_index
        target_index = total_events - 1
        self.logger.info("Waiting for consumer to emit message "
                         f"with index {target_index}")
        wait_until(lambda: get_max_data_index(data_path, self.flink.node) ==
                   target_index,
                   timeout_sec=300,
                   backoff_sec=60,
                   err_msg="Flink transaction workload failed to consume "
                   f"{total_events} messages after 5 min")

        max_index = get_max_data_index(data_path, self.flink.node)
        # Stop flink
        self.flink.stop()

        # Assert the fail, this is for illustrative purposes and
        # probably will always pass since wait_until will fail
        # if max_index would be wrong
        assert max_index == target_index, \
            f"Flink workload consume max offset is incorrect: {max_index} " \
            f"(should be: {target_index})"

        return
