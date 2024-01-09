# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time

from ducktape.cluster.remoteaccount import RemoteCommandError
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
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
        self.flink = FlinkService(test_context, self.redpanda, self.topic)
        # Prepare client
        config = self.redpanda.security_config()
        user = config.get("sasl_plain_username")
        passwd = config.get("sasl_plain_password")
        protocol = config.get("security_protocol", "SASL_PLAINTEXT")
        self.kafkacli = KafkaCliTools(self.redpanda,
                                      user=user,
                                      passwd=passwd,
                                      protocol=protocol)
        # Prepare Workloads
        self.workload_manager = WorkloadManager(self.logger)

        return

    def tearDown(self):
        self.kafkacli.delete_topic(self.topic)
        return super().tearDown()

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

    def _parse_csv(self, data_str, data_types):
        csv = []
        lines = data_str.splitlines()
        # Check if data_type list provides correct number of types
        # Assume all lines have same number of columns
        if len(lines[0].split(',')) != len(data_types):
            raise RuntimeError("Data types list does not match column "
                               "quantity in CSV created by Flink "
                               "transaction workload")
        for line in lines:
            values = []
            raw_values = line.split(',')
            # Process data types per item to handle transformations
            for idx in range(len(data_types)):
                # Strip data from whitespace chars
                src = raw_values[idx].strip()
                # transforms if type not equal
                if not isinstance(src, data_types[idx]):
                    # try to transform
                    try:
                        src = data_types[idx](src)
                    except Exception:
                        self.logger.warning(
                            f"Error processing CSV line '{line}' using data "
                            f"type list of '{data_types}'")
                # Save it
                values.append(src)
            csv.append(values)

        return csv

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

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Add workload config management
        _data_path = "/workloads/data"
        # Currently, each INSERT operator will generate 1 subjob
        # So this config will generate 256 / 64 jobs
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "data_path": "file://" + _data_path,
            "producer_group": "flink_group",
            "consumer_group": "flink_group",
            "topic_name": self.topic_name,
            "mode": "produce",
            "word_size": 64,
            "batch_size": 32,
            "count": 512
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

        # Load data output. There should be 1 partition file
        # with offset at the env equals to _workload_config['count']
        # Example filename:
        #     /workloads/data/part-cfbe2720-7117-40fd-8c28-fca31a459ff8-0-0
        # Data sample:
        # ...
        # 542,415,31,"2024-01-09 22:22:43.647",17,TiJJtOuzOkiOsGmws
        # ...

        # Wait for file, timeout 5 min (10 sec * 30 iterations)
        self.logger.info("Waiting for consume workload data file")
        backoff_sec = 10
        iterations = 30
        files = []
        while iterations > 0:
            # List files in data folder
            try:
                files = self.flink.node.account.ssh_output(
                    f"ls -1 {_data_path}")
                files = files.decode().splitlines()
            except RemoteCommandError:
                # Ignore ssh_output command errors
                # as folder will not be existent just yet
                continue
            # exit if file is present
            # Not that there is no need to check file name as tmp file will
            # have filename started with '.part' and it will not be listed by
            # bare 'ls -1' command
            if len(files) > 0:
                break
            self.logger.info("No data file present. "
                             f"Sleeping for {backoff_sec} sec")
            time.sleep(backoff_sec)
            iterations -= 1

        # There should be at least one file
        assert len(
            files
        ) > 0, f"Flink transaction workload produced no data files after 5 min"

        # Load data file and parse it
        data = {}
        for f in files:
            # Create record
            if f not in data:
                data[f] = []
            # This is basic test, no big data expected
            # So, no copying file locally.
            csv_data = self.flink.node.account.ssh_output(
                f"cat {_data_path}/{f}").decode()
            # Parse the csv
            # Second parameter is the quick and dirty value type map
            data[f] = self._parse_csv(csv_data, [int, int, int, str, int, str])

        # Find max offset
        offset_column = 1
        max_index = 0
        for ff, part_data in data.items():
            for row in part_data:
                if max_index < row[offset_column]:
                    max_index = row[offset_column]

        # Stop flink
        self.flink.stop()

        # 'count - 1' coz index starts with '0'
        target_index = _workload_config['count'] - 1
        assert max_index == target_index, \
            f"Flink workload consume max offset is incorrect: {max_index} " \
            f"(should be: {target_index})"

        return
