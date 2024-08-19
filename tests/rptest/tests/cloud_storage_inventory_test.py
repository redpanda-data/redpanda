import base64
from pathlib import Path

from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale
from rptest.utils.si_utils import NTP


class CloudStorageInventoryTest(PreallocNodesTest):
    def __init__(self, test_context):
        self.test_context = test_context
        super().__init__(test_context=self.test_context,
                         node_prealloc_count=1,
                         num_brokers=3,
                         si_settings=SISettings(test_context=test_context))
        self.scale = Scale(self.test_context)
        self.topics = (
            TopicSpec(name='topic-A', partition_count=3, replication_factor=3),
            TopicSpec(name='topic-B', partition_count=3, replication_factor=3),
        )
        self.extra_rp_conf = {
            'cloud_storage_inventory_based_scrub_enabled': True,
            'cloud_storage_enable_scrubbing': True,
            'cloud_storage_inventory_self_managed_report_config': True,
            'cloud_storage_inventory_report_check_interval_ms': 10000
        }

        self.redpanda.set_extra_rp_conf(self.extra_rp_conf)

        # A gz CSV which contains the following rows among others for testing
        """
        "a-bucket","5ad6cd22/kafka/topic-A/110_24/626-631-1573504-1-v1.log.2"
        "a-bucket","8bb8847f/kafka/topic-A/0_24/494-499-1573504-1-v1.log.2"
        "a-bucket","bf3cd6b8/kafka/topic-B/0_24/464-469-1573504-1-v1.log.2"
        "a-bucket","d15817a9/kafka/topic-B/2_24/560-565-1573504-1-v1.log.2"
        """
        self.compressed_report = (
            "H4sICIwos2YAA3Rlc3QAjdJLTsQwDAbgPcfoGk/9ipMs4SIoaWuEBgkWA+en81hAiJQ5gD/9v+2pQP1ajttpepwQV3dGmo/Fj2U+fXy"
            "+LfA00wvrrKKgkoFClIAKBN90eP94PfD0MP02pG6RuLYG4VkJQSGEsRIWwWpqjXI1BCFIGBteYwpb24bPhlAEYf5vUGts6kuSXg5G4E"
            "6KRjBJ7o7cT0EgZGOjVGStrUF02yrnDHKeHzhxNSnEoddGcgJFGRtbQQmxvczlR0wVTMfXjS4xVPKewZaB4x1d3K2sa+z"
            "+6j6n2MnRGKnWlDS2OS630bwbedwl+YZeLP8xnm+fmvd1DFNkXCWit8KliSQDyTQ2BKkuXBqDr39KwDj"
            "+sqzFsBbuGWT7fLwjhxXRLay9Lkkgpa7wA6Qtm1GEBAAA")

        self.inventory_dir = f"{self.redpanda.DATA_DIR}/cloud_storage_inventory"
        self.inventory_path = Path(self.inventory_dir)

    def setUp(self):
        pass

    def start_redpanda(self, **kwargs):
        self.extra_rp_conf.update(kwargs)
        self.redpanda.set_extra_rp_conf(self.extra_rp_conf)
        self.redpanda.start()

    def populate_bucket(self):
        root_path = f'redpanda_scrubber_inventory/{self.si_settings.cloud_storage_bucket}/redpanda_scrubber_inventory/'
        sample_dates = {'2001-01-13T23-23Z/', '2021-01-13T23-23Z/'}
        for sample_date in sample_dates:
            manifest_json_path = f'{root_path}{sample_date}manifest.json'
            manifest_checksum_path = f'{root_path}{sample_date}manifest.checksum'
            self.cloud_storage_client.put_object(
                self.si_settings.cloud_storage_bucket, manifest_checksum_path,
                '')
            self.cloud_storage_client.put_object(
                self.si_settings.cloud_storage_bucket, manifest_json_path,
                """{"files": [{
                    "key": "report.gz"
                }]}""")
            self.cloud_storage_client.put_object(
                self.si_settings.cloud_storage_bucket,
                "report.gz",
                base64.decodebytes(self.compressed_report.encode()),
                is_bytes=True)

    @cluster(num_nodes=3)
    def test_load_inventory_report(self):
        self.start_redpanda()
        self._create_initial_topics()
        self.populate_bucket()
        wait_until(lambda: self._hashes_are_found_on_leaders(),
                   timeout_sec=60,
                   backoff_sec=5)

    def _hashes_are_found_on_leaders(self):
        found = set()
        expected = set()
        for spec in self.topics:
            for pid in range(spec.partition_count):
                expected.add(NTP(ns="kafka", topic=spec.name, partition=pid))
        for node in self.redpanda.started_nodes():
            found |= self._find_ntp_hashes_on_node(node)
        self.logger.debug(f"{found=}, {expected=}")
        return found == expected

    def _find_ntp_hashes_on_node(self, node):
        """
        This check simply collects NTP hashes from a node. It does not check if the hash is on
        the leader. Because the test runs after the hashes are created, the leader for an NTP
        may have changed, causing a mismatch in where the hash is found vs current NTP leader.
        """
        found = set()
        for row in node.account.ssh_capture(
                f"test -d {self.inventory_dir} && find {self.inventory_dir} -type f || true"
        ):
            path = Path(row.strip())
            self.logger.debug(f"found path: {path=}")

            parents = path.parents
            root, ns, topic, partition = parents[3::-1]
            assert root == self.inventory_path, (
                f"Parent of file in hash path unexpected:"
                f"{root=} != { self.inventory_path=}")

            assert path.name.isdigit(
            ), f"unexpected non-integer file name {path.name=}"

            found.add(
                NTP(ns=ns.name,
                    topic=topic.name,
                    partition=int(partition.name)))
        return found
