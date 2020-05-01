import unittest

from .. import utils


class TestCluster(unittest.TestCase):
    def testParsingNodeId(self):
        assert utils._parse_node_id([
            "--redpanda-cfg", "conf/local_multi_node/n3.yaml",
            "--default-log-level", "info", "-c", "1", "--cpuset", "2-2",
            "--memory 6G"
        ]) == '3'
