import re

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService, RESTART_LOG_ALLOW_LIST
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.utils import BadLogLines

STRICT_DATA_ERR_MSG_SUFFIX = "not found, is the expected filesystem mounted?"

STRICT_DATA_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    re.compile(STRICT_DATA_ERR_MSG_SUFFIX),
]


class StrictDataInitTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, extra_rp_conf={}, **kwargs)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=1, log_allow_list=STRICT_DATA_LOG_ALLOW_LIST)
    def test_strict_data_init_enabled(self):
        target_node = self.redpanda.nodes[0]

        # Enable the `storage_strict_data_init` property
        # the node should fail to restart with it enabled.
        self.rpk.cluster_config_set("storage_strict_data_init", "true")
        self.redpanda.stop_node(target_node)
        self.redpanda.start_node(target_node, expect_fail=True)

        # Verify the reason for the node not starting is what
        # we expect it to be.
        try:
            self.redpanda.raise_on_bad_logs()
        except BadLogLines as b:
            bad_lines = b.node_to_lines[target_node]
            assert any(STRICT_DATA_ERR_MSG_SUFFIX in b for b in bad_lines)
        else:
            assert False, "The reason why redpanda failed to start isn't due to a nonexistent magic file"

        # Write the empty `.redpanda_data_dir` file then start
        # the node once more. It should start this time.
        file_path = f"{RedpandaService.DATA_DIR}/.redpanda_data_dir"
        target_node.account.ssh(f"touch {file_path}")
        self.redpanda.start_node(target_node)
