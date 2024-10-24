from logging import Logger
import random

from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService
from rptest.services.admin import Admin


class FullDiskHelper:
    CONF_MIN_FREE_BYTES = "storage_min_free_bytes"

    def __init__(self, logger: Logger, redpanda: RedpandaService):
        self.logger = logger
        self.redpanda = redpanda
        self.admin = Admin(redpanda)

    # TODO factor out similar code in cluster_config_test.py
    def _wait_for_node_config_value(self, key: str, value: int) -> None:
        _get = lambda k: self.admin.get_cluster_config()[k]

        def match(key: str, val: int) -> bool:
            v = _get(key)
            self.logger.info(
                f"config wait: want {type(value)} {value}, got {v}" +
                f" {type(v)}")
            return v == value

        wait_until(lambda: match(key, value), timeout_sec=15)
        self.logger.debug(f"Verified val {value} for {key}.")

    def _set_low_space(self, degraded, *, node=None):
        victim = node or random.choice(self.redpanda.nodes)
        new_threshold: int = 0
        if degraded:
            new_threshold = 2**60  # arbitrary huge value
        else:
            new_threshold = 2**30  # smallest supported value
        updates = {self.CONF_MIN_FREE_BYTES: new_threshold}
        self.logger.debug(f" victim {victim.name}, " +
                          f"new_thresh {new_threshold}")
        self.redpanda.set_cluster_config(updates)

        # self.admin.patch_cluster_config(upsert=updates, remove=[])
        self.logger.debug(f"Confirming new config values..")
        self._wait_for_node_config_value(self.CONF_MIN_FREE_BYTES,
                                         new_threshold)

    def trigger_low_space(self, node=None):
        self._set_low_space(True, node=node)

    def clear_low_space(self):
        self._set_low_space(False)
