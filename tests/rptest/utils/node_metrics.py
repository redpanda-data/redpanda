from math import floor

from rptest.services.redpanda import RedpandaService


class NodeMetrics:
    """ Convenience class for grabbing per-node metrics, like disk space. """
    def __init__(self, redpanda: RedpandaService):
        self.redpanda = redpanda

    def _get_metrics_vals(self, name_substr: str) -> list[float]:
        family = self.redpanda.metrics_sample(name_substr)
        assert family
        return list(map(lambda s: floor(s.value), family.samples))

    def disk_total_bytes(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_total_bytes")

    def disk_free_bytes(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_free_bytes")

    def disk_space_alert(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_free_space_alert")
