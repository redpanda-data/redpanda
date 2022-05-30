from rptest.services.redpanda import RedpandaService


class PartitionMetrics:
    def __init__(self, redpanda: RedpandaService):
        self.redpanda = redpanda

    """ Convenience class for grabbing partition metrics. """

    def bytes_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_produced_total")

    def records_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_produced")

    def bytes_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_fetched_total")

    def records_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_fetched")

    def _sum_metrics(self, metric_name):

        family = self.redpanda.metrics_sample(metric_name,
                                              nodes=self.redpanda.nodes)
        assert family, "Missing metrics."
        total = 0
        for sample in family.samples:
            total += sample.value

        self.redpanda.logger.debug(f"sum: {metric_name} - {total}")
        return total
