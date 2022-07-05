# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.services.redpanda import MetricsEndpoint


class MetricCheckFailed(Exception):
    def __init__(self, metric, old_value, new_value):
        self.metric = metric
        self.old_value = old_value
        self.new_value = new_value

    def __str__(self):
        return f"MetricCheckFailed<{self.metric} old={self.old_value} new={self.new_value}>"


class MetricCheck(object):
    """
    A MetricCheck spans a region of code: instantiate at the start, then
    call `expect` or `evaluate` later to measure how your metrics of
    interest have changed over that region.
    """
    def __init__(self,
                 logger,
                 redpanda,
                 node,
                 metrics,
                 labels=None,
                 reduce=None,
                 metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS):
        """
        :param redpanda: a RedpandaService
        :param logger: a Logger
        :param node: a ducktape Node
        :param metrics: a list of metric names, or a single compiled regex (use re.compile())
        :param labels: dict, to filter metrics as we capture and check.
        :param reduce: reduction function (e.g. sum) if multiple samples match metrics+labels
        :param metrics_endpoint: MetricsEndpoint enumeration instance specifies which
        Prometheus endpoint to query
        """
        self.redpanda = redpanda
        self.node = node
        self.labels = labels
        self.logger = logger

        self._reduce = reduce
        self._metrics_endpoint = metrics_endpoint
        self._initial_samples = self._capture(metrics)

    def _capture(self, check_metrics):
        metrics = self.redpanda.metrics(self.node, self._metrics_endpoint)

        samples = {}
        for family in metrics:
            for sample in family.samples:
                if isinstance(check_metrics, re.Pattern):
                    include = bool(check_metrics.match(sample.name))
                else:
                    include = sample.name in check_metrics

                if not include:
                    continue

                if self.labels:
                    label_mismatch = False
                    for k, v in self.labels.items():
                        if sample.labels.get(k, None) != v:
                            label_mismatch = True
                            continue

                    if label_mismatch:
                        continue

                self.logger.info(
                    f"  Read {sample.name}={sample.value} {sample.labels}")
                if sample.name in samples:
                    if self._reduce is None:
                        raise RuntimeError(
                            f"Labels {self.labels} on {sample.name} not specific enough"
                        )
                    else:
                        samples[sample.name] = self._reduce(
                            [samples[sample.name], sample.value])

                else:
                    samples[sample.name] = sample.value

        for k, v in samples.items():
            self.logger.info(f"  Captured {k}={v}")

        if len(samples) == 0:
            metrics_endpoint = ("/metrics" if self._metrics_endpoint
                                == MetricsEndpoint.METRICS else
                                "/public_metrics")
            url = f"http://{self.node.account.hostname}:9644{metrics_endpoint}"
            import requests
            dump = requests.get(url).text
            self.logger.warn(f"Metrics dump: {dump}")
            raise RuntimeError("Failed to capture metrics!")

        return samples

    def expect(self, expectations):
        # Gather current values for all the metrics we are going to
        # apply expectations to (this may be a subset of the metrics
        # we originally gathered at construction time).
        samples = self._capture([e[0] for e in expectations])

        error = None
        for (metric, comparator) in expectations:
            try:
                old_value = self._initial_samples[metric]
            except KeyError:
                self.logger.error(
                    f"Missing metrics {metric} on {self.node.account.hostname}.  Have metrics: {list(self._initial_samples.keys())}"
                )
                raise

            new_value = samples[metric]
            ok = comparator(old_value, new_value)
            if not ok:
                error = MetricCheckFailed(metric, old_value, new_value)
                # Log each bad metric, raise the last one as our actual exception below
                self.logger.error(str(error))

        if error:
            raise error

    def evaluate(self, expectations):
        """
        Similar to `expect`, but instead of asserting the expections are
        true, just evaluate whether they are and return a boolean.
        """
        samples = self._capture([e[0] for e in expectations])
        for (metric, comparator) in expectations:
            old_value = self._initial_samples.get(metric, None)
            if old_value is None:
                return False

            new_value = samples[metric]
            if not comparator(old_value, new_value):
                return False

        return True
