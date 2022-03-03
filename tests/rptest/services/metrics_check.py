# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re


class MetricCheckFailed(Exception):
    def __init__(self, metric, old_value, new_value):
        self.metric = metric
        self.old_value = old_value
        self.new_value = new_value

    def __str__(self):
        return f"MetricCheckFailed<{self.metric} old={self.old_value} new={self.new_value}>"


class MetricCheck(object):
    def __init__(self, logger, redpanda, node, metrics, labels):
        """
        :param redpanda: a RedpandaService
        :param logger: a Logger
        :param node: a ducktape Node
        :param metrics: a list of metric names, or a single compiled regex (use re.compile())
        :param labels: dict, to filter metrics as we capture and check.
        """
        self.redpanda = redpanda
        self.node = node
        self.labels = labels
        self.logger = logger

        self._initial_samples = self._capture(metrics)

    def _capture(self, check_metrics):
        metrics = self.redpanda.metrics(self.node)

        samples = {}
        for family in metrics:
            for sample in family.samples:
                if isinstance(check_metrics, re.Pattern):
                    include = bool(check_metrics.match(sample.name))
                else:
                    include = sample.name in check_metrics

                if not include:
                    continue

                label_mismatch = False
                for k, v in self.labels.items():
                    if sample.labels.get(k, None) != v:
                        label_mismatch = True
                        continue

                if label_mismatch:
                    continue

                if sample.name in samples:
                    raise RuntimeError(
                        f"Labels {self.labels} on {sample.name} not specific enough"
                    )

                self.logger.info(
                    f"  Captured {sample.name}={sample.value} {sample.labels}")
                samples[sample.name] = sample.value

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
