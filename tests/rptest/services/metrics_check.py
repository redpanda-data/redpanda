# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from cmath import exp
from pandasql import PandaSQL

import pandas as pd

class MetricCheck(object):
    """
    A MetricCheck spans a region of code: instantiate at the start, then
    call `evaluate` later to measure how your metrics of interest have changed
    over that region.
    """
    def __init__(self, logger, redpanda, node, sql_template, names):
        """
        :param redpanda: a RedpandaService
        :param logger: a Logger
        :param node: a ducktape Node
        :param sql_template: SQL query to evaluate over the metrics snapshot templated by {name}
        :param names: list of metric names to capture
        """
        self.redpanda = redpanda
        self.node = node
        self.logger = logger
        # Query should evaluate to single row per name and should be aliased as value
        # for evaluate() to work.
        self._sql_template = sql_template
        self._names = names
        # Initial samples in the order of names
        self._initial_samples = self._capture()


    def _capture(self):
        metrics = self.redpanda.metrics(self.node)
        samples = []
        for name in self._names:
            samples.append(PandaSQL()(self._sql_template.substitute(name=name)))

        if len(samples) == 0:
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                self.logger.warn(f"=== metrics dump for {self.node} ====")
                self.logger.warn(metrics)
                raise RuntimeError("Failed to capture metrics!")
        return samples

    def evaluate(self, expectations):
        """
        Evaluates expectations by comparing the initial and current snapshot of metrics.

        :param expectations: a list of comparators, one for each name evaluated on it's values
        :return True if the expectations are met, False otherwise
        """
        assert len(self._names) == len(expectations)
        # Capture a newer snapshot with the same metrics SQL query.
        current_samples = self._capture()
        result = True

        for comparator, samples in zip(expectations, zip(self._initial_samples, current_samples)):
            assert len(samples[0].index) == 1
            assert len(samples[1].index) == 1
            before = samples[0].at[0, "value"]
            after = samples[1].at[0, "value"]
            try:
              result = comparator(before, after)
              if not result:
                  self.logger.warn(f"Result mismatch before: {before}, after: {after}, node: {self.node.account.hostname}")
            except (TypeError, ValueError):
                self.logger.exception("Encountered error evaluating comparator. This may be legit if one of the operands is NoneType.")
                return False
        return result