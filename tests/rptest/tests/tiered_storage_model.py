# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections.abc import Callable
from typing import Any, List, Optional, Dict
from functools import reduce
from collections import defaultdict
import z3
from threading import Lock
from abc import ABC, abstractclassmethod
import os
import sys
import random

from enum import Enum
from logging import Logger
import logging

TestRunStage = Enum(
    "TestRunStage",
    ["Startup", "Produce", "Intermediate", "Consume", "Shutdown"])

CONFIDENCE_THRESHOLD = 0.8
FAKE_BASE_TIMESTAMP_MS = 1690000000000
FAKE_TIMESTAMP_STEP_MS = 1
LOG_SEGMENT_SIZE = 1024 * 1024
LARGE_SEGMENT_SIZE = 3 * LOG_SEGMENT_SIZE
LOCAL_TARGET_SIZE = LARGE_SEGMENT_SIZE
SPILLOVER_MANIFEST_SIZE = 5
LOW_THRESHOLD = 1


class TieredStorageEndToEndTest(ABC):
    """Tiered storage interface which is consumed by
    validators and inputs."""
    @abstractclassmethod
    def set_redpanda_cluster_config(self,
                                    config_name: str,
                                    config_value: Any | None,
                                    needs_restart: bool = False):
        """Set redpanda cluster configuration.
        param config_name: name of the configuration parameter
        param config_value: value of the configuration parameter or None if the parameter should be removed
        param needs_restart: True if the configuration change requires restart"""
        pass

    @abstractclassmethod
    def get_redpanda_service(self):
        pass

    @abstractclassmethod
    def get_bucket_view(self):
        pass

    @abstractclassmethod
    def get_ntp(self):
        pass

    @abstractclassmethod
    def get_logger(self) -> Logger:
        pass

    @abstractclassmethod
    def get_public_metric(self, metric_name: str):
        """Get metric value or None if the metric is not available"""
        pass

    @abstractclassmethod
    def get_private_metric(self, metric_name: str):
        """Get metric value or None if the metric is not available"""
        pass

    @abstractclassmethod
    def alter_topic_config(self, config_name: str, config_value: str):
        """Change topic configuration using rpk"""
        pass

    @abstractclassmethod
    def subscribe_to_logs(self, validator, pattern):
        """Subscribe to all log changes on all nodes. The validator will be invoked
        when the log line is matched."""
        pass

    @abstractclassmethod
    def set_consumer_parameters(self, **kwargs):
        pass


class Model:
    """Tiered-storage model expressed in Z3 terms.
    Model manages all conditions and relationships between them.
    Once the test run is solved its job is to translate from the
    representation used by Z3 to the actual objects."""

    __default = None  # default model instance used by all tests

    def __init__(self):
        self.__solver = z3.Solver()
        self.__table = {}
        self.__curr_expr = []

    @classmethod
    def default(cls):
        if cls.__default is None:
            cls.__default = cls()
        return cls.__default

    def _register(self, cond):
        self.__table[cond.name] = cond

    def add(self, s_expr):
        self.__curr_expr.append(s_expr)

    def solve_for(self, *s_expr_list):
        print(f"solving for {s_expr_list}")
        results = []
        args = [s for s in s_expr_list] + self.__curr_expr
        self.__solver.push()
        self.__solver.add(z3.And(*args))
        try:
            if self.__solver.check() == z3.sat:
                m = self.__solver.model()
                for d in m.decls():
                    if str(d) in self.__table:
                        print(f"Model parameter {d} = {m[d]}")
                        # Only add the expression if it's a boolean set to True
                        # The validators can only check that something is present. The
                        # input modifiers can only enable something which is disabled by
                        # default. This simplifies the model a lot.
                        if m[d] == True:
                            results.append(self.__table[str(d)])
            else:
                raise ValueError("Unsatisfiable")
        finally:
            # Backtrack
            self.__solver.pop()
        return str(s_expr_list), results

    def find_all_solutions(self, parameters):
        def get_combination(ix):
            result = []
            for p in parameters:
                val = p[ix % len(p)]
                result.append(val)
                ix = ix // len(p)
            return result

        total_combinations = reduce(lambda a, b: a * b,
                                    [len(p) for p in parameters])
        print(f"total {total_combinations} combinations")
        results = []
        for ix in range(0, total_combinations):
            comb = get_combination(ix)
            print(f"solving {ix}")
            results.append(self.solve_for(*comb))
        return results


class TestInput(ABC):
    """Base class for all test state modifiers. Test inputs include
    produced data, configuration parameters, topic configuration
    parameters, etc. It can also be used to represent things which
    are applied during test runtime. Things like failure injection and
    random process restarts.
    """
    @abstractclassmethod
    def name(self) -> str:
        """Get name of the input mutator"""
        pass

    @abstractclassmethod
    def start(self, test: TieredStorageEndToEndTest):
        """Initialize input before the first use"""
        pass

    @abstractclassmethod
    def run(self, test: TieredStorageEndToEndTest):
        """Apply input (should only be called if need_to_run returns True)"""
        pass

    @abstractclassmethod
    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the input need to be applied during this test execution stage"""
        pass


class EffectValidator(ABC):
    """Base class for all test effect validators. Example of an effect is
    a segment or manifest upload. Each effect can be validated by sampling
    the state of the test periodically. For instance, the validator may
    check the metric or look for the log messages.

    The validator is stateful. Each observation changes its state. The end result
    is produced when the validator has observed the even enough times. The
    validator exposes two parameters: the result which can be True or False and the
    confidence level which is a probability in 0.0 to 1.0 range.

    Some validators can only run in certain stages of the test (produce or consume).
    These validators have to check the status of the test internally. The test is
    expected to be passed in the c-tor.

    The validator can interfere with the test by prolonging certain test stages (this
    is why it's called "validator" and not "observer"). For instance, the validator
    may require test to run until the confidence reaches certain level.
    """
    @abstractclassmethod
    def name(self) -> str:
        """Get name of the validator"""
        pass

    @abstractclassmethod
    def start(self, test: TieredStorageEndToEndTest):
        pass

    @abstractclassmethod
    def run(self, test: TieredStorageEndToEndTest):
        """Run checks incrementally"""
        pass

    @abstractclassmethod
    def get_result(self) -> Optional[bool]:
        """Return True if the effect is validated to be present or False if it's
        validated to be not present. Return None if not enough information was
        gathered."""
        pass

    @abstractclassmethod
    def get_confidence(self) -> float:
        """Get confidence level as value in [0-1] range"""
        pass

    @abstractclassmethod
    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the validator need to run during this test execution stage"""
        pass


ExpressionType = Enum("ExpressionType", ["Bool", "Int", "Offset"])


def clamp(value, inclusive_lower_bound, inclusive_upper_bound):
    assert (inclusive_lower_bound < inclusive_upper_bound)
    return max(min(value, inclusive_upper_bound), inclusive_lower_bound)


class Expression:
    """Base class for both inputs and effects"""
    def __init__(self,
                 model: Model,
                 name: str,
                 _type: ExpressionType,
                 maybe_comment: Optional[str] = None):
        self.__name = name
        self.__type = _type
        if self.__type == ExpressionType.Bool:
            self.__expr = z3.Bool(name)
        elif self.__type == ExpressionType.Int:
            self.__expr = z3.Int(name)
        elif self.__type == ExpressionType.Offset:
            self.__expr = z3.Int(name)
        self.__comment = maybe_comment
        model._register(self)

    @property
    def name(self):
        return self.__name

    @property
    def expr(self):
        return self.__expr

    def __call__(self):
        return self.__expr

    @property
    def type_str(self):
        return self.__type

    @property
    def comment(self):
        return self.__comment

    def requires(self, *s_expr_list):
        """Request some preconditions to be met
        Example:
          ts_read.requires(cache_read == True)
        Method returns s-expression that should be added
        to the model explicitly.
        Example:
          se = ts_read.requires(cache_read == True)
          model.add(se)
        """
        # Invariant: the s_expression should be boolean
        s_expr = z3.And(
            [z3.Implies(self.__expr == True, se) for se in s_expr_list])
        return s_expr

    def inv_requires(self, *s_expr_list):
        """Similar to 'requires' but current condition is flipped"""
        s_expr = z3.And(
            [z3.Implies(self.__expr == False, se) for se in s_expr_list])
        return s_expr

    def __str__(self):
        short_str = None
        short_len = 48
        if self.__comment is None:
            short_str = f"{self.__name}:{self.__type}"
        else:
            short_str = f"{self.__name}:{self.__type}:{self.__comment}"
        if len(short_str) < short_len:
            short_str = short_str.ljust(short_len, '.')
            return f"Expr[{short_str}]"
        short_str = short_str[0:(short_len - 1)]
        short_str = short_str.ljust(short_len, '>')
        return f"Expr[{short_str}]"

    def __eq__(self, val):
        if isinstance(val, Expression):
            return self.__expr == val()
        return self.__expr == val

    def __lt__(self, val):
        if isinstance(val, Expression):
            return self.__expr < val()
        return self.__expr < val

    def __gt__(self, val):
        if isinstance(val, Expression):
            return self.__expr > val()
        return self.__expr > val

    def __le__(self, val):
        if isinstance(val, Expression):
            return self.__expr <= val()
        return self.__expr <= val

    def __ge__(self, val):
        if isinstance(val, Expression):
            return self.__expr >= val()
        return self.__expr >= val

    def __ne__(self, val):
        if isinstance(val, Expression):
            return self.__expr != val()
        return self.__expr != val

    def __truediv__(self, val):
        if isinstance(val, Expression):
            return self.__expr / val()
        return self.__expr / val

    def make_inputs(self) -> List[TestInput]:
        """Create set of input for the test"""
        return []

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        return []


class LogBasedValidator(EffectValidator):
    """Checks that the object is uploaded to S3"""
    def __init__(self,
                 name,
                 pattern,
                 confidence_threshold=8,
                 execution_stage=TestRunStage.Produce):
        self.__confidence_threshold = confidence_threshold
        self.__name = name
        self.__num_matches = 0
        self.__confidence = 0.0
        self.__pattern = pattern
        self.__exec_stage = execution_stage
        self.__logger = None

    def start(self, test: TieredStorageEndToEndTest):
        """Set initial state using the test instance"""
        self.__logger = test.get_logger()
        test.get_logger().info(
            f"starting validator {self.__name}, pattern {self.__pattern}, threshold {self.__confidence_threshold}"
        )
        test.subscribe_to_logs(self, self.__pattern)

    def on_match(self, node_name, line):
        self.__logger.debug(
            f"Validator {self.__name} got matching log line {line} on node {node_name}"
        )
        self.__num_matches += 1
        self.__confidence = clamp(
            self.__num_matches / self.__confidence_threshold, 0.0, 1.0)

    def name(self) -> str:
        """Get name of the validator"""
        return self.__name

    def run(self, test: TieredStorageEndToEndTest):
        # This validator gets updated by the test runner through
        # the callback
        pass

    def get_result(self) -> Optional[bool]:
        """Return True if the effect is validated to be present or False if it's
        validated to be not present. Return None if not enough information was
        gathered."""
        return self.__num_matches > 0

    def get_confidence(self) -> float:
        """Get confidence level as value in [0-1] range"""
        return self.__confidence

    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the validator need to run during this test execution stage"""
        return stage == self.__exec_stage


class LogUniquenessValidator(EffectValidator):
    lock = Lock()
    """Checks that the object is uploaded to S3"""
    def __init__(self,
                 name,
                 pattern,
                 confidence_threshold=8,
                 execution_stage=TestRunStage.Produce):
        self.__confidence_threshold = confidence_threshold
        self.__name = name
        self.__matches = defaultdict(set)
        self.__duplicates = 0
        self.__confidence = 0.0
        self.__pattern = pattern
        self.__exec_stage = execution_stage
        self.__logger = None
        self.__subscribed = False

    def start(self, test: TieredStorageEndToEndTest):
        """Set initial state using the test instance"""
        with LogUniquenessValidator.lock:
            assert self.__subscribed == False, f"start() called twice for {self.__name}"
            self.__logger = test.get_logger()
            test.get_logger().info(
                f"starting validator {self.__name}, pattern {self.__pattern}, threshold {self.__confidence_threshold}"
            )
            test.subscribe_to_logs(self, self.__pattern)
            self.__subscribed = True

    def on_match(self, node_name, line):
        with LogUniquenessValidator.lock:
            orig = line
            line = line[30:]  # Remove timestamp and log-level

            if line not in self.__matches[node_name]:
                self.__logger.debug(
                    f"Validator {self.__name} got matching log line \"{orig}\" from {node_name}"
                )
                if self.__duplicates > 0:
                    # The validator has seen duplicates already. No need to continue collecting.
                    return
                self.__matches[node_name].add(line)
                total_matches = sum([len(v) for v in self.__matches.values()])
                self.__confidence = clamp(
                    total_matches / self.__confidence_threshold, 0.0, 1.0)
                return

            self.__logger.error(
                f"Validator {self.__name} got duplicate log line \"{orig}\" from {node_name}"
            )
            lines = self.__matches.get(node_name) or []
            for l in lines:
                self.__logger.error(f"    previous entry: {l}")
            self.__confidence = 1.0
            self.__duplicates += 1

    def name(self) -> str:
        """Get name of the validator"""
        return self.__name

    def run(self, test: TieredStorageEndToEndTest):
        # This validator gets updated by the test runner through
        # the callback
        pass

    def get_result(self) -> Optional[bool]:
        """Return True if the effect is validated to be present or False if it's
        validated to be not present. Return None if not enough information was
        gathered."""
        with LogUniquenessValidator.lock:
            return len(self.__matches) > 0 and self.__duplicates == 0

    def get_confidence(self) -> float:
        """Get confidence level as value in [0-1] range"""
        with LogUniquenessValidator.lock:
            return self.__confidence

    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the validator need to run during this test execution stage"""
        return stage == self.__exec_stage


class MetricBasedValidator(EffectValidator):
    """Checks that the object is uploaded to S3"""
    def __init__(self,
                 name,
                 metric_name,
                 confidence_threshold=8,
                 execution_stage=TestRunStage.Produce):
        self.__confidence_threshold = confidence_threshold
        self.__name = name
        self.__num_checks = 0
        self.__baseline = None
        self.__confidence = 0.0
        self.__result = False
        self.__metric_name = metric_name
        self.__exec_stage = execution_stage

    def start(self, test: TieredStorageEndToEndTest):
        """Set initial state using the test instance"""
        test.get_logger().info(
            f"starting validator {self.__name}, metric {self.__metric_name}, threshold {self.__confidence_threshold}"
        )

    def __get_metric(self, test: TieredStorageEndToEndTest):
        """Fetch the metric from the table of metrics using test instance"""
        metric = test.get_public_metric(self.__metric_name)
        if metric is None:
            metric = test.get_private_metric(self.__metric_name)
        test.get_logger().debug(
            f"metric {self.__metric_name} is equal to {metric}")
        return metric

    def name(self) -> str:
        """Get name of the validator"""
        return self.__name

    def run(self, test: TieredStorageEndToEndTest):
        metric_value = self.__get_metric(test)
        if metric_value is None:
            # Metric is not produced yet, the baseline hast
            # to be set to None in this case.
            return

        if self.__baseline is None:
            # This is the first time we see the metric value. Use it as a baseline.
            self.__baseline = metric_value

        delta = metric_value - self.__baseline
        self.__num_checks += 1
        # Require at least one upload for the positive result
        self.__result = delta > 0
        # Recompute confidence level based on new observation
        self.__confidence = clamp(delta / self.__confidence_threshold, 0.0,
                                  1.0)

    def get_result(self) -> Optional[bool]:
        """Return True if the effect is validated to be present or False if it's
        validated to be not present. Return None if not enough information was
        gathered."""
        return self.__result

    def get_confidence(self) -> float:
        """Get confidence level as value in [0-1] range"""
        return self.__confidence

    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the validator need to run during this test execution stage"""
        return stage == self.__exec_stage


class BucketBasedValidator(EffectValidator):
    """Validator that uses bucket state to validate the effect"""
    def __init__(self, name, execution_stage=TestRunStage.Produce):
        self.__name = name
        self.__num_checks = 0
        self.__result = False
        self.__exec_stage = execution_stage

    def start(self, test: TieredStorageEndToEndTest):
        """Set initial state using the test instance"""
        test.get_logger().info(
            f"starting bucket based validator {self.__name}")

    def name(self) -> str:
        """Get name of the validator"""
        return self.__name

    def _do_check(self, bucket_view, ntp, logger=None) -> bool:
        return False

    def run(self, test: TieredStorageEndToEndTest):
        if self.__result == True:
            return
        bucket_view = test.get_bucket_view()
        if bucket_view is None:
            return
        ntp = test.get_ntp()
        res = self._do_check(bucket_view, ntp, test.get_logger())
        self.__num_checks += 1
        # Require at least one upload for the positive result
        self.__result = res

    def get_result(self) -> Optional[bool]:
        """Return True if the effect is validated to be present or False if it's
        validated to be not present. Return None if not enough information was
        gathered."""
        return self.__result

    def get_confidence(self) -> float:
        """Bucket validator always gives certain result because it's checking the
        end state and not counting events."""
        return 1.0 if self.__num_checks > 0 else 0.0

    def need_to_run(self, stage: TestRunStage) -> bool:
        """Return True if the validator need to run during this test execution stage"""
        return stage == self.__exec_stage


class TopicConfigInput(TestInput):
    def __init__(self, name, config, value, stage=TestRunStage.Startup):
        self.__name = name
        self.__config = config
        self.__value = value
        self.__stage = stage

    def name(self) -> str:
        return self.__name

    def start(self, test: TieredStorageEndToEndTest):
        pass

    def run(self, test: TieredStorageEndToEndTest):
        test.alter_topic_config(self.__config, self.__value)

    def need_to_run(self, stage: TestRunStage) -> bool:
        return stage == self.__stage


class ClusterConfigInput(TestInput):
    def __init__(self,
                 name,
                 configs: Dict[str, str],
                 stage=TestRunStage.Startup,
                 restart_required=False):
        self.__name = name
        self.__configs = configs
        self.__stage = stage
        self.__restart_required = restart_required

    def name(self) -> str:
        return self.__name

    def run(self, test: TieredStorageEndToEndTest):
        for config, value in self.__configs.items():
            test.set_redpanda_cluster_config(
                config, value, needs_restart=self.__restart_required)

    def start(self, test: TieredStorageEndToEndTest):
        pass

    def need_to_run(self, stage: TestRunStage) -> bool:
        return stage == self.__stage


class RandomizedClusterConfigInput(TestInput):
    def __init__(self, name, stages, configs: List[Dict[str, str]]):
        self.__name = name
        self.__configs = configs
        self.__stages = stages

    def name(self) -> str:
        return self.__name

    def start(self, test: TieredStorageEndToEndTest):
        choice = random.choice(self.__configs)
        for config, value in choice.items():
            test.set_redpanda_cluster_config(config, value)

    def run(self, test: TieredStorageEndToEndTest):
        choice = random.choice(self.__configs)
        for config, value in choice.items():
            test.set_redpanda_cluster_config(config, value)

    def need_to_run(self, stage: TestRunStage) -> bool:
        return stage in self.__stages


class ConsumerInput(TestInput):
    def __init__(self, name, params):
        self.__name = name
        self.__params = params

    def name(self) -> str:
        return self.__name

    def start(self, test: TieredStorageEndToEndTest):
        test.set_consumer_parameters(**self.__params)

    def run(self, test: TieredStorageEndToEndTest):
        pass

    def need_to_run(self, stage: TestRunStage) -> bool:
        return stage == TestRunStage.Consume


class ProducerInput(TestInput):
    def __init__(self, name, params):
        self.__name = name
        self.__params = params

    def name(self) -> str:
        return self.__name

    def start(self, test: TieredStorageEndToEndTest):
        test.get_logger().info(
            f"{self.name} setting producer parameters {self.__params}")
        test.set_producer_parameters(**self.__params)

    def run(self, test: TieredStorageEndToEndTest):
        pass

    def need_to_run(self, stage: TestRunStage) -> bool:
        return stage == TestRunStage.Produce


class TS_Write(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Write", ExpressionType.Bool,
                         "Segment is uploaded to S3")

    class Bucket_validator(BucketBasedValidator):
        def __init__(self):
            super().__init__("TS_Bucket_Write")

        def _do_check(self, bucket_view, ntp, logger) -> bool:
            return bucket_view.segment_objects > 0

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        return [
            MetricBasedValidator(
                "TS_Write",
                "vectorized_cloud_storage_successful_uploads_total",
                execution_stage=TestRunStage.Produce),
            LogBasedValidator(
                "TS_STM_Write",
                "Add segment command applied.*is_compacted: false",
                execution_stage=TestRunStage.Produce),
            TS_Write.Bucket_validator(),
            # account.ssh_capture('tail -f') gives us false positives
            # LogUniquenessValidator("TS_NoDuplicates",
            #                        "Add segment command applied with",
            #                        execution_stage=TestRunStage.Produce),
        ]


class TS_Read(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Read", ExpressionType.Bool,
                         "Segment is downloaded from S3")

    def make_validators(self) -> List[EffectValidator]:
        return [
            MetricBasedValidator(
                "TS_Read",
                "vectorized_cloud_storage_successful_downloads_total",
                confidence_threshold=4,
                execution_stage=TestRunStage.Consume)
        ]


class SegmentRoll(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "SegmentRoll", ExpressionType.Bool,
                         "Segment is rolled")

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        # TODO: implement log based validator
        return [
            MetricBasedValidator(
                "SegmentRoll",
                "vectorized_storage_log_log_segments_created_total",
                execution_stage=TestRunStage.Produce)
        ]


class SegmentsRemoved(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "SegmentsRemoved", ExpressionType.Bool,
                         "Segments are removed from local storage")

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        return [
            MetricBasedValidator(
                "SegmentsRemoved",
                "vectorized_storage_log_log_segments_removed_total",
                execution_stage=TestRunStage.Intermediate)
        ]


class ApplySpilloverTriggered(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "SpilloverHousekeepingTriggered", ExpressionType.Bool,
            "Housekeeping triggered spillover in the ntp_archiver")

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        return [
            LogBasedValidator("TS_HousekeepingSpillover",
                              "Spillover command applied",
                              confidence_threshold=LOW_THRESHOLD)
        ]


class TS_ManifestUploaded(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_ManifestUploaded", ExpressionType.Bool,
                         "Manifest is uploaded to S3")

    def make_validators(self) -> List[EffectValidator]:
        return [
            MetricBasedValidator(
                "TS_ManifestUploaded_metric",
                "vectorized_cloud_storage_partition_manifest_uploads_total")
        ]


class SpilloverManifestUploaded(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "SpilloverManifestUploaded",
                         ExpressionType.Bool, "Spillover manifest is uploaded")

    class Bucket_validator(BucketBasedValidator):
        def __init__(self):
            super().__init__("SpilloverManifestUploaded_bucket")

        def _do_check(self, bucket_view, ntp, logger):
            spm = bucket_view.get_spillover_manifests(ntp)
            return spm is not None

    def make_validators(self) -> List[EffectValidator]:
        """Create set of effect observers to run while the data is produced or consumed"""
        return [
            MetricBasedValidator(
                "SpilloverManifestUpload",
                "redpanda_cloud_storage_spillover_manifest_uploads_total",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Produce),
            LogBasedValidator("TS_SpilloverManifestUploaded",
                              "Uploaded spillover manifest",
                              confidence_threshold=LOW_THRESHOLD),
            # Check that the spillover manifests are uploaded to the bucket for
            # the ntp
            SpilloverManifestUploaded.Bucket_validator(),
        ]


class AdjacentSegmentMergerReupload(Expression):
    """Checks that adjacent segment reupload is triggered"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "AdjacentSegmentMergerReupload",
                         ExpressionType.Bool,
                         "Adjacent segment reupload is triggered")

    def make_validators(self) -> List[EffectValidator]:
        """Create set of validators"""
        return [
            LogBasedValidator(
                "AdjacentSegmentMergerReupload",
                "adjacent_segment_merger.*Found adjacent segment run",
                confidence_threshold=LOW_THRESHOLD)
        ]


class CompactedReupload(Expression):
    """Checks that compacted segment reupload is triggered"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "CompactedSegmentReupload",
                         ExpressionType.Bool,
                         "Compacted segment reupload is triggered")

    class Bucket_validator(BucketBasedValidator):
        def __init__(self):
            super().__init__("CompactedSegmentReupload_bucket")

        def _do_check(self, bucket_view, ntp, logger) -> bool:
            num_compacted = 0
            manifest = bucket_view.partition_manifests[ntp]
            for _, sm in manifest['segments'].items():
                if sm['is_compacted'] == True:
                    num_compacted += 1
            return num_compacted > 0

    def make_validators(self) -> List[EffectValidator]:
        """Create set of validators"""
        return [
            LogBasedValidator("CompactedSegmentReupload_CandidateFound",
                              "segment_reupload.*Found segment for ntp",
                              confidence_threshold=LOW_THRESHOLD),
            LogBasedValidator(
                "CompactedSegmentReupload_STM_Updated",
                "Add segment command applied.*is_compacted: true",
                confidence_threshold=LOW_THRESHOLD),
            CompactedReupload.Bucket_validator(),
        ]


class TopicCompactionEnabled(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TopicCompactionEnabled", ExpressionType.Bool,
                         "Topic compaction is enabled")

    def make_inputs(self) -> List[TestInput]:
        return [
            TopicConfigInput("TopicCompactionEnabled_topic_config",
                             "cleanup.policy", "compact,delete"),
            ProducerInput("TopicCompactionEnable_producer_config", {
                "key_set_cardinality": 250,
                "msg_count": 200000
            }),
            ClusterConfigInput(
                "TopicCompactionEnabled_rp_config",
                dict(max_compacted_log_segment_size=LOG_SEGMENT_SIZE))
        ]


class RemoteWriteTopicConfig(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "RemoteWriteTopicConfig", ExpressionType.Bool,
                         "redpanda.remote.write topic property is set")

    def make_inputs(self) -> List[TestInput]:
        return [
            TopicConfigInput("RemoteWriteTopicConfig", "redpanda.remote.write",
                             "true")
        ]


class RemoteReadTopicConfig(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "RemoteReadTopicConfig", ExpressionType.Bool,
                         "redpanda.remote.read topic property is set")

    def make_inputs(self) -> List[TestInput]:
        return [
            TopicConfigInput("RemoteReadTopicConfig", "redpanda.remote.read",
                             "true")
        ]


class RemoteWriteClusterConfig(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "RemoteWriteClusterConfig",
                         ExpressionType.Bool,
                         "cloud_storage_enable_remote_write config is set")

    def make_inputs(self) -> List[TestInput]:
        return [
            ClusterConfigInput("RemoteWriteClusterConfig",
                               {"cloud_storage_enable_remote_write": "true"})
        ]


class RemoteReadClusterConfig(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "RemoteReadClusterConfig", ExpressionType.Bool,
                         "cloud_storage_enable_remote_read config is set")

    def make_inputs(self) -> List[TestInput]:
        return [
            ClusterConfigInput("RemoteReadClusterConfig",
                               {"cloud_storage_enable_remote_read": "true"})
        ]


class ShortLocalRetentionTopicConfig(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "ShortLocalRetentionTopicConfig", ExpressionType.Bool,
            "retention.local.target.bytes config is set to small value (1 segment size)"
        )

    def make_inputs(self) -> List[TestInput]:
        return [
            TopicConfigInput(
                "ShortLocalRetentionTopicConfig",
                # Local retention should be long enough to allow segment merging
                # but low enough to allow remote reads to happen
                "retention.local.target.bytes",
                str(LOCAL_TARGET_SIZE),
                stage=TestRunStage.Intermediate)
        ]


class EnableSpillover(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "EnableSpillover", ExpressionType.Bool,
            "Spillover is enabled by setting cloud_storage_spillover_manifest_max_segments to small value"
        )

    def make_inputs(self) -> List[TestInput]:
        return [
            ClusterConfigInput(
                "EnableSpillover",
                dict(cloud_storage_spillover_manifest_size=None,
                     cloud_storage_spillover_manifest_max_segments=
                     SPILLOVER_MANIFEST_SIZE))
        ]


class TS_Housekeeping(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "TS_Housekeeping", ExpressionType.Bool,
            "Housekeeping is enabled by setting cloud_storage_housekeeping_interval_ms to small value"
        )

    def make_inputs(self) -> List[TestInput]:
        return [
            ClusterConfigInput(
                "TS_Housekeeping",
                {"cloud_storage_housekeeping_interval_ms": "2000"})
        ]


class LocalStorageHousekeeping(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "LocalStorageHousekeeping",
                         ExpressionType.Bool,
                         "Housekeeping in the local storage is triggered")

    def make_validators(self) -> List[EffectValidator]:
        return [
            LogBasedValidator("LocalStorageHousekeeping",
                              "house keeping with configuration from manager",
                              confidence_threshold=LOW_THRESHOLD)
        ]


class EnableAdjacentSegmentMerging(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "EnableAdjacentSegmentMerging",
                         ExpressionType.Bool,
                         "Adjacent segment merging is enabled")

    def make_inputs(self):
        return [
            ClusterConfigInput(
                "EnableAdjacentSegmentMerging",
                dict(cloud_storage_enable_segment_merging=True,
                     cloud_storage_segment_size_target=LARGE_SEGMENT_SIZE))
        ]


class EnableCompactedSegmentReupload(Expression):
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "EnableCompactedSegmentReupload",
                         ExpressionType.Bool,
                         "Compacted segment reupload is enabled")

    def make_inputs(self):
        return [
            ClusterConfigInput(
                "EnableCompactedSegmentReupload",
                dict(cloud_storage_enable_compacted_topic_reupload=True))
        ]


class SegmentSelfCompaction(Expression):
    """Segment was self compacted in the local storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "SegmentSelfCompaction", ExpressionType.Bool,
                         "Segment was self compacted")

    def make_validators(self):
        return [
            LogBasedValidator("SegmentSelfCompaction",
                              "segment .* compaction result: .*",
                              confidence_threshold=LOW_THRESHOLD)
        ]


class AdjacentSegmentCompaction(Expression):
    """Adjacent segments compacted in the local storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "AdjacentSegmentCompaction",
                         ExpressionType.Bool,
                         "Adjacent segments were compacted")

    def make_validators(self):
        return [
            LogBasedValidator("AdjacentSegmentCompaction",
                              "Adjacent segments of .*, compaction result",
                              confidence_threshold=LOW_THRESHOLD)
        ]


class TS_Timequery(Expression):
    """Timequery is used with tiered-storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Timequery", ExpressionType.Bool,
                         "Timequery is used")

    def make_inputs(self) -> List[TestInput]:
        return [
            ProducerInput(
                "TS_Timequery",
                dict(fake_timestamp_ms=FAKE_BASE_TIMESTAMP_MS,
                     fake_timestamp_step_ms=FAKE_TIMESTAMP_STEP_MS))
        ]


class TS_ChunkedRead(Expression):
    """Chunked read from the tiered-storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_ChunkedRead", ExpressionType.Bool,
                         "Chunked read is used")

    def make_validators(self):
        return [
            LogBasedValidator("TS_ChunkedRead_log",
                              pattern="chunk received, chunk length",
                              execution_stage=TestRunStage.Consume),
            # Disabled because metric is always 0.0 (TODO: investigate)
            # MetricBasedValidator(
            #     "TS_ChunkedRead_metric",
            #     "vectorized_cloud_storage_read_path_chunks_hydrated_total",
            #     execution_stage=TestRunStage.Consume)
        ]


class EnableChunkedRead(Expression):
    """Chunked reads are enabled"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "EnableChunkedRead", ExpressionType.Bool,
                         "Chunked reads are enabled")

    def make_inputs(self):
        return [
            ClusterConfigInput("EnableChunkedRead",
                               {"cloud_storage_disable_chunk_reads": "false"})
        ]


class TS_TxRangeMaterialized(Expression):
    """tx-manifest is downloaded and used when reading from the topic"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_TxRangeMaterialized", ExpressionType.Bool,
                         "tx-manifest is used when reading from the topic")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_TxRangeMaterialized",
                # This pattern is used by two messages, one is added to the log
                # when the tx-range is available in the local cache and the
                # other one is added when the tx-range is already materialized.
                pattern="materialize tx_range",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Consume)
        ]


class TransactionsAborted(Expression):
    """Produces uses transactions and aborts some of them"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TransactionsAborted", ExpressionType.Bool,
                         "Transactions are aborted")

    def make_inputs(self):
        return [
            ProducerInput(
                "TransactionsAborted_producer_input",
                dict(
                    use_transactions=True,
                    transaction_abort_rate=0.1,
                    msgs_per_transaction=10,
                    msg_size=512,
                    msg_count=20000,
                ))
        ]


class TS_UploadIntervalTriggerWrite(Expression):
    """Segment upload triggered by time limit"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_UploadIntervalTriggerWrite",
                         ExpressionType.Bool,
                         "Segment upload triggered by time limit")

    def make_validators(self):
        return [
            LogBasedValidator("TS_TimeboxedWrite",
                              pattern="Using adjusted segment name",
                              confidence_threshold=LOW_THRESHOLD,
                              execution_stage=TestRunStage.Produce)
        ]


class EnableUploadInterval(Expression):
    """Timeboxed uploads are enabled"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "EnableUploadInterval", ExpressionType.Bool,
                         "Timeboxed uploads are enabled")

    def make_inputs(self):
        return [
            # Set upload interval to very low value
            ClusterConfigInput(
                "EnableUploadInterval_config",
                {"cloud_storage_segment_max_upload_interval_sec": 1},
                restart_required=True)
        ]


class TS_Delete(Expression):
    """The segment is deleted from the cloud storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Delete", ExpressionType.Bool,
                         "Segment is deleted from the cloud storage")

    def make_validators(self):
        return [
            # We currently don't have metric for S3 delete
            LogBasedValidator("TS_Delete_log",
                              "remote.*Deleting objects count",
                              execution_stage=TestRunStage.Intermediate,
                              confidence_threshold=LOW_THRESHOLD),
        ]


class TS_STM_DeleteByGC(Expression):
    """Segment deletion triggered by garbage collection"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_STM_DeleteByGC", ExpressionType.Bool,
                         "Segment deletion triggered by garbage collection")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_STM_DeleteByGC_log",
                "ntp_archiver_service.*Deleting segment from cloud storage",
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_STM_GarbageCollect(Expression):
    """GC is applied to the data in the STM"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_STM_GarbageCollect", ExpressionType.Bool,
                         "GC is applied to the data in the STM")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_STM_GarbageCollect_log",
                # The spillover GC uses similar line that has 'spillover' after
                # the number of segments. We need to make sure that we don't
                # match it by using more precise pattern.
                "ntp_archiver_service.*Deleted \d* segments from the cloud",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_STM_RetentionApplied(Expression):
    """Retention is triggered in tiered-storage in the STM region of the log"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_STM_RetentionApplied", ExpressionType.Bool,
                         "Retention is triggered in tiered-storage")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_STM_Retention_log",
                "ntp_archiver_service.*size_based_retention.*Advancing start offset to .* satisfy retention policy",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_Spillover_DeleteByGC(Expression):
    """Segment deletion from the archive triggered by garbage collection"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Spillover_DeleteByGC", ExpressionType.Bool,
                         "Segment deletion triggered by garbage collection")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_Spillover_DeleteByGC_log",
                "ntp_archiver_service.*Enqueuing spillover segment delete from cloud storage",
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_Spillover_GarbageCollect(Expression):
    """GC is applied to the data in the spillover region of the log"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "TS_Spillover_GarbageCollect", ExpressionType.Bool,
            "GC is applied to the data in the spillover region of the log")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_Spillover_GarbageCollect_log",
                "ntp_archiver_service.*Deleted .* spillover segments from the cloud",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_Spillover_SizeBasedRetentionApplied(Expression):
    """Size based retention is applied to the data in the spillover region of the log"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "TS_Spillover_SizeBasedRetentionApplied",
            ExpressionType.Bool,
            "Size based retention is applied to the data in the spillover region of the log"
        )

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_Spillover_SizeBasedRetention_log",
                "ntp_archiver_service.*Archive truncated to offset",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_Spillover_ManifestDeleted(Expression):
    """Spillover manifest deleted from the cloud storage"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "TS_Spillover_ManifestDeleted",
                         ExpressionType.Bool,
                         "Spillover manifest deleted from the cloud storage")

    def make_validators(self):
        return [
            LogBasedValidator(
                "TS_Spillover_ManifestDeleted_log",
                "ntp_archiver_service.*Enqueuing spillover manifest delete from cloud",
                confidence_threshold=LOW_THRESHOLD,
                execution_stage=TestRunStage.Intermediate),
        ]


class TS_SmallSizeBasedRetentionTopicConfig(Expression):
    """Size based retention is set for the topic using
    retention.bytes config. The retention is set to very small
    value.
    """
    def __init__(self, model: Model = Model.default()):
        super().__init__(
            model, "TS_SmallSizeBasedRetentionTopicConfig",
            ExpressionType.Bool,
            "Size based retention is set for the topic using retention.bytes config"
        )

    def make_inputs(self):
        return [
            # Set retention to very small value to trigger size based retention
            # in the STM region and in the archive. In this case the retention is
            # equal to local.target size.
            TopicConfigInput(
                "TS_SmallSizeBasedRetentionTopicConfig",
                "retention.bytes",
                # This has to be larger than local target size to allow
                # reads from the cloud storage. Otherwise the reads will
                # be served by the local storage only.
                str(LOCAL_TARGET_SIZE),
                stage=TestRunStage.Intermediate)
        ]


class SegmentRolledByTimeout(Expression):
    """Segment is rolled because of segment.ms property"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "SegmentRolledByTimeout", ExpressionType.Bool,
                         "Segment is rolled because of segment.ms property")

    def make_validators(self):
        return [
            LogBasedValidator("SegmentRolledByTimeout_log",
                              "segment.ms applied, new segment start offset",
                              confidence_threshold=4,
                              execution_stage=TestRunStage.Produce),
        ]


class EnableSegmentMs(Expression):
    """segment.ms is enabled for the topic"""
    def __init__(self, model: Model = Model.default()):
        super().__init__(model, "EnableSegmentMs", ExpressionType.Bool,
                         "segment.ms is enabled for the topic")

    def make_inputs(self):
        return [
            TopicConfigInput("EnableSegmentMs_topic_conf",
                             "segment.ms",
                             "10",
                             stage=TestRunStage.Startup),
            # We need to have larger segments to trigger segment.ms
            TopicConfigInput("LogSegmentSize_topic_conf",
                             "segment.bytes",
                             str(1024 * 1024 * 128),
                             stage=TestRunStage.Startup),
            # The 'segment.ms' is applied during the local storage housekeeping
            # so the actual segments won't be rolled as frequently as 'segment.ms'
            # suggests.
            ClusterConfigInput("SetLowCompactionInterval_rp_conf",
                               {"log_compaction_interval_ms": "1000"}),
            # Update lower clamp applied to segment.ms for testing
            ClusterConfigInput("SetLogSegmentMsMin_rp_conf",
                               {"log_segment_ms_min": "60000"}),
        ]


class TestCase(dict):
    """Base class for test cases"""
    def __init__(self, validators, inputs, name="None"):
        self.__validators = validators
        self.__inputs = inputs
        self.__name = name
        dict.__init__(self, name=name)

    def __str__(self):
        return self.name

    def validators(self) -> List[EffectValidator]:
        """All validators which are needed to be checked by the test case"""
        return self.__validators

    def inputs(self) -> List[TestInput]:
        """All inputs which are needed to set up the test case"""
        return self.__inputs

    def assert_validators(self, test: TieredStorageEndToEndTest):
        """Check that all validators are done and raise error if
        some of them failed."""
        num_failed = 0
        for v in self.validators():
            r = v.get_result()
            c = v.get_confidence()
            if r is not None:
                test.get_logger().info(
                    f"Result of {v.name()} is {r} with confidence {c}")
                if r is False or c < CONFIDENCE_THRESHOLD:
                    num_failed += 1
            else:
                test.get_logger().info(f"Result of {v.name()} is None")
        assert num_failed == 0, f"{num_failed} validators failed"

    @property
    def name(self):
        return self.__name


def get_test_case_from_name(name):
    tc_list = get_tiered_storage_test_cases(False)
    for tc in tc_list:
        if tc.name == name:
            return tc
    return None


def get_tiered_storage_test_cases(fast_run=False):
    model = Model.default()
    ts_read = TS_Read()
    ts_write = TS_Write()
    ts_delete = TS_Delete()
    ts_manifest_uploaded = TS_ManifestUploaded()
    ts_housekeeping = TS_Housekeeping()
    segment_roll = SegmentRoll()
    segments_removed = SegmentsRemoved()
    local_housekeeping = LocalStorageHousekeeping()
    short_local_retention = ShortLocalRetentionTopicConfig()
    topic_remote_read = RemoteReadTopicConfig()
    topic_remote_write = RemoteWriteTopicConfig()
    global_remote_read = RemoteReadClusterConfig()
    global_remote_write = RemoteWriteClusterConfig()
    # Spillover
    spillover_enabled = EnableSpillover()
    spillover_manifest_uploaded = SpilloverManifestUploaded()
    ts_apply_spillover = ApplySpilloverTriggered()
    # Retention in the STM region
    ts_stm_size_based_retention_applied = TS_STM_RetentionApplied()
    ts_stm_garbage_collect = TS_STM_GarbageCollect()
    ts_stm_delete_by_gc = TS_STM_DeleteByGC()
    # Retention in the spillover region
    ts_spillover_size_based_retention_applied = TS_Spillover_SizeBasedRetentionApplied(
    )
    ts_spillover_garbage_collect = TS_Spillover_GarbageCollect()
    ts_spillover_delete_by_gc = TS_Spillover_DeleteByGC()
    ts_spillover_manifest_deleted = TS_Spillover_ManifestDeleted()
    # Retention topic config
    ts_small_size_based_retention_topic_config = TS_SmallSizeBasedRetentionTopicConfig(
    )
    # Timequery
    ts_timequery = TS_Timequery()
    # ASM
    adjacent_segment_merger_reupload = AdjacentSegmentMergerReupload()
    adjacent_segment_merging_enabled = EnableAdjacentSegmentMerging()
    # Compaction
    compacted_segment_reupload = CompactedReupload()
    enable_compacted_reupload = EnableCompactedSegmentReupload()
    segment_self_compaction = SegmentSelfCompaction()
    topic_is_compacted = TopicCompactionEnabled()
    adjacent_segment_compaction = AdjacentSegmentCompaction()
    # Chunked reads
    enable_chunked_reads = EnableChunkedRead()
    ts_chunked_read = TS_ChunkedRead()
    # Upload interval
    enable_upload_interval = EnableUploadInterval()
    upload_interval_triggered = TS_UploadIntervalTriggerWrite()
    # Transactions
    ts_txrange_materialized = TS_TxRangeMaterialized()
    transactions_aborted = TransactionsAborted()
    # segment.ms
    segment_rolled_by_timeout = SegmentRolledByTimeout()
    enable_segment_ms = EnableSegmentMs()

    model.add(ts_write.requires(segment_roll() == True))
    model.add(
        ts_write.requires(
            z3.Or(topic_remote_write() == True,
                  global_remote_write() == True)))
    model.add(
        ts_read.requires(
            z3.Or(topic_remote_read() == True,
                  global_remote_read() == True)))
    model.add(ts_manifest_uploaded() == ts_write())
    model.add(ts_read.requires(ts_write() == True))
    model.add(ts_read.requires(segments_removed() == True))
    model.add(
        segments_removed.requires(
            z3.And(short_local_retention() == True,
                   local_housekeeping() == True)))
    model.add(ts_chunked_read.requires(ts_read() == True))
    model.add(ts_chunked_read.requires(enable_chunked_reads() == True))
    model.add(
        spillover_manifest_uploaded.requires(spillover_enabled() == True))
    model.add(spillover_manifest_uploaded.requires(ts_write() == True))
    model.add(ts_apply_spillover.requires(ts_housekeeping() == True))
    model.add(
        spillover_manifest_uploaded.requires(ts_apply_spillover() == True))
    model.add(
        adjacent_segment_merger_reupload.requires(
            adjacent_segment_merging_enabled() == True))
    model.add(segment_self_compaction.requires(local_housekeeping() == True))
    model.add(
        compacted_segment_reupload.requires(
            z3.And(segment_self_compaction() == True,
                   topic_is_compacted() == True,
                   enable_compacted_reupload() == True)))
    # Multiple segments can be compacted and merged only if the topic
    # config is set and segments are self compacted
    model.add(
        adjacent_segment_compaction.requires(
            z3.And(topic_is_compacted() == True,
                   segment_self_compaction() == True)))

    # We need enable upload interval to see uploads triggered by timeout
    # We also need uploads to happen (this is not precise definition but its
    # good enough for this test, the upload can be started even if the segment
    # is not rolled)
    model.add(
        upload_interval_triggered.requires(enable_upload_interval() == True))
    model.add(upload_interval_triggered.requires(ts_write() == True))

    # Aborted transactions
    model.add(ts_txrange_materialized.requires(transactions_aborted() == True))
    model.add(ts_txrange_materialized.requires(ts_read() == True))

    # Retention in the STM region
    model.add(ts_stm_delete_by_gc.requires(ts_stm_garbage_collect() == True))
    model.add(
        ts_stm_garbage_collect.requires(
            ts_stm_size_based_retention_applied() == True))
    model.add(
        ts_stm_size_based_retention_applied.requires(
            ts_housekeeping() == True))
    model.add(ts_stm_size_based_retention_applied.requires(ts_write() == True))
    # Retention in the spillover region
    model.add(
        ts_spillover_delete_by_gc.requires(
            ts_spillover_garbage_collect() == True))
    model.add(
        ts_spillover_garbage_collect.requires(
            ts_spillover_size_based_retention_applied() == True))
    model.add(
        ts_spillover_size_based_retention_applied.requires(
            ts_housekeeping() == True))
    model.add(
        ts_delete.requires(
            z3.Or(ts_stm_delete_by_gc() == True,
                  ts_spillover_delete_by_gc() == True)))
    model.add(
        ts_spillover_manifest_deleted.requires(
            ts_spillover_delete_by_gc() == True))
    model.add(
        ts_spillover_size_based_retention_applied.requires(
            spillover_manifest_uploaded() == True))
    # Set retention
    model.add(
        ts_stm_size_based_retention_applied.requires(
            ts_small_size_based_retention_topic_config() == True))
    model.add(
        ts_spillover_size_based_retention_applied.requires(
            ts_small_size_based_retention_topic_config() == True))
    # segment.ms
    model.add(segment_rolled_by_timeout.requires(enable_segment_ms() == True))

    tc_list = []

    solutions = []
    if fast_run:
        solutions.append(
            model.solve_for(ts_read() == True,
                            segment_rolled_by_timeout() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            ts_chunked_read() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            ts_timequery() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            spillover_manifest_uploaded() == True))
        # solutions.append(
        #     model.solve_for(ts_read() == True,
        #                     spillover_manifest_uploaded() == True,
        #                     segment_rolled_by_timeout() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            ts_txrange_materialized() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            adjacent_segment_merger_reupload() == True))
        # solutions.append(
        #     model.solve_for(ts_read() == True,
        #                     adjacent_segment_merger_reupload() == True,
        #                     segment_rolled_by_timeout() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            ts_timequery() == True,
                            spillover_manifest_uploaded() == True))
        # solutions.append(
        #     model.solve_for(ts_read() == True,
        #                     compacted_segment_reupload() == True,
        #                     adjacent_segment_compaction() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            ts_txrange_materialized() == True,
                            spillover_manifest_uploaded() == True))
        solutions.append(
            model.solve_for(ts_read() == True,
                            adjacent_segment_merger_reupload() == True,
                            spillover_manifest_uploaded() == True))
        solutions.append(
            model.solve_for(ts_delete() == True,
                            spillover_manifest_uploaded() == True,
                            ts_spillover_manifest_deleted() == True))
        # solutions.append(
        #     model.solve_for(ts_delete() == True,
        #                     adjacent_segment_compaction() == True))
    else:
        # Check that we can enable uploads using both topic level config
        # and cluster level config.
        solutions += model.find_all_solutions([
            [
                ts_read() == True,
            ],
            [
                topic_remote_read() == True,
                global_remote_read() == True,
            ],
            [
                topic_remote_write() == True,
                global_remote_write() == True,
            ],
        ])

        # Check all combinations without timequery. Also, use only one method
        # to enable tiered-storage to limit number of test cases.
        solutions += model.find_all_solutions([
            [
                ts_read() == True,
                ts_delete() == True,
            ],
            [
                compacted_segment_reupload() == True,
                compacted_segment_reupload() == False,
            ],
            [
                ts_txrange_materialized() == True,
                ts_txrange_materialized() == False,
            ],
            [
                ts_chunked_read() == True,
                ts_chunked_read() == False,
            ],
            [
                upload_interval_triggered() == True,
                upload_interval_triggered() == False,
            ],
            [
                spillover_manifest_uploaded() == True,
                spillover_manifest_uploaded() == False,
            ],
            [
                segment_rolled_by_timeout() == True,
                segment_rolled_by_timeout() == False,
            ],
        ])

        # Check timequery. Note that the compaction and transactions are not used
        # because timequery check does not support them yet.
        solutions += model.find_all_solutions([
            [
                ts_read() == True,
            ],
            [
                ts_timequery() == True,
            ],
            [
                adjacent_segment_merger_reupload() == True,
                adjacent_segment_merger_reupload() == False,
            ],
            [
                ts_chunked_read() == True,
                ts_chunked_read() == False,
            ],
            [
                upload_interval_triggered() == True,
                upload_interval_triggered() == False,
            ],
            [
                spillover_manifest_uploaded() == True,
                spillover_manifest_uploaded() == False,
            ],
            [
                segment_rolled_by_timeout() == True,
                segment_rolled_by_timeout() == False,
            ],
        ])

    for desc, solution in solutions:
        val_list = []
        inp_list = []
        for c in solution:
            for v in c.make_validators():
                val_list.append(v)
            for i in c.make_inputs():
                inp_list.append(i)
        tc = TestCase(val_list, inp_list, name=desc)
        tc_list.append(tc)
    return tc_list


if __name__ == '__main__':
    cases = get_tiered_storage_test_cases(False)
    for c in cases:
        print(f"test case {c}")
        for v in c.validators():
            print(f"  validator {v.name()}")
        for i in c.inputs():
            print(f"  input {i.name()}")
    print(f"found {len(cases)} test cases")
