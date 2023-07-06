# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# noqa: E501

from ast import NodeTransformer, parse, unparse, Constant
from copy import deepcopy


# Credit to blhsing:
# https://stackoverflow.com/questions/74528551/how-to-substitute-value-of-variables-in-python-expression-but-not-evaluate-the
class NamesToConstants(NodeTransformer):
    """
    This is used in combination with eval to show formula
    with constant substitution for visibility on which values
    leads to assertion reasult. Example:
        Simple formula
        value1 <= value2
        True: 23 <= 56
    """
    _args = {}

    def __init__(self, args):
        self._args = args

    def visit_Name(self, node):
        # using own dict of vars instead of globals()
        if node.id in self._args:
            value = self._args[node.id]
            # convert value to integer if viable
            try:
                value = int(value)
            except Exception:
                pass
            return Constant(value=value)
        return node


class RedpandaChecksBase(object):
    """
    Base class to hold all checks related to specific test.
    Improves readability and code cleanines.
    Example use: 
    def check_non_zero_segment_upload(): 
        ...
        return

    code:
        checks.check_non_zero_segment_upload()
    """

    _check_template = {
        "desc": "check_description",
        "expr": "check_formula",
        "expr_text": "",
        "result": None,
        "show": True
    }
    _calculated = False

    checks = []
    args = {}
    summary = None

    # Add argument to internal dict for eval
    def add_arg(self, key, value):
        self.args[key] = value

    # Add expression for future evaluation
    # set show=False to hide passed result from Summary
    # use case: checks done for each partition spams Summary
    def add_check(self, desc, expression, show=True):
        _check = deepcopy(self._check_template)
        _check["desc"] = desc
        _check["expr"] = expression
        _check["show"] = show

        self.checks.append(_check)

    # Evaluate all of the expressions
    # And populate text version to show it
    def conduct_checks(self):
        if self._calculated:
            return
        else:
            # class that converts arguments to its values
            names2const = NamesToConstants(self.args)
            # iterate all checks
            for idx in range(len(self.checks)):
                # get expression for this check
                _item = self.checks[idx]
                _expr = _item['expr']
                # Do the magic:
                # - 'precompile/parse' it
                # - visit each argument/node and update to values
                # - unparse it back to text
                _parsed = parse(_expr)
                names2const.visit(_parsed)
                _item['expr_text'] = unparse(_parsed).strip()
                # evaluate text version of expression
                # using locally stored arguments
                _item['result'] = eval(_item['expr_text'], {}, self.args)
            # Mark that we calculated all expressions
            self._calculated = True

    # Create text Summary to show in logs
    def get_summary_as_text(self):
        """
        Creates summary that contains formulas and values.
        Example:
            # Summary:
            ## Failed checks:
            manifest_uploads <= expect_manifest_uploads
            -> False: 11815 <= 34

            ## Passed checks:
            actual_byte_rate > produce_byte_rate / throughput_tolerance
            -> True: 10082186 > 10485760 / 2

            lag_seconds < config_interval
            -> True: 0 < 16

            manifest_uploads > 0
            -> True: 11815 > 0

            segment_uploads > 0
            -> True: 9815 > 0        
        """
        def format_items(iterator):
            _fmt = "\n{0}\n-> {1}: {2}\n"
            _str = ""
            for item in iterator:
                # Show in summary only if result is false or show is true
                if item["show"] or not item['result']:
                    _str += _fmt.format(item['expr'], item['result'],
                                        item['expr_text'])
            return _str

        if not self.summary:
            _summary = "\n# Summary:"

            _failed = filter(lambda x: not x['result'], self.checks)
            _failed_summary = format_items(_failed)
            if len(_failed_summary) > 0:
                _summary += "\n## Failed checks:"
                _summary += _failed_summary

            _passed = filter(lambda x: x['result'], self.checks)
            _passed_summary = format_items(_passed)
            if len(_passed_summary) > 0:
                _summary += "\n## Passed checks:"
                _summary += _passed_summary

            self.summary = _summary
        return self.summary

    # Assert calculated results and generate AssertionError
    # if any fails occured
    def assert_results(self):
        _r = filter(lambda x: not x['result'], self.checks)
        if len(list(_r)) > 0:
            raise AssertionError("At least one check failed.\n" +
                                 self.get_summary_as_text())
        else:
            return True

    def check_non_zero_value(self, uploads, value_label=None):
        if value_label:
            self.add_arg(value_label, uploads)
            self.add_check(
                f"Non-zero '{value_label}' value",
                f"{value_label} > 0",
            )
        else:
            self.add_arg("non_zero_value", uploads)
            self.add_check(
                "Non-zero value check",
                "non_zero_value > 0",
            )


class InfiniteRetentionChecks(RedpandaChecksBase):
    """
    Class with checks that used accross multiple tests in infinite retention
    """
    def __init__(self, params) -> None:
        self.params = params

    def check_byte_rate_respected(self, actual_byte_rate, var_label=None):
        """
        Producer should be within a factor of two of the intended byte rate,
        or something is wrong with the test (running on nodes that can't
        keep up?) or with Redpanda (some instability interrupted produce?)
        """
        self.add_arg("produce_byte_rate", self.params.produce_byte_rate)
        self.add_arg("producer_tolerance", self.params.producer_tolerance)

        # Check the workload is respecting rate limit
        if var_label:
            self.add_arg(var_label, actual_byte_rate)
            self.add_check(
                "Check the workload is respecting rate limit",
                f"{var_label} < produce_byte_rate * producer_tolerance",
            )
        else:
            self.add_arg("actual_byte_rate", actual_byte_rate)
            self.add_check(
                "Check the workload is respecting rate limit",
                "actual_byte_rate < produce_byte_rate * producer_tolerance",
            )

    def check_byte_rate_respected_no_stress(self, actual_byte_rate):
        """
        Actual byte rate must exceed expected if no stress introduced
        """
        self.add_arg("actual_byte_rate", actual_byte_rate)
        self.add_arg("produce_byte_rate", self.params.produce_byte_rate)
        self.add_arg("producer_tolerance", self.params.producer_tolerance)
        self.add_check(
            "Producer should be within a factor of two of the intended byte rate, "
            "or something is wrong with the test (running on nodes that can't "
            "keep up?) or with Redpanda (some instability interrupted produce?)",
            "actual_byte_rate > produce_byte_rate / producer_tolerance",
        )

    def check_expected_message_count(self, actual_msg_count):
        """
        Check that measured message count correspunds to expected
        """
        self.add_arg("msg_count", self.params.msg_count)
        self.add_arg("sum_high_watermarks", actual_msg_count)
        self.add_check(
            "Ensure that all messages made it to topic",
            "sum_high_watermarks >= msg_count",
        )

    def check_iteration_message_count(self, actual, iteration):
        self.add_arg(f"i{iteration}_sum_high_watermarks", actual)
        self.add_arg(f"i{iteration}_msg_count",
                     self.params.msg_count * iteration)
        self.add_check(
            "Ensure that all messages made it to topic",
            f"i{iteration}_sum_high_watermarks >= i{iteration}_msg_count",
        )

    def check_lag_not_exceed_configured_interval(self, actual, configured):
        """
        Lag should not be higher than expected
        """
        self.add_arg("lag_seconds", actual)
        self.add_arg("config_interval", configured)
        self.add_check(
            "Measure how far behind the tiered storage uploads are: "
            "success condition should be that they are within some "
            "time range of the most recently produced data",
            "lag_seconds < config_interval",
        )

    def check_expected_manifest_uploads(self, upload_count, produce_duration):
        """
        Manifest upload count should be greater than min values
        """
        self.add_arg("manifest_uploads", int(upload_count))
        self.add_arg("produce_duration", int(produce_duration))
        self.add_arg("manifest_upload_interval",
                     self.params.manifest_upload_interval)
        self.add_check(
            "For spillover active tests manifest uploads should be "
            "significant",
            "manifest_uploads > produce_duration // manifest_upload_interval + 3",
        )
