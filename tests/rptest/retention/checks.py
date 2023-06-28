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


class Checks(object):
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

    def add_arg(self, key, value):
        self.args[key] = value

    def add_check(self, desc, expression, show=True):
        _check = deepcopy(self._check_template)
        _check["desc"] = desc
        _check["expr"] = expression
        _check["show"] = show

        self.checks.append(_check)

    def conduct_checks(self):
        if self._calculated:
            return
        else:
            names2const = NamesToConstants(self.args)
            for idx in range(len(self.checks)):
                _item = self.checks[idx]
                _expr = _item['expr']
                _parsed = parse(_expr)
                names2const.visit(_parsed)
                _item['expr_text'] = unparse(_parsed).strip()
                _item['result'] = eval(_item['expr_text'], {}, self.args)
            self._calculated = True

    def get_summary_as_text(self):
        def format_items(iterator):
            _fmt = "\n{0}\n-> {1}: {2}\n"
            _str = ""
            for item in iterator:
                # Show in summary only if result is false or show is true
                if item["show"] or not item['result']:
                    _str += _fmt.format(
                        item['expr'],
                        item['result'],
                        item['expr_text']
                    )
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

    def assert_results(self):
        _r = filter(lambda x: not x['result'], self.checks)
        if len(list(_r)) > 0:
            raise AssertionError(
                "At least one check failed.\n" + self.get_summary_as_text()
            )
        else:
            return True


def main():
    # this is very simple unittest for Checks :)
    args = {
        "a": 5,
        "b": 6
    }
    expr = "a+b*2"
    parsed_expr = parse(expr)
    NamesToConstants(args).visit(parsed_expr)
    expr_values = unparse(parsed_expr).strip()
    result = eval("a+b*2", {}, args)
    print(f"Simple check: {expr}")
    print(f"{expr_values} = {result}")

    chk = Checks()
    chk.add_arg("value1", 23)
    chk.add_arg("value2", 56)
    chk.add_check("Simple formula", "value1 <= value2")
    chk.conduct_checks()
    print(chk.get_summary_as_text())
    if chk.assert_results():
        print(chk.get_summary_as_text())


if __name__ == '__main__':
    main()
