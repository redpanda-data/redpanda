# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import warnings
from contextlib import contextmanager
from typing import Any

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from rptest.services.cluster import cluster as rp_cluster
from rptest.utils.mode_checks import skip_in_cdt, skip_file_in_cdt


@contextmanager
def _cloud_provider(value: str | None):
    """Temporarily set/unset CLOUD_PROVIDER, restoring the prior value."""
    prev = os.environ.get("CLOUD_PROVIDER")
    if value is None:
        os.environ.pop("CLOUD_PROVIDER", None)
    else:
        os.environ["CLOUD_PROVIDER"] = value
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("CLOUD_PROVIDER", None)
        else:
            os.environ["CLOUD_PROVIDER"] = prev


def _ignore_marks(target: object) -> list[Any]:
    marks: list[Any] = getattr(target, "marks", [])
    return [m for m in marks if m.name == "IGNORE"]


class SkipInCdtTest(Test):
    """
    Unit test for the CDT opt-out decorators. Runs with num_nodes=0 (no
    cluster); inspects the ducktape marks the decorators attach under each
    CLOUD_PROVIDER value.
    """

    @cluster(num_nodes=0)
    def test_method_scope(self):
        # docker: decorator is a no-op, method runs as today
        with _cloud_provider("docker"):

            @skip_in_cdt(reason="r")
            @cluster(num_nodes=1)
            def m_docker() -> None:
                pass

            assert _ignore_marks(m_docker) == [], "method must not be ignored on docker"
            assert getattr(m_docker, "_skip_in_cdt_reason") == "r"

        # cloud: method ignored with an ignore-ALL mark (not a param matcher)
        with _cloud_provider("aws"):

            @skip_in_cdt(reason="r")
            @cluster(num_nodes=1)
            def m_aws() -> None:
                pass

            marks = _ignore_marks(m_aws)
            assert len(marks) == 1, "method must be ignored on aws"
            # regression guard: reason must NOT be forwarded to ducktape ignore
            assert marks[0].injected_args is None, (
                "must be ignore-all, not a param matcher"
            )

    @cluster(num_nodes=0)
    def test_class_scope(self):
        with _cloud_provider("aws"):

            @skip_in_cdt(reason="cls")
            class Dummy(Test):
                @cluster(num_nodes=1)
                def test_a(self):
                    pass

                @cluster(num_nodes=1)
                def test_b(self):
                    pass

                def helper(self):
                    pass

            assert _ignore_marks(Dummy.test_a), "test_a must be ignored on aws"
            assert _ignore_marks(Dummy.test_b), "test_b must be ignored on aws"
            assert _ignore_marks(Dummy.helper) == [], "non-test method untouched"
            assert getattr(Dummy, "_skip_in_cdt_reason") == "cls"

        with _cloud_provider("docker"):

            @skip_in_cdt(reason="cls")
            class Dummy2(Test):
                @cluster(num_nodes=1)
                def test_a(self):
                    pass

            assert _ignore_marks(Dummy2.test_a) == [], "no ignore on docker"

    @cluster(num_nodes=0)
    def test_file_scope(self):
        class Local(Test):
            @cluster(num_nodes=1)
            def test_x(self):
                pass

        class Imported(Test):
            @cluster(num_nodes=1)
            def test_y(self):
                pass

        # Simulate a module file containing Local and an imported base class.
        Local.__module__ = "fake_module"
        Imported.__module__ = "other_module"
        ns: dict[str, Any] = {
            "__name__": "fake_module",
            "Local": Local,
            "Imported": Imported,
        }

        with _cloud_provider("aws"):
            skip_file_in_cdt(reason="file", namespace=ns)

        assert _ignore_marks(Local.test_x), "class defined in module must be ignored"
        assert _ignore_marks(Imported.test_y) == [], "imported class left untouched"

    @cluster(num_nodes=0)
    def test_class_scope_inherited_raises(self):
        # A class whose test methods are inherited (not directly defined) cannot be
        # safely class-scoped (ducktape marks are function-scoped); @skip_in_cdt must
        # raise rather than silently leave the inherited test running in CDT.
        class Base(Test):
            @cluster(num_nodes=1)
            def test_inherited(self):
                pass

        class Sub(Base):  # no own test methods
            pass

        raised = False
        try:
            skip_in_cdt(reason="x")(Sub)
        except RuntimeError:
            raised = True
        assert raised, "@skip_in_cdt on a class with only inherited tests must raise"

    @cluster(num_nodes=0)
    def test_file_scope_covers_same_module_base(self):
        # skip_file_in_cdt marks each module class's OWN tests; a same-module base's
        # tests get marked, so a thin subclass inheriting them is covered (no raise).
        class Base(Test):
            @cluster(num_nodes=1)
            def test_x(self):
                pass

        class Sub(Base):  # no own test methods
            pass

        Base.__module__ = "fake_module"
        Sub.__module__ = "fake_module"
        ns: dict[str, Any] = {"__name__": "fake_module", "Base": Base, "Sub": Sub}
        with _cloud_provider("aws"):
            skip_file_in_cdt(reason="f", namespace=ns)
        assert _ignore_marks(Base.test_x), "base's own test must be marked"
        assert _ignore_marks(Sub.test_x), "subclass inherits the marked base test"

    @cluster(num_nodes=0)
    def test_rptest_cluster_wrapper_marks(self):
        # Production test files use rptest's @cluster wrapper, not ducktape's raw
        # one; the mechanism only sees `marks` because the wrapper propagates them
        # (services/cluster.py: `wrapped.marks = f.marks`). Pin that contract -- the
        # IGNORE mark must still land on a method decorated with the real wrapper.
        with _cloud_provider("aws"):

            @skip_in_cdt(reason="r")
            @rp_cluster(num_nodes=1)
            def m() -> None:
                pass

            assert _ignore_marks(m), (
                "IGNORE must land on an rptest @cluster-wrapped method"
            )

    @cluster(num_nodes=0)
    def test_file_scope_warns_on_cross_module_inherited(self):
        # A class with its OWN test plus a test inherited from ANOTHER module:
        # skip_file marks the own one but cannot cover the inherited (cross-module)
        # one, so it must warn rather than silently miss it.
        class OtherBase(Test):
            @cluster(num_nodes=1)
            def test_inherited(self):
                pass

        class Mixed(OtherBase):
            @cluster(num_nodes=1)
            def test_own(self):
                pass

        OtherBase.__module__ = "other_module"
        Mixed.__module__ = "this_module"
        ns: dict[str, Any] = {"__name__": "this_module", "Mixed": Mixed}

        with _cloud_provider("aws"):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                skip_file_in_cdt(reason="mixed", namespace=ns)

        assert _ignore_marks(Mixed.test_own), "own test must be marked"
        assert any("test_inherited" in str(w.message) for w in caught), (
            "must warn about the cross-module inherited test"
        )
