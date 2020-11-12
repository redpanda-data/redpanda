# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from unittest import TestCase

from gobekli.consensus import LinearizabilityRegisterChecker
from gobekli.consensus import Violation


class TestLinearizabilityRegisterChecker(TestCase):
    def test_write_ok(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_ended("h7h2v")

    def test_write_out_order_ok(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_ended("2hch4")
        checker.write_ended("h7h2v")

    def test_write_wrong_version_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 0, "43")
        try:
            checker.write_ended("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "h7h2v:0 -> j3qq4 doesn't lead to the latest observed state: j3qq4:0":
                raise e

    def test_write_out_order_wrong_version_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("h7h2v", "2hch4", 1, "44")
        try:
            checker.write_ended("2hch4")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "2hch4:1 -> h7h2v doesn't lead to the pending state: h7h2v:1":
                raise e

    def test_write_condition_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq3", "h7h2v", 1, "43")
        try:
            checker.write_ended("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "h7h2v:1 -> j3qq3 doesn't lead to the latest observed state: j3qq4:0":
                raise e

    def test_write_stale_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_ended("h7h2v")
        checker.write_started("j3qq4", "2hch4", 2, "44")
        try:
            checker.write_ended("2hch4")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "2hch4:2 -> j3qq4 doesn't lead to the latest observed state: h7h2v:1":
                raise e

    def test_write_fork1_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("j3qq4", "2hch4", 1, "44")
        checker.write_ended("h7h2v")
        try:
            checker.write_ended("2hch4")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "2hch4:1 -> j3qq4 doesn't lead to the latest observed state: h7h2v:1":
                raise e

    def test_write_fork2_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("j3qq4", "2hch4", 1, "44")
        checker.write_ended("2hch4")
        try:
            checker.write_ended("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "h7h2v:1 -> j3qq4 doesn't lead to the latest observed state: 2hch4:1":
                raise e

    def test_cant_commit_canceled(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_canceled("h7h2v")
        try:
            checker.write_ended("2hch4")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "2hch4:2 -> h7h2v doesn't lead to the latest observed state: j3qq4:0":
                raise e

    def test_cant_cancel_comitted(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_ended("2hch4")
        try:
            checker.write_canceled("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Can't cancel an already applied write: h7h2v":
                raise e

    def test_can_commit_timeouted(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_timeouted("h7h2v")
        checker.write_ended("2hch4")

    def test_cant_commit_gced(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_started("j3qq4", "2hch4", 2, "44")
        checker.write_ended("2hch4")
        try:
            checker.write_ended("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "current head 2hch4:2 doesn't lead to h7h2v and has greater version":
                raise e

    def test_read_ok(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.read_started("pid1")
        checker.read_ended("pid1", "j3qq4", "42")

    def test_read_phantom_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.read_started("pid1")
        try:
            checker.read_ended("pid1", "h7h2v", "42")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Stale or phantom read h7h2v":
                raise e

    def test_read_wrong_value_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.read_started("pid1")
        try:
            checker.read_ended("pid1", "j3qq4", "43")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Read value 43 doesn't match written value 42":
                raise e

    def test_read_commits_write1(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.read_started("pid1")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.read_ended("pid1", "h7h2v", "43")
        checker.write_ended("h7h2v")

    def test_read_commits_write2(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.read_started("pid1")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.read_ended("pid1", "h7h2v", "43")
        try:
            checker.write_canceled("h7h2v")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Can't cancel an already applied write: h7h2v":
                raise e

    def test_stale_read_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_ended("h7h2v")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_ended("2hch4")
        checker.read_started("pid1")
        try:
            checker.read_ended("pid1", "h7h2v", "43")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Stale or phantom read h7h2v":
                raise e

    def test_read_new_after_timeout(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_timeouted("h7h2v")
        checker.read_started("pid1")
        checker.read_ended("pid1", "h7h2v", "43")

    def test_read_old_after_timeout(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_timeouted("h7h2v")
        checker.read_started("pid1")
        checker.read_ended("pid1", "j3qq4", "42")

    def test_read_new_after_canceled_err(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_ended("h7h2v")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_canceled("2hch4")
        checker.read_started("pid1")
        try:
            checker.read_ended("pid1", "2hch4", "44")
            raise Exception("should be unreachable")
        except Violation as e:
            if e.message != "Stale or phantom read 2hch4":
                raise e

    def test_read_old_after_canceled_ok(self):
        checker = LinearizabilityRegisterChecker()
        checker.init("j3qq4", 0, "42")
        checker.write_started("j3qq4", "h7h2v", 1, "43")
        checker.write_ended("h7h2v")
        checker.write_started("h7h2v", "2hch4", 2, "44")
        checker.write_canceled("2hch4")
        checker.read_started("pid1")
        checker.read_ended("pid1", "h7h2v", "43")
