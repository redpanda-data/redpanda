# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import ABC, abstractmethod


class FaultBase(ABC):
    @property
    @abstractmethod
    def name(self):
        pass


class RecoverableFault(FaultBase):
    @abstractmethod
    def inject(self):
        pass

    @abstractmethod
    def heal(self):
        pass


class OneoffFault(FaultBase):
    @abstractmethod
    def execute(self):
        pass
