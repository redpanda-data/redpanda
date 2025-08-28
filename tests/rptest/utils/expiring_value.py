# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class ExpiringValue(Generic[T]):
    """
    Single value cache with optional expiration.
    """

    def __init__(
        self, value: Optional[T] = None, *, expire_at: Optional[float] = None
    ) -> None:
        """
        Create an ExpiringValue with optional initial value and expiration.

        :param value: The initial value.
        :param expire_at: The expiration time in seconds since the epoch. If
            None, the value will never expire.
        """
        self._data = value
        self._expire_at = expire_at

    def update(self, value: T, *, expire_at: Optional[float] = None) -> None:
        """
        :param value: The new value.
        :param expire_at: The expiration time in seconds since the epoch. If
            None, the value will never expire.
        """
        self._data = value
        self._expire_at = expire_at

    def value(self) -> Optional[T]:
        """
        Get the value if it is not expired.
        """
        if self._expire_at is not None and self._expire_at <= time.time():
            return None

        return self._data
