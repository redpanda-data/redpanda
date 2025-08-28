# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import collections
import typing

T = typing.TypeVar("T")


class BookendCollection(typing.Generic[T]):
    """
    A collection for collecting a fixed number of elements from the beginning
    and end of a stream and dropping everything in the middle.

    bookend (noun): a support placed at the end of a row of books to keep them
    upright, typically forming one of a pair.
    """

    class Gap:
        """
        A placeholder for dropped elements.
        """

        def __init__(self, size: int):
            self.size = size

        def __repr__(self):
            return "<Gap size={}>".format(self.size)

        def __eq__(self, other) -> bool:
            return isinstance(other, self.__class__) and self.size == other.size

    def __init__(self, *, head: int, tail: int):
        assert head > 0, "head must be greater than 0"
        assert tail > 0, "tail must be greater than 0"

        self._head_capacity = head
        self._tail_capacity = tail

        self._head_data = []
        self._num_dropped = 0
        self._tail_data = collections.deque()

    def append(self, item: T) -> None:
        if len(self._head_data) < self._head_capacity:
            self._head_data.append(item)
        else:
            self._tail_data.append(item)
            if len(self._tail_data) > self._tail_capacity:
                self._tail_data.popleft()
                self._num_dropped += 1

    def __iter__(self) -> typing.Iterator[T | Gap]:
        for item in self._head_data:
            yield item

        if self._num_dropped > 0:
            yield self.Gap(self._num_dropped)

        for item in self._tail_data:
            yield item

    def __str__(self) -> str:
        return "".join(str(item) for item in self)
