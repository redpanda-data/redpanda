# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from logging import Logger
from time import sleep, time_ns
from typing import Callable, NamedTuple


class RateTarget(NamedTuple):
    max_total_sec: int
    target_sec: int
    # rates in counter/sec
    target_min_rate: int
    target_max_rate: int


class CounterMeasurement(NamedTuple):
    count: int
    timestamp: int


def time_ms() -> int:
    return time_ns() // 10**6


def sleep_ms(msec: int) -> None:
    sleep(msec / 1000)


class ExpectRate:
    """Measure observed counter (e.g. bytes) rate, until:
    - Fail: We exceed max total duration: Throw an exception.
    - Success: We achieve target rate for at least the min measured duration.

                 --- time --->
      .''''''''''''''''''''''''''''''''''''.
      |         total duration             |
      :'''''''''''''''''''''''''''''''''''':
      |                | measured duration |
      :....................................:
      '                '    target_sec     '

    Accumulate samples every `sample_interval_ms` msec into a vector. As total
    time accumulated exceeds `target sec`, check to see if the average rate over
    the last `target_sec` is within target range.

    Samples Vector
                 --- time --->
      :'''''''''''''''''''''''''''''''''''':
      |t0 |  |  |  |  |  |  |  |  |  |  |  |
      :....................................:
      ' ^              ^                 ^ '
        |              |<-  target sec ->|
        |              |                 |
        0        interval_start      newest sample"""
    def __init__(self,
                 sample: Callable[[], int],
                 logger: Logger,
                 units: str = "bytes"):
        self.logger = logger
        self.sample = sample
        self.units = units

    def expect_rate(self, target: RateTarget) -> None:

        sample_interval_ms = 1000
        ts = lambda x: x.timestamp
        total_msec = 0
        interval_start = -1
        samples: list[CounterMeasurement] = []
        t_0 = time_ms()
        delta_t = 0
        while total_msec < target.max_total_sec * 1000:
            m = CounterMeasurement(count=self.sample(), timestamp=time_ms())
            samples.append(m)
            elapsed_msec = ts(m) - t_0

            # find latest index that is at least target_sec old
            for i, sample in enumerate(samples):
                delta_t = ts(m) - ts(sample)
                if delta_t < target.target_sec * 1000:
                    break
                interval_start = i

            if interval_start >= 0:
                elapsed_count = m.count - samples[interval_start].count
                elapsed_msec = m.timestamp - samples[interval_start].timestamp
                rate = elapsed_count / (elapsed_msec / 1000.0)
                if target.target_min_rate <= rate and rate <= target.target_max_rate:
                    self.logger.info(
                        f"Rate target met: {rate:.1f} {self.units}/sec " +
                        f"over the last {elapsed_msec/1000:.3f} sec.")
                    return
                else:
                    self.logger.debug(
                        f"rate: {rate:.1f} for {elapsed_msec/1000:.3f}")
            else:
                self.logger.debug(
                    f"(waiting, {delta_t/1000:.1f}/{target.target_sec} sec) " +
                    f"{self.units}: {m.count}")

            sleep_ms(sample_interval_ms)
            total_msec = time_ms() - t_0

        raise RuntimeError("Unable to attain target rate in time.")