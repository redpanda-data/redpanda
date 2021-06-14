/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/types.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

/// Common utils shared across fixtures

template<typename Fn>
static ss::future<model::record_batch_reader::data_t> do_drain(
  model::offset offset,
  std::size_t limit,
  model::timeout_clock::time_point timeout,
  Fn&& fn) {
    struct state {
        std::size_t batches_read{0};
        model::offset next_offset;
        model::record_batch_reader::data_t batches;
        explicit state(model::offset o)
          : next_offset(o) {}
    };
    return ss::do_with(
      state(offset),
      std::forward<Fn>(fn),
      [limit, timeout](state& s, Fn& fn) mutable {
          return ss::do_until(
                   [&s, limit, timeout] {
                       const auto now = model::timeout_clock::now();
                       return (s.batches_read >= limit) || (now > timeout);
                   },
                   [&s, &fn]() mutable {
                       return fn(s.next_offset)
                         .then([](model::record_batch_reader rbr) {
                             return model::consume_reader_to_memory(
                               std::move(rbr), model::no_timeout);
                         })
                         .then([&s](model::record_batch_reader::data_t b) {
                             using namespace std::chrono_literals;
                             if (b.empty()) {
                                 return ss::sleep(20ms);
                             }
                             s.batches_read += b.size();
                             s.next_offset = ++b.back().last_offset();
                             for (model::record_batch& rb : b) {
                                 s.batches.push_back(std::move(rb));
                             }
                             return ss::now();
                         });
                   })
            .then([&s] { return std::move(s.batches); });
      });
}

static storage::log_reader_config log_rdr_cfg(const model::offset& min_offset) {
    return storage::log_reader_config(
      min_offset,
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      std::nullopt);
}
