// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/lock_manager.h"

#include "container/chunked_vector.h"
#include "model/offset_interval.h"
#include "ssx/abort_source.h"
#include "ssx/when_all.h"
#include "storage/segment.h"

#include <seastar/core/abort_on_expiry.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

namespace storage {

static ss::future<std::unique_ptr<lock_manager::lease>> range(
  segment_set::underlying_t segs,
  model::opt_abort_source_t as = std::nullopt,
  std::optional<ss::semaphore::clock::time_point> read_lock_deadline
  = std::nullopt) {
    auto ctx = std::make_unique<lock_manager::lease>(
      segment_set(std::move(segs)));

    ss::abort_source never_abort;
    ss::abort_on_expiry<ss::semaphore::clock> expiry(
      read_lock_deadline.value_or(ss::semaphore::clock::time_point::max()));
    ssx::composite_abort_source combined(
      as.has_value() ? as->get() : never_abort, expiry.abort_source());

    chunked_vector<ss::future<ss::rwlock::holder>> dispatch;
    dispatch.reserve(ctx->range.size());
    for (auto& s : ctx->range) {
        dispatch.push_back(s->read_lock(combined.as()));
    }

    ctx->locks
      = co_await ssx::when_all_succeed<chunked_vector<ss::rwlock::holder>>(
        std::move(dispatch));
    co_return ctx;
}

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const timequery_config& cfg) {
    auto query_interval = model::bounded_offset_interval::checked(
      cfg.min_offset, cfg.max_offset);

    segment_set::underlying_t tmp;
    // Copy the first segment that has timestamps >= cfg.time and overlaps with
    // the offset range [min_offset, max_offset].
    // We only need one segment/batch to satisfy a timequery.
    auto it = std::find_if(
      _set.begin(), _set.end(), [&query_interval, &cfg](const auto& s) {
          if (s->empty()) {
              return false;
          }

          // We exclude the segments that only contain configuration batches
          // from our search, as their timestamps may be wildly different from
          // the user provided timestamps.
          if (s->index().non_data_timestamps()) {
              return false;
          }

          if (s->index().max_timestamp() < cfg.time) {
              return false;
          }

          // Safety: unchecked is safe here because we did already check
          // `!s->empty()` above to ensure that the segment has data.
          auto segment_interval = model::bounded_offset_interval::unchecked(
            s->offsets().get_base_offset(), s->offsets().get_dirty_offset());

          return segment_interval.overlaps(query_interval);
      });

    if (it != _set.end()) {
        tmp.push_back(*it);
    }

    return range(std::move(tmp), cfg.abort_source);
}

ss::future<std::unique_ptr<lock_manager::lease>>
lock_manager::range_lock(const local_log_reader_config& cfg) {
    segment_set::underlying_t tmp;
    std::copy_if(
      _set.lower_bound(cfg.start_offset),
      _set.end(),
      std::back_inserter(tmp),
      [&cfg](ss::lw_shared_ptr<segment>& s) {
          // must be base offset
          return s->offsets().get_base_offset() <= cfg.max_offset;
      });
    return range(std::move(tmp), cfg.abort_source, cfg.read_lock_deadline);
}

std::ostream& operator<<(std::ostream& o, const lock_manager::lease& l) {
    fmt::print(o, "({})", l.range);
    return o;
}

} // namespace storage
