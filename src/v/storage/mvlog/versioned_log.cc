// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/versioned_log.h"

#include "container/fragmented_vector.h"
#include "model/offset_interval.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/segment_appender.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

using namespace std::literals::chrono_literals;

namespace storage::experimental::mvlog {

active_segment::active_segment(
  std::unique_ptr<file> f, model::offset o, segment_id id, size_t target_size)
  : segment_file(std::move(f))
  , appender(std::make_unique<segment_appender>(segment_file.get()))
  , readable_seg(std::make_unique<readable_segment>(segment_file.get()))
  , construct_time(ss::lowres_clock::now())
  , target_max_size(target_size)
  , id(id)
  , base_offset(o)
  , next_offset(o) {}

active_segment::~active_segment() = default;

readonly_segment::~readonly_segment() = default;
readonly_segment::readonly_segment(std::unique_ptr<active_segment> active_seg)
  : segment_file(std::move(active_seg->segment_file))
  , readable_seg(std::move(active_seg->readable_seg))
  , id(active_seg->id)
  , offsets(model::bounded_offset_interval::checked(
      active_seg->base_offset, model::prev_offset(active_seg->next_offset))) {}

versioned_log::versioned_log(storage::ntp_config cfg)
  : ntp_cfg_(std::move(cfg)) {}

ss::future<> versioned_log::roll_unlocked() {
    vassert(
      !active_segment_lock_.ready(),
      "roll_unlocked() must be called with active segment lock held");
    vassert(has_active_segment(), "Expected an active segment");
    vlog(
      log.info,
      "Rolling segment file {}",
      active_seg_->segment_file->filepath().c_str());
    co_await active_seg_->segment_file->flush();
    auto ro_seg = std::make_unique<readonly_segment>(std::move(active_seg_));
    segs_.push_back(std::move(ro_seg));
}

ss::future<> versioned_log::create_unlocked(model::offset base) {
    vassert(
      !active_segment_lock_.ready(),
      "create_unlocked() must be called with active segment lock held");
    vassert(!has_active_segment(), "Expected no active segment");
    const auto new_path = fmt::format(
      "{}/{}.log", ntp_cfg_.work_directory(), next_segment_id_());
    vlog(log.info, "Creating new segment file {}", new_path);
    auto f = co_await file_mgr_.create_file(std::filesystem::path{new_path});
    auto active_seg = std::make_unique<active_segment>(
      std::move(f), base, next_segment_id_, compute_max_segment_size());
    active_seg_ = std::move(active_seg);
    ++next_segment_id_;
}

ss::future<> versioned_log::apply_segment_ms() {
    auto lock = co_await active_segment_lock_.get_units();
    if (!has_active_segment()) {
        vlog(log.trace, "No active segment to roll");
        co_return;
    }
    const auto target_roll_deadline = compute_roll_deadline();
    if (!target_roll_deadline.has_value()) {
        vlog(log.trace, "No segment rolling deadline");
        co_return;
    }
    const auto now = ss::lowres_clock::now();
    if (now >= target_roll_deadline.value()) {
        vlog(
          log.debug,
          "Starting to roll {}, now vs deadline: {} vs {}",
          ss::sstring(active_seg_->segment_file->filepath()),
          now.time_since_epoch(),
          target_roll_deadline->time_since_epoch());
        co_await roll_unlocked();
    }
}

ss::future<> versioned_log::close() {
    // TODO(awong): handle concurrency. Should also prevent further appends,
    // readers, etc.
    if (active_seg_) {
        co_await active_seg_->segment_file->close();
    }
    for (auto& seg : segs_) {
        co_await seg->segment_file->close();
    }
}

ss::future<> versioned_log::append(model::record_batch b) {
    auto lock = co_await active_segment_lock_.get_units();
    if (has_active_segment()) {
        const auto cur_size = active_seg_->segment_file->size();
        const auto target_max_size = active_seg_->target_max_size;
        if (cur_size >= target_max_size) {
            vlog(
              log.debug,
              "Starting to roll {}, cur bytes vs target bytes: {} vs {}",
              active_seg_->segment_file->filepath().c_str(),
              cur_size,
              target_max_size);
            // The active segment needs to be rolled.
            co_await roll_unlocked();
        }
    }
    if (!has_active_segment()) {
        co_await create_unlocked(b.base_offset());
    }
    vassert(has_active_segment(), "Expected active segment");
    // TODO(awong): it's probably worth handling this more gracefully with an
    // error code. We shouldn't rely on storage-external callers to uphold this
    // invariant, even if it is true.
    vassert(
      b.base_offset() == active_seg_->next_offset,
      "Appending {} when {} was expected",
      b.base_offset(),
      active_seg_->next_offset);
    auto next = model::next_offset(b.last_offset());
    co_await active_seg_->appender->append(std::move(b));
    active_seg_->next_offset = next;
}

size_t versioned_log::segment_count() const {
    return segs_.size() + (active_seg_ ? 1 : 0);
}
bool versioned_log::has_active_segment() const {
    return active_seg_ != nullptr;
}

size_t versioned_log::compute_max_segment_size() const {
    if (
      ntp_cfg_.has_overrides()
      && ntp_cfg_.get_overrides().segment_size.has_value()) {
        return ntp_cfg_.get_overrides().segment_size.value();
    }
    // TODO(awong): get defaults from log_manager
    // TODO(awong): clamp by min/max configs
    vassert(false, "Not implemented");
}

std::optional<ss::lowres_clock::time_point>
versioned_log::compute_roll_deadline() const {
    if (!has_active_segment() || !ntp_cfg_.segment_ms().has_value()) {
        return std::nullopt;
    }
    return active_seg_->construct_time + ntp_cfg_.segment_ms().value();
}

} // namespace storage::experimental::mvlog
