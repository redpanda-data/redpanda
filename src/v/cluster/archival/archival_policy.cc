/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/archival_policy.h"

#include "base/vlog.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "model/fundamental.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <utility>

constexpr size_t compacted_segment_size_multiplier{3};

namespace archival {

using namespace std::chrono_literals;

archival_policy::archival_policy(
  model::ntp ntp,
  cluster::partition& parent,
  std::optional<segment_time_limit> limit)
  : _ntp(std::move(ntp))
  , _upload_limit(limit)
  , _parent(&parent) {}

bool archival_policy::upload_deadline_reached() {
    if (!_upload_limit.has_value()) {
        return false;
    } else if (_upload_limit.value() == 0s) {
        // This code path is only used to trigger partial upload
        // in test envronment.
        return true;
    }
    auto now = ss::lowres_clock::now();

    if (!_upload_deadline.has_value()) {
        _upload_deadline = now + (*_upload_limit)();
    }
    return _upload_deadline < now;
}

ss::future<segment_collector_stream_result> archival_policy::get_next_segment(
  model::offset begin_inclusive,
  model::offset end_exclusive,
  std::optional<model::offset> flush_offset,
  ss::shared_ptr<storage::log> log,
  const cloud_storage::partition_manifest& manifest,
  ss::lowres_clock::duration segment_lock_duration) {
    std::optional<model::offset> end_inclusive;
    bool force_upload = flush_offset.has_value() || upload_deadline_reached();
    if (force_upload) {
        end_inclusive = flush_offset.value_or(
          model::prev_offset(end_exclusive));
    }
    vlog(
      archival_log.debug,
      "get_next_segment {}, begin_inclusive: {}, end_exclusive: {}, "
      "end_inclusive: {}, force_upload: {}",
      _ntp,
      begin_inclusive,
      end_exclusive,
      end_inclusive,
      force_upload);

    segment_collector segment_collector{
      segment_collector_mode::new_upload,
      begin_inclusive,
      manifest,
      *log,
      config::shard_local_cfg().cloud_storage_segment_size_target().value_or(
        config::shard_local_cfg().log_segment_size),
      end_inclusive,
      end_exclusive,
      flush_offset};

    segment_collector.collect_segments();
    if (!segment_collector.segment_ready_for_upload()) {
        co_return candidate_creation_error::no_segments_collected;
    }

    if (_upload_limit) {
        _upload_deadline = ss::lowres_clock::now() + _upload_limit.value()();
    }
    co_return co_await segment_collector.make_upload_candidate_stream(
      segment_lock_duration);
}

ss::future<segment_collector_stream_result>
archival_policy::get_next_compacted_segment(
  model::offset begin_inclusive,
  ss::shared_ptr<storage::log> log,
  const cloud_storage::partition_manifest& manifest,
  ss::lowres_clock::duration segment_lock_duration) {
    if (log->segment_count() == 0) {
        vlog(
          archival_log.warn,
          "Upload policy find next compacted segment: no segments ntp: {}",
          _ntp);
        co_return candidate_creation_error::no_segments_collected;
    }
    segment_collector compacted_segment_collector{
      segment_collector_mode::compacted_reupload,
      begin_inclusive,
      manifest,
      *log,
      config::shard_local_cfg().compacted_log_segment_size
        * compacted_segment_size_multiplier};

    compacted_segment_collector.collect_segments();
    if (!compacted_segment_collector.should_replace_manifest_segment()) {
        co_return candidate_creation_error::cannot_replace_manifest_entry;
    }

    co_return co_await compacted_segment_collector.make_upload_candidate_stream(
      segment_lock_duration);
}

} // namespace archival
