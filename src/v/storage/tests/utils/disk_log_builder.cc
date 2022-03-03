// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/tests/utils/disk_log_builder.h"

#include "storage/disk_log_appender.h"
#include "storage/types.h"

#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>

using namespace std::chrono_literals; // NOLINT

// util functions to be moved from storage_fixture
// make_ntp, make_dir etc
namespace storage {
disk_log_builder::disk_log_builder(storage::log_config config)
  : _log_config(std::move(config))
  , _storage(
      [this]() {
          return kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            _log_config.base_dir,
            debug_sanitize_files::yes);
      },
      [this]() { return _log_config; }) {}

// Batch generation
ss::future<> disk_log_builder::add_random_batch(
  model::offset offset,
  int num_records,
  maybe_compress_batches comp,
  model::record_batch_type bt,
  log_append_config config,
  should_flush_after flush) {
    auto buff = ss::circular_buffer<model::record_batch>();
    buff.push_back(
      test::make_random_batch(offset, num_records, bool(comp), bt));
    return write(std::move(buff), config, flush);
}

ss::future<> disk_log_builder::add_random_batches(
  model::offset offset,
  int count,
  maybe_compress_batches comp,
  log_append_config config,
  should_flush_after flush) {
    return write(
      test::make_random_batches(offset, count, bool(comp)), config, flush);
}

ss::future<> disk_log_builder::add_random_batches(
  model::offset offset, log_append_config config, should_flush_after flush) {
    return write(test::make_random_batches(offset), config, flush);
}

ss::future<> disk_log_builder::add_batch(
  model::record_batch batch,
  log_append_config config,
  should_flush_after flush) {
    auto buf = ss::circular_buffer<model::record_batch>();
    buf.push_back(std::move(batch));
    return write(std::move(buf), config, flush);
}
// Log managment
ss::future<> disk_log_builder::start(model::ntp ntp) {
    return start(ntp_config(std::move(ntp), get_log_config().base_dir));
}

ss::future<> disk_log_builder::start(storage::ntp_config cfg) {
    return _storage.start().then([this, cfg = std::move(cfg)]() mutable {
        return _storage.log_mgr()
          .manage(std::move(cfg))
          .then([this](storage::log log) { _log = log; });
    });
}

ss::future<> disk_log_builder::truncate(model::offset o) {
    return get_log().truncate(
      storage::truncate_config(o, ss::default_priority_class()));
}

ss::future<> disk_log_builder::gc(
  model::timestamp collection_upper_bound,
  std::optional<size_t> max_partition_retention_size) {
    return get_log().compact(compaction_config(
      collection_upper_bound,
      max_partition_retention_size,
      ss::default_priority_class(),
      _abort_source));
}

ss::future<> disk_log_builder::stop() { return _storage.stop(); }

// Low lever interface access
// Access log impl
log& disk_log_builder::get_log() {
    vassert(_log.has_value(), "Log is unintialized. Please use start() first");
    return *_log;
}

disk_log_impl& disk_log_builder::get_disk_log_impl() {
    return *reinterpret_cast<disk_log_impl*>(_log->get_impl());
}

segment_set& disk_log_builder::get_log_segments() {
    auto& segment_set = get_disk_log_impl().segments();
    vassert(!segment_set.empty(), "There are no segments in the segment_set");
    return segment_set;
}

segment& disk_log_builder::get_segment(size_t index) {
    auto& segment_set = get_log_segments();
    vassert(
      index < segment_set.size(), "There are no segments in the segment_set");
    return *std::next(segment_set.begin(), index)->get();
}

segment_index& disk_log_builder::get_seg_index_ptr(size_t index) {
    return get_segment(index).index();
}

// Create segments
ss::future<> disk_log_builder::add_segment(
  model::offset offset, model::term_id term, ss::io_priority_class pc) {
    return get_disk_log_impl().new_segment(offset, term, pc);
}

// Configuration getters
const log_config& disk_log_builder::get_log_config() const {
    return _log_config;
}

// Common interface for appending batches
ss::future<> disk_log_builder::write(
  ss::circular_buffer<model::record_batch> buff,
  const log_append_config& config,
  should_flush_after flush) {
    if (buff.empty()) {
        return ss::now();
    }
    auto base_offset = buff.front().base_offset();
    auto reader = model::make_memory_record_batch_reader(std::move(buff));
    // we do not use the log::make_appender method to be able to controll the
    // appender base offset and insert holes into the log
    disk_log_appender appender(
      get_disk_log_impl(), config, log_clock::now(), base_offset);
    return std::move(reader)
      .for_each_ref(std::move(appender), config.timeout)
      .then([this, flush](storage::append_result ar) {
          _bytes_written += ar.byte_size;
          if (flush) {
              return _log->flush();
          }
          return ss::now();
      });
}

} // namespace storage
