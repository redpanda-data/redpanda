/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/timestamp.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace cloud_topics::l1 {

compaction_sink::compaction_sink(
  io* io,
  compaction_committer* committer,
  model::topic_id_partition tp,
  object_builder::options opts)
  : _io(io)
  , _committer(committer)
  , _tp(tp)
  , _opts(opts) {}

bool compaction_sink::needs_roll() const {
    // TODO: This needs to consider L1 object size and what-not eventually.
    return !_active_staging_file;
}

ss::future<> compaction_sink::commit_update_and_roll() {
    if (!_active_staging_file) {
        co_return;
    }

    auto active_staging_file = std::exchange(_active_staging_file, nullptr);
    auto builder = std::exchange(_builder, nullptr);

    auto object_info_fut = co_await ss::coroutine::as_future(builder->finish());
    co_await builder->close();
    if (object_info_fut.failed()) {
        auto e = object_info_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                        : ss::log_level::error,
          "Exception creating object_info: {}. Exiting compaction early.",
          e);
        co_await active_staging_file->remove();
        std::rethrow_exception(e);
    }
    auto object_info = object_info_fut.get();

    auto [first, last] = object_info.index.partitions.equal_range(_tp);
    vassert(
      std::distance(first, last) == 1,
      "Expected one partition range in builder.");
    size_t length = 0;
    size_t file_position = 0;
    kafka::offset first_offset{};
    kafka::offset last_offset{};
    model::timestamp max_timestamp{};

    for (auto it = first; it != last; ++it) {
        length += it->second.length;
        file_position = it->second.file_position;
        first_offset = it->second.first_offset;
        last_offset = it->second.last_offset;
        max_timestamp = it->second.max_timestamp;
    }

    auto ntp_md = metastore::object_metadata::ntp_metadata{
      .tidp = _tp,
      .base_offset = first_offset,
      .last_offset = last_offset,
      .max_timestamp = max_timestamp,
      .pos = file_position,
      .size = length};

    auto out = object_output_t{
      .ntp_md = std::move(ntp_md),
      .info = std::move(object_info),
      .staging_file = std::move(active_staging_file)};

    _committer->push_update(std::move(out));
}

ss::future<> compaction_sink::maybe_roll() {
    if (!needs_roll()) {
        co_return;
    }

    co_await commit_update_and_roll();

    auto staging_file_fut = co_await ss::coroutine::as_future(
      _io->create_tmp_file());

    if (staging_file_fut.failed()) {
        auto e = staging_file_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                        : ss::log_level::error,
          "Exception creating staging file: {}",
          e);
        std::rethrow_exception(e);
    }
    auto staging_file_result = staging_file_fut.get();

    _active_staging_file = std::move(staging_file_result).value();
    auto output_stream = co_await _active_staging_file->output_stream();

    _builder = object_builder::create(std::move(output_stream), _opts);

    co_await _builder->start_partition(_tp);

    co_return;
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    co_await maybe_roll();
    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }
    co_await _builder->add_batch(std::move(b));
    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::finalize() { co_await commit_update_and_roll(); }

} // namespace cloud_topics::l1
