// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "model/tests/random_batch.h"
#include "storage/segment_utils.h"
#include "utils/disk_log_builder.h"

#include <seastar/core/fstream.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/short_streams.hh>

using namespace storage;
using namespace model;

static ss::logger test_logger{"self_compact_test"};

namespace {
ss::sstring iobuf_to_string(iobuf buf) {
    auto input_stream = make_iobuf_input_stream(std::move(buf));
    return ss::util::read_entire_stream_contiguous(input_stream).get();
}
} // namespace

SEASTAR_THREAD_TEST_CASE(test_self_compact_segment) {
    disk_log_builder b;
    b | start() | add_segment(0);
    auto stop = ss::defer([&b] { b.stop().get(); });

    auto first_data_batch = make_random_batch(test::record_batch_spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 10,
      .max_key_cardinality = 2,
      .bt = record_batch_type::raft_data});

    auto config_batch = make_random_batch(test::record_batch_spec{
      .offset = model::offset{10},
      .allow_compression = false,
      .count = 1,
      .max_key_cardinality = 1,
      .bt = record_batch_type::archival_metadata});

    auto second_data_batch = make_random_batch(test::record_batch_spec{
      .offset = model::offset{11},
      .allow_compression = false,
      .count = 10,
      .max_key_cardinality = 1,
      .bt = record_batch_type::raft_data});

    std::vector<record_batch> batches;
    batches.emplace_back(std::move(first_data_batch));
    batches.emplace_back(std::move(config_batch));
    batches.emplace_back(std::move(second_data_batch));

    auto seg = b.get_log_segments().front();
    for (auto& b : batches) {
        seg->append(std::move(b)).get();
    }

    seg->flush().get();
    seg->mark_as_compacted_segment();

    b | add_segment(21);

    vlog(test_logger.info, "Appended batches");

    auto batches_before = b.consume().get();
    std::vector<record> records_before;
    for (const auto& b : batches_before) {
        auto recs_in_b = b.copy_records();
        records_before.insert(
          records_before.end(),
          std::make_move_iterator(recs_in_b.begin()),
          std::make_move_iterator(recs_in_b.end()));
    }

    vlog(test_logger.info, "Reading log before compaction");

    ss::abort_source as;

    vlog(test_logger.info, "Doing self compaction");
    seg->appender().close().get();
    seg->release_appender().get();

    compaction_config cfg{
      model::offset{records_before.size()}, ss::default_priority_class(), as};
    b.apply_compaction(cfg).get();

    vlog(test_logger.info, "Reading log after compaction");
    auto batches_after = b.consume().get();
    std::vector<record> records_after;
    for (const auto& b : batches_after) {
        auto recs_in_b = b.copy_records();
        records_after.insert(
          records_after.end(),
          std::make_move_iterator(recs_in_b.begin()),
          std::make_move_iterator(recs_in_b.end()));
    }

    vlog(test_logger.info, "Batches before compaction:");
    for (const auto& b : batches_before) {
        vlog(
          test_logger.info,
          "[{}, {}] count={} size={}",
          b.base_offset()(),
          b.last_offset()(),
          b.record_count(),
          b.memory_usage());
    }

    vlog(test_logger.info, "Batches after compaction:");
    for (const auto& b : batches_after) {
        vlog(
          test_logger.info,
          "[{}, {}] count={} size={}",
          b.base_offset()(),
          b.last_offset()(),
          b.record_count(),
          b.memory_usage());
    }

    vlog(test_logger.info, "Records before compaction:");
    auto offset = 0;
    for (const auto& r : records_before) {
        ss::sstring value{"null"};
        if (r.has_value()) {
            value = iobuf_to_string(r.value().copy());
        }

        vlog(
          test_logger.info,
          "[{}] {} -> {}",
          offset,
          iobuf_to_string(r.key().copy()),
          value);
        ++offset;
    }

    vlog(test_logger.info, "Records after compaction:");
    offset = 0;
    for (const auto& r : records_after) {
        ss::sstring value{"null"};
        if (r.has_value()) {
            value = iobuf_to_string(r.value().copy());
        }

        vlog(
          test_logger.info,
          "[{}] {} -> {}",
          offset,
          iobuf_to_string(r.key().copy()),
          value);
        ++offset;
    }
}
