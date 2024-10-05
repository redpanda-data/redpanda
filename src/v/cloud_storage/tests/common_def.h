/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "bytes/iobuf.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/parser.h"
#include "storage/record_batch_utils.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <boost/numeric/conversion/cast.hpp>
#include <boost/test/tools/interface.hpp>

namespace cloud_storage {
static const auto manifest_namespace = model::kafka_namespace;  // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::initial_revision_id(0); // NOLINT
inline const auto archiver_term = model::term_id{123};
static const ss::sstring manifest_url = ssx::sformat( // NOLINT
  "10000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());
static const ss::sstring manifest_serde_url = ssx::sformat(
  "10000000/meta/{}_{}/manifest.bin", manifest_ntp.path(), manifest_revision());

static const auto json_manifest_format_path = std::pair{
  manifest_format::json,
  remote_manifest_path(std::filesystem::path(manifest_url))};
static const auto serde_manifest_format_path = std::pair{
  manifest_format::serde,
  remote_manifest_path(std::filesystem::path(manifest_serde_url))};

inline iobuf iobuf_deep_copy(const iobuf& i) {
    iobuf res;
    for (const auto& f : i) {
        res.append(f.get(), f.size());
    }
    return res;
};

inline iobuf generate_segment(model::offset base_offset, int count) {
    auto buff
      = model::test::make_random_batches(base_offset, count, false).get();
    iobuf result;
    for (auto&& batch : buff) {
        auto hdr = storage::batch_header_to_disk_iobuf(batch.header());
        result.append(std::move(hdr));
        result.append(iobuf_deep_copy(batch.data()));
    }
    return result;
}

struct batch_t {
    int num_records;
    model::record_batch_type type;
    std::vector<size_t> record_sizes;
    /// Set to true to completely erase the batch from the segment
    /// to check compacted segments.
    bool hole{false};
    std::optional<model::timestamp> timestamp{std::nullopt};
};

/// \returns generated batches and committed offset + 1
inline std::pair<ss::circular_buffer<model::record_batch>, model::offset>
make_random_batches(model::offset o, const std::vector<batch_t>& batches) {
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(batches.size());
    for (auto batch : batches) {
        if (batch.hole) {
            o += model::offset(batch.num_records);
            continue;
        }
        auto b = model::test::make_random_batch(
          o,
          batch.num_records,
          false,
          batch.type,
          batch.record_sizes.size()
              != boost::numeric_cast<size_t>(batch.num_records)
            ? std::nullopt
            : std::make_optional(batch.record_sizes),
          batch.timestamp);
        o = b.last_offset() + model::offset(1);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
    }
    return std::make_pair(std::move(ret), o);
}

/// \returns generated segment body and committed offset + 1 (next offset)
inline std::pair<iobuf, model::offset> generate_segment(
  model::offset base_offset, const std::vector<batch_t>& batches) {
    auto [buff, next_offset] = make_random_batches(base_offset, batches);
    iobuf result;
    for (auto&& batch : buff) {
        auto hdr = storage::batch_header_to_disk_iobuf(batch.header());
        result.append(std::move(hdr));
        result.append(iobuf_deep_copy(batch.data()));
    }
    return std::make_pair(std::move(result), next_offset);
}

class recording_batch_consumer : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    recording_batch_consumer(
      std::vector<model::record_batch_header>& headers,
      std::vector<iobuf>& records,
      std::vector<uint64_t>& file_offsets)
      : headers(headers)
      , records(records)
      , file_offsets(file_offsets) {}

    consume_result
    accept_batch_start(const model::record_batch_header&) const override {
        return consume_result::accept_batch;
    }

    void consume_batch_start(
      model::record_batch_header header,
      size_t physical_base_offset,
      size_t /*size_on_disk*/) override {
        file_offsets.push_back(physical_base_offset);
        headers.push_back(header);
    }

    void skip_batch_start(
      model::record_batch_header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {}

    void consume_records(iobuf&& ib) override {
        records.push_back(std::move(ib));
    }

    ss::future<stop_parser> consume_batch_end() override {
        co_return stop_parser::no;
    }

    void print(std::ostream& o) const override {
        o << "counting_record_consumer";
    }

    std::vector<model::record_batch_header>& headers;
    std::vector<iobuf>& records;
    std::vector<uint64_t>& file_offsets;
};

} // namespace cloud_storage
