#pragma once
#include "bytes/iobuf.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "model/fundamental.h"
#include "storage/parser.h"
#include "storage/segment_appender_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"

namespace cloud_storage {
static constexpr std::string_view manifest_payload = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 1004,
    "segments": {
        "1-2-v1.log": {
            "is_compacted": false,
            "size_bytes": 100,
            "committed_offset": 2,
            "base_offset": 1
        }
    }
})json";
static const auto manifest_namespace = model::ns("test-ns");    // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::revision_id(0); // NOLINT
static const ss::sstring manifest_url = ssx::sformat(        // NOLINT
  "20000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());
// NOLINTNEXTLINE
static const ss::sstring segment_url
  = "ce4fd1a3/test-ns/test-topic/42_0/1-2-v1.log";

static const std::vector<s3_imposter_fixture::expectation>
  default_expectations({
    s3_imposter_fixture::expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)},
    s3_imposter_fixture::expectation{
      .url = "/" + segment_url, .body = "segment1"},
  });

inline iobuf iobuf_deep_copy(const iobuf& i) {
    iobuf res;
    for (const auto& f : i) {
        res.append(f.get(), f.size());
    }
    return res;
};

inline iobuf generate_segment(model::offset base_offset, int count) {
    auto buff = storage::test::make_random_batches(base_offset, count, false);

    iobuf result;
    for (auto&& batch : buff) {
        auto hdr = storage::disk_header_to_iobuf(batch.header());
        result.append(std::move(hdr));
        result.append(iobuf_deep_copy(batch.data()));
    }
    return result;
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

    stop_parser consume_batch_end() override { return stop_parser::no; }

    void print(std::ostream& o) const override {
        o << "counting_record_consumer";
    }

    std::vector<model::record_batch_header>& headers;
    std::vector<iobuf>& records;
    std::vector<uint64_t>& file_offsets;
};

} // namespace cloud_storage
