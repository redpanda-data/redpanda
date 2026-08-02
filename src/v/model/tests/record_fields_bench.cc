// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_fields.h"
#include "model/record_utils.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <cstdint>

namespace {

constexpr int32_t records_per_batch = 100;
constexpr size_t key_size = 16;

model::record_batch make_batch(size_t value_size) {
    iobuf body;
    for (int32_t i = 0; i < records_per_batch; ++i) {
        auto r = model::record(
          model::record_attributes(0),
          /*timestamp_delta=*/i,
          /*offset_delta=*/i,
          iobuf::from(random_generators::gen_alphanum_string(key_size)),
          iobuf::from(random_generators::gen_alphanum_string(value_size)),
          {});
        model::append_record_to_buffer(body, r);
    }
    model::record_batch_header hdr{};
    hdr.type = model::record_batch_type::raft_data;
    hdr.base_offset = model::offset(0);
    hdr.record_count = records_per_batch;
    hdr.last_offset_delta = records_per_batch - 1;
    hdr.first_timestamp = model::timestamp(0);
    hdr.max_timestamp = model::timestamp(records_per_batch - 1);
    hdr.reset_size_checksum_metadata(body);
    return {hdr, std::move(body), model::record_batch::tag_ctor_ng{}};
}

size_t run_full(const model::record_batch& b) {
    size_t cnt = 0;
    b.for_each_record([&cnt](model::record r) {
        perf_tests::do_not_optimize(r);
        ++cnt;
    });
    return cnt;
}

// The field set used when building a compaction index.
size_t run_key_fields(const model::record_batch& b) {
    size_t cnt = 0;
    b.for_each_record<
      model::record_field::offset_delta,
      model::record_field::key,
      model::record_field::is_tombstone>([&cnt](auto pr) {
        perf_tests::do_not_optimize(pr);
        ++cnt;
    });
    return cnt;
}

// The field set used by a batch timequery.
size_t run_timequery_fields(const model::record_batch& b) {
    size_t cnt = 0;
    b.for_each_record<
      model::record_field::timestamp_delta,
      model::record_field::offset_delta>([&cnt](auto pr) {
        perf_tests::do_not_optimize(pr);
        ++cnt;
    });
    return cnt;
}

size_t run_metadata(const model::record_batch& b, bool fully_parse) {
    size_t cnt = 0;
    b.for_each_record_metadata(
      [&cnt](model::record_metadata md) {
          perf_tests::do_not_optimize(md);
          ++cnt;
          return ss::stop_iteration::no;
      },
      fully_parse);
    return cnt;
}

size_t run_record_key(const model::record_batch& b) {
    size_t cnt = 0;
    b.for_each_record_key([&cnt](const model::record_key_metadata& k) {
        perf_tests::do_not_optimize(k);
        ++cnt;
    });
    return cnt;
}

class record_fields_bench {
protected:
    model::record_batch _batch_64b = make_batch(64);
    model::record_batch _batch_1kib = make_batch(1024);
    model::record_batch _batch_16kib = make_batch(16UL * 1024);
};

} // namespace

PERF_TEST_F(record_fields_bench, full_64b) { return run_full(_batch_64b); }
PERF_TEST_F(record_fields_bench, full_1kib) { return run_full(_batch_1kib); }
PERF_TEST_F(record_fields_bench, full_16kib) { return run_full(_batch_16kib); }

PERF_TEST_F(record_fields_bench, key_fields_64b) {
    return run_key_fields(_batch_64b);
}
PERF_TEST_F(record_fields_bench, key_fields_1kib) {
    return run_key_fields(_batch_1kib);
}
PERF_TEST_F(record_fields_bench, key_fields_16kib) {
    return run_key_fields(_batch_16kib);
}

PERF_TEST_F(record_fields_bench, timequery_fields_64b) {
    return run_timequery_fields(_batch_64b);
}
PERF_TEST_F(record_fields_bench, timequery_fields_1kib) {
    return run_timequery_fields(_batch_1kib);
}
PERF_TEST_F(record_fields_bench, timequery_fields_16kib) {
    return run_timequery_fields(_batch_16kib);
}

PERF_TEST_F(record_fields_bench, metadata_64b) {
    return run_metadata(_batch_64b, false);
}
PERF_TEST_F(record_fields_bench, metadata_1kib) {
    return run_metadata(_batch_1kib, false);
}
PERF_TEST_F(record_fields_bench, metadata_16kib) {
    return run_metadata(_batch_16kib, false);
}

PERF_TEST_F(record_fields_bench, metadata_full_parse_64b) {
    return run_metadata(_batch_64b, true);
}
PERF_TEST_F(record_fields_bench, metadata_full_parse_1kib) {
    return run_metadata(_batch_1kib, true);
}
PERF_TEST_F(record_fields_bench, metadata_full_parse_16kib) {
    return run_metadata(_batch_16kib, true);
}

PERF_TEST_F(record_fields_bench, record_key_64b) {
    return run_record_key(_batch_64b);
}
PERF_TEST_F(record_fields_bench, record_key_1kib) {
    return run_record_key(_batch_1kib);
}
PERF_TEST_F(record_fields_bench, record_key_16kib) {
    return run_record_key(_batch_16kib);
}
