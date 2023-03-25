// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/core/temporary_buffer.hh>

struct fixture {
    storage::disk_log_builder b{storage::log_config(
      storage::random_dir(),
      1_GiB,
      storage::debug_sanitize_files::yes,
      ss::default_priority_class(),
      storage::with_cache::no)};
    ~fixture() { b.stop().get(); }
};

FIXTURE_TEST(half_next_page, fixture) {
    using namespace storage; // NOLINT
    // gurantee next half page on 4096 segments(default)
    const size_t data_size = (config::shard_local_cfg().append_chunk_size() / 2)
                             + 1;
    ss::temporary_buffer<char> data(data_size);
    auto key = iobuf();
    auto value = iobuf();
    value.append(data.share());
    key.append(data.share());
    auto batchbldr = record_batch_builder(
      model::record_batch_type::raft_data, model::offset(0));
    auto batch = std::move(
                   batchbldr.add_raw_kv(std::move(key), std::move(value)))
                   .build();
    b | start() | add_segment(0);
    auto& seg = b.get_segment(0);
    info("About to append batch: {}", batch);
    seg.append(std::move(batch)).get();
    info("Segment: {}", seg);
    seg.flush().get();
    b | add_random_batch(1, 1, maybe_compress_batches::yes);
    auto recs = b.consume().get0();
    BOOST_REQUIRE_EQUAL(recs.size(), 2);
    for (auto& rec : recs) {
        BOOST_REQUIRE_EQUAL(rec.header().crc, model::crc_record_batch(rec));
    }
}
