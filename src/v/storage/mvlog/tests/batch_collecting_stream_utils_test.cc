// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "hashing/crc32.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/mvlog/batch_collecting_stream_utils.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/record_batch_utils.h"
#include "storage/types.h"

#include <seastar/core/io_priority_class.hh>

#include <gtest/gtest.h>

namespace storage::experimental::mvlog {

TEST(BatchCollectingStreamTest, TestBasicStream) {
    iobuf buf;
    auto in_batches
      = model::test::make_random_batches(model::offset{0}, 10, true).get();
    for (int i = 0; i < 10; i++) {
        in_batches[i].header().ctx.term = model::term_id{i};
    }
    for (const auto& b : in_batches) {
        record_batch_entry_body entry_body;
        entry_body.record_batch_header.append(
          batch_header_to_disk_iobuf(b.header()));
        entry_body.records.append(b.data().copy());
        entry_body.term = model::term_id{1};

        auto entry_body_buf = serde::to_iobuf(std::move(entry_body));
        auto body_size = static_cast<int32_t>(entry_body_buf.size_bytes());
        entry_header entry_hdr{
          entry_header_crc(body_size, entry_type::record_batch),
          body_size,
          entry_type::record_batch,
        };
        buf.append(entry_header_to_iobuf(entry_hdr));
        buf.append(std::move(entry_body_buf));
    }

    log_reader_config cfg{
      model::offset{0}, model::offset::max(), ss::default_priority_class()};
    batch_collector collector(cfg, model::term_id{1}, 128_MiB);
    entry_stream entries(make_iobuf_input_stream(std::move(buf)));

    auto res = collect_batches_from_stream(entries, collector).get();
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(res.value(), collect_stream_outcome::end_of_stream);

    auto out_batches = collector.release_batches();
    ASSERT_EQ(out_batches.size(), 10);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(in_batches[i], out_batches[i]);
    }
}

} // namespace storage::experimental::mvlog
