// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "storage/mvlog/segment_appender.h"

#include "base/vlog.h"
#include "model/record.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/logger.h"
#include "storage/record_batch_utils.h"

namespace storage::experimental::mvlog {

ss::future<> segment_appender::append(model::record_batch batch) {
    // Build the body of the entry first so we can checksum its size.
    record_batch_entry_body entry_body;
    entry_body.term = batch.term();
    entry_body.record_batch_header.append(
      storage::batch_header_to_disk_iobuf(batch.header()));
    entry_body.records.append(std::move(batch).release_data());
    auto entry_body_buf = serde::to_iobuf(std::move(entry_body));

    // Now build the header with the checksum.
    auto body_size = static_cast<int32_t>(entry_body_buf.size_bytes());
    entry_header entry_hdr{
      entry_header_crc(body_size, entry_type::record_batch),
      body_size,
      entry_type::record_batch,
    };
    auto orig_size = file_->size();
    iobuf buf;
    buf.append(entry_header_to_iobuf(entry_hdr));
    buf.append(std::move(entry_body_buf));
    co_await file_->append(std::move(buf));
    vlog(
      log.trace,
      "Appended offsets [{}, {}], pos [{}, {})",
      batch.base_offset(),
      batch.last_offset(),
      orig_size,
      file_->size());
}

} // namespace storage::experimental::mvlog
