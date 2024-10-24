// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "storage/mvlog/batch_collecting_stream_utils.h"

#include "base/vlog.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/logger.h"
#include "storage/record_batch_utils.h"

namespace storage::experimental::mvlog {

std::ostream& operator<<(std::ostream& o, collect_stream_outcome out) {
    switch (out) {
    case collect_stream_outcome::buffer_full:
        return o << "collect_stream_outcome::buffer_full";
    case collect_stream_outcome::end_of_stream:
        return o << "collect_stream_outcome::end_of_stream";
    case collect_stream_outcome::stop:
        return o << "collect_stream_outcome::stop";
    }
}

ss::future<result<collect_stream_outcome, errc>>
collect_batches_from_stream(entry_stream& entries, batch_collector& collector) {
    while (true) {
        auto entry_res = co_await entries.next();
        if (entry_res.has_error()) {
            co_return entry_res.error();
        }
        auto& entry = entry_res.value();
        if (!entry.has_value()) {
            co_return collect_stream_outcome::end_of_stream;
        }
        switch (entry->hdr.type) {
        case entry_type::record_batch: {
            auto res = collect_batch_from_buf(
              std::move(entry->body), collector);
            if (res.has_error()) {
                co_return res.error();
            }
            switch (res.value()) {
            // Pass the result through so the collector can be reset if
            // needed...
            case reader_outcome::buffer_full:
                co_return collect_stream_outcome::buffer_full;
            case reader_outcome::stop:
                co_return collect_stream_outcome::stop;

            // ...otherwise, the stream is not done yet and we should continue.
            case reader_outcome::success:
                [[fallthrough]];
            case reader_outcome::skip:
                continue;
            }
            break;
        }
        }
    }
}

result<reader_outcome, errc>
collect_batch_from_buf(iobuf body_buf, batch_collector& collector) {
    auto entry_body = serde::from_iobuf<record_batch_entry_body>(
      std::move(body_buf));
    auto batch_header = storage::batch_header_from_disk_iobuf(
      std::move(entry_body.record_batch_header));
    auto computed_crc = model::internal_header_only_crc(batch_header);
    if (computed_crc != batch_header.header_crc) {
        return errc::checksum_mismatch;
    }
    auto term_res = collector.set_term(entry_body.term);
    if (term_res.has_error() || term_res.value() != reader_outcome::success) {
        return term_res;
    }
    vlog(
      log.trace,
      "Collecting offsets [{}, {}]",
      batch_header.base_offset,
      batch_header.last_offset());
    return collector.add_batch(batch_header, std::move(entry_body.records));
}

} // namespace storage::experimental::mvlog
