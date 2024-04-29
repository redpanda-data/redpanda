// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/skipping_data_source.h"

#include "io/pager.h"
#include "io/paging_data_source.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>

using ::experimental::io::paging_data_source;

namespace storage::experimental::mvlog {

ss::future<ss::temporary_buffer<char>> skipping_data_source::get() noexcept {
    while (true) {
        // Initialize a stream if we haven't already or we've exhausted the
        // previous one.
        if (!cur_stream_) {
            if (reads_.empty()) {
                co_return ss::temporary_buffer<char>();
            }
            auto first_read = *reads_.begin();
            const auto end_pos = pager_->size();
            if (first_read.offset >= end_pos) {
                // Skip the read if starts past the end of the file.
                reads_.pop_front();
                continue;
            }
            const auto max_len = end_pos - first_read.offset;

            // Cap the read to the end of the file.
            cur_stream_ = ss::input_stream<char>(
              ss::data_source(std::make_unique<paging_data_source>(
                pager_,
                paging_data_source::config{
                  first_read.offset, std::min(max_len, first_read.length)})));
            reads_.pop_front();
        }
        // Keep using the stream until it hits the end of the stream and the
        // returned reads are empty.
        auto buf = co_await cur_stream_->read();
        if (buf.empty()) {
            cur_stream_.reset();
            continue;
        }
        co_return buf;
    }
}

} // namespace storage::experimental::mvlog
