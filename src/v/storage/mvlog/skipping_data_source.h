// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

namespace experimental::io {
class pager;
} // namespace experimental::io

namespace storage::experimental::mvlog {

class skipping_data_source final : public seastar::data_source_impl {
public:
    struct read_interval {
        uint64_t offset;
        uint64_t length;
    };
    using read_list_t = ss::chunked_fifo<read_interval>;
    skipping_data_source(::experimental::io::pager* pager, read_list_t reads)
      : reads_(std::move(reads))
      , pager_(pager) {}

    ss::future<ss::temporary_buffer<char>> get() noexcept override;

private:
    read_list_t reads_;
    std::optional<ss::input_stream<char>> cur_stream_;
    ::experimental::io::pager* pager_;
};

} // namespace storage::experimental::mvlog
