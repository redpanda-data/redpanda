/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/page.h"

namespace experimental::io {

page::page(uint64_t offset, seastar::temporary_buffer<char> data)
  : offset_(offset)
  , size_(data.size())
  , data_(std::move(data)) {}

uint64_t page::offset() const noexcept { return offset_; }

uint64_t page::size() const noexcept { return size_; }

seastar::temporary_buffer<char>& page::data() noexcept { return data_; }

const seastar::temporary_buffer<char>& page::data() const noexcept {
    return data_;
}

} // namespace experimental::io
