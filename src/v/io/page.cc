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
    vassert(!test_flag(flags::faulting), "Cannot access faulting page data");
    return data_;
}

template<typename T>
auto underlying(T type) {
    return static_cast<std::underlying_type_t<T>>(type);
}

void page::set_flag(flags flag) noexcept {
    assert(flag != flags::num_flags);
    flags_.set(underlying(flag));
}

void page::clear_flag(flags flag) noexcept {
    assert(flag != flags::num_flags);
    flags_.reset(underlying(flag));
}

bool page::test_flag(flags flag) const noexcept {
    assert(flag != flags::num_flags);
    return flags_.test(underlying(flag));
}

} // namespace experimental::io
