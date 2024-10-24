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

#include "base/vassert.h"

namespace experimental::io {

page::page(uint64_t offset, seastar::temporary_buffer<char> data)
  : offset_(offset)
  , size_(data.size())
  , data_(std::move(data)) {}

page::page(
  uint64_t offset,
  seastar::temporary_buffer<char> data,
  const class cache_hook& hook)
  : cache_hook(hook)
  , offset_(offset)
  , size_(data.size())
  , data_(std::move(data)) {
    vassert(
      hook.evicted(), "Non-evicted cache hook for page offset {}", offset_);
}

uint64_t page::offset() const noexcept { return offset_; }

uint64_t page::size() const noexcept { return size_; }

seastar::temporary_buffer<char>& page::data() noexcept { return data_; }

const seastar::temporary_buffer<char>& page::data() const noexcept {
    vassert(!test_flag(flags::faulting), "Cannot access faulting page data");
    return data_;
}

char* page::get_write() noexcept {
    vassert(!test_flag(flags::faulting), "Cannot access faulting page data");
    return data_.get_write();
}

void page::clear() { data_ = {}; }

template<typename T>
auto underlying(T type) {
    return static_cast<std::underlying_type_t<T>>(type);
}

void page::set_flag(flags flag) noexcept {
    flags_.set(underlying(flag));
    vassert(
      !(test_flag(flags::dirty) && test_flag(flags::faulting)),
      "Dirty and faulting states are mutually exclusive");
}

void page::clear_flag(flags flag) noexcept { flags_.reset(underlying(flag)); }

bool page::test_flag(flags flag) const noexcept {
    return flags_.test(underlying(flag));
}

void page::add_waiter(page::waiter& waiter) { waiters_.push_back(waiter); }

void page::signal_waiters() {
    waiters_.clear_and_dispose(
      [](page::waiter* waiter) { waiter->ready.set_value(); });
}

} // namespace experimental::io
