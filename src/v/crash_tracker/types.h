/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "base/vfmt.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"

#include <memory>
#include <ostream>

namespace crash_tracker {

enum class crash_type {
    unknown,
    startup_exception,
    segfault,
    abort,
    illegal_instruction,
    assertion,
    oom
};

std::ostream& operator<<(std::ostream&, crash_type);

/// reserved_string is a simple wrapper around a std::array that allows
/// pre-allocating a string of a certain size with trailing '\0's and allows
/// safer access to the populated part of the string.
template<size_t Size>
struct reserved_string {
    static_assert(Size > 0, "Size must be greater than 0");
    // Add an extra byte for the trailing '\0'
    using underlying_t = std::array<char, Size + 1>;
    using type = typename underlying_t::value_type;
    using pointer = typename underlying_t::pointer;
    using const_pointer = typename underlying_t::const_pointer;
    using reference = typename underlying_t::reference;
    using const_reference = typename underlying_t::const_reference;
    using iterator = typename underlying_t::iterator;

    reserved_string() = default;
    explicit reserved_string(ss::sstring other) noexcept {
        vassert(
          other.size() <= Size,
          "String size {} exceeds reserved size {}",
          other.size(),
          Size);
        std::copy(other.begin(), other.end(), _str->begin());
    };

    iterator begin() { return _str->begin(); }
    iterator end() { return _str->end(); }

    const_pointer c_str() const noexcept { return _str->data(); }

    reference operator[](size_t i) { return (*_str)[i]; }
    const_reference operator[](size_t i) const { return (*_str)[i]; }

    /// The length of the populated, non-'\0' prefix of the string.
    size_t length() const noexcept { return strlen(_str->data()); }

    /// The full capacity of the string, including the all-'\0' suffix.
    constexpr size_t capacity() const noexcept { return Size; }

private:
    std::unique_ptr<underlying_t> _str{std::make_unique<underlying_t>()};
};

template<std::size_t Size>
void tag_invoke(
  serde::tag_t<serde::write_tag>, iobuf& out, reserved_string<Size> t) {
    static_assert(Size <= std::numeric_limits<serde::serde_size_t>::max());
    serde::write(out, static_cast<serde::serde_size_t>(t.length()));
    auto begin = t.begin();
    // Note: we only serialize the first `length()` characters of the string and
    // not the full `capacity()`
    for (size_t i = 0; i < t.length(); ++i) {
        serde::write(out, *(begin++));
    }
}

template<std::size_t Size>
void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  reserved_string<Size>& t,
  const std::size_t bytes_left_limit) {
    using Type = typename reserved_string<Size>::type;
    const auto size = serde::read_nested<serde::serde_size_t>(
      in, bytes_left_limit);
    if (unlikely(size > t.capacity())) {
        throw serde::serde_exception(fmt::format(
          "reading reserved_string, size {} exceeds reserved size {}",
          size,
          t.capacity()));
    }
    for (auto i = 0U; i < size; ++i) {
        t[i] = serde::read_nested<Type>(in, bytes_left_limit);
    }
}

struct crash_description
  : serde::
      envelope<crash_description, serde::version<0>, serde::compat_version<0>> {
    // We pre-allocate the memory necessary for the buffers needed to fill a
    // crash_description and overestimate its serialized size to be able to
    // pre-allocate a sufficiently large iobuf to write it to
    constexpr static size_t string_buffer_reserve = 4096;
    constexpr static size_t overhead_overestimate = 1024;
    constexpr static size_t serde_size_overestimate
      = overhead_overestimate + 2 * string_buffer_reserve;
    using reserved_string_t = reserved_string<string_buffer_reserve>;

    crash_type type{};
    model::timestamp crash_time;
    reserved_string_t crash_message;
    reserved_string_t stacktrace;
    ss::sstring app_version;
    ss::sstring arch;

    crash_description() = default;

    auto serde_fields() {
        return std::tie(
          type, crash_time, crash_message, stacktrace, app_version, arch);
    }
};

struct crash_tracker_metadata
  : serde::envelope<
      crash_tracker_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    uint32_t crash_count{0};
    uint64_t config_checksum{0};
    model::timestamp last_start_ts;

    auto serde_fields() {
        return std::tie(crash_count, config_checksum, last_start_ts);
    }
};

class crash_loop_limit_reached : public std::runtime_error {
public:
    explicit crash_loop_limit_reached()
      : std::runtime_error("Crash loop detected, aborting startup.") {}
};

bool is_crash_loop_limit_reached(std::exception_ptr);

} // namespace crash_tracker

VFMT_DECL(crash_tracker::crash_description);
