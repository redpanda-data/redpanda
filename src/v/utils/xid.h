/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "bytes/bytes.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

#include <array>
#include <cstdint>
#include <exception>

class invalid_xid final : public std::exception {
public:
    explicit invalid_xid(const ss::sstring&);
    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/**
 * Class representing XID
 *
 * Based on (https://github.com/rs/xid)
 */
class xid {
public:
    // number of bytes in an underlying data structure
    static constexpr size_t size = 12;
    // size of xid string representation
    static constexpr size_t str_size = 20;

    using data_t = std::array<uint8_t, size>;

    explicit constexpr xid(data_t data)
      : _data(data) {}
    // default constructor to make the xid type lexically castable from string
    constexpr xid() = default;

    xid(const xid&) = default;
    xid(xid&&) = default;
    xid& operator=(const xid&) = default;
    xid& operator=(xid&&) = default;
    ~xid() = default;
    friend bool operator==(const xid&, const xid&) = default;

    operator bytes_view() { return to_bytes_view(_data); }

    template<typename H>
    friend H AbslHashValue(H h, const xid& id) {
        return H::combine(std::move(h), id._data);
    }

    static xid from_string(const ss::sstring&);

    friend std::ostream& operator<<(std::ostream&, const xid&);

    friend std::istream& operator>>(std::istream&, xid&);

private:
    friend struct fmt::formatter<xid>;
    data_t _data;
};

template<>
struct fmt::formatter<xid> : fmt::formatter<std::string_view> {
    fmt::format_context::iterator
    format(const xid& id, format_context& ctx) const;
};
