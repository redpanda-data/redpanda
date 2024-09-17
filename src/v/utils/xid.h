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
#include "bytes/iobuf_parser.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

#include <array>
#include <cstdint>
#include <exception>

/**
 * Exception indicating that provided XID isn't valid f.e. incorrect length of
 * invalid characters
 */
class invalid_xid final : public std::exception {
public:
    explicit invalid_xid(std::string_view);
    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/**
 * Class representing an XID.
 *
 * The binary representation of the id has 12 bytes. String representation is
 * using base32 encoding (w/o padding) for better space efficiency when stored
 * in that form (20 bytes).
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

    /**
     * Creates an XID from string.
     *
     * The function parse and validate the string representation of XID.
     *
     * @return an xid decoded from the string provided
     */
    static xid from_string(std::string_view);

    friend bool operator==(const xid&, const xid&) = default;

    friend std::istream& operator>>(std::istream&, xid&);

    friend void
    read_nested(iobuf_parser& in, xid& id, const size_t bytes_left_limit);

    friend void write(iobuf& out, xid id);

    template<typename H>
    friend H AbslHashValue(H h, const xid& id) {
        return H::combine(std::move(h), id._data);
    }

private:
    friend struct fmt::formatter<xid>;
    data_t _data{0};
};

template<>
struct fmt::formatter<xid> : fmt::formatter<std::string_view> {
    fmt::format_context::iterator
    format(const xid& id, format_context& ctx) const;
};
