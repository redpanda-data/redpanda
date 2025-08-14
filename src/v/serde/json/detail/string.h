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
#include "bytes/iobuf.h"

#include <seastar/core/temporary_buffer.hh>

namespace serde::json::detail {

/// An incremental parser for JSON strings.
class string_parser {
private:
    enum class state {
        start,
        in_string,
        in_escape,
        in_unicode,
        surrogate_start,
        surrogate_escape,
        in_utf8_sequence,
        finished_with_error,
        finished_with_value,
    };

public:
    enum class result {
        need_more_data,
        invalid_json_string,
        done,
    };

public:
    /// Return the number of bytes read from the buffer.
    ///
    /// The buffer is not modified but sequences of bytes might be shared with
    /// the iobuf returned by value() so they must not be modified as long as
    /// the parser is running or resulting value is not disposed of.
    ///
    /// advance() should be called as long as error is result::need_more_data.
    /// If result is result::done, the parser is done and value() can be called
    /// to get the result.
    size_t advance(ss::temporary_buffer<char>& buf, result& err);

    /// Return the result of the parsing.
    /// Can be called only if a previous call to advance() returned
    /// result::done.
    iobuf value() && {
        if (_state != state::finished_with_value) {
            throw std::runtime_error(
              "string_parser is not done, cannot get value");
        }

        return std::move(_sink);
    }

private:
    state _state{state::start};
    iobuf _sink;

    uint8_t _unicode_index{0};
    std::array<char, 8> _unicode_buffer{};

    // UTF-8 sequence tracking for incremental parsing
    uint8_t _utf8_bytes_remaining{0};
    uint32_t _utf8_codepoint{0};
};

} // namespace serde::json::detail
