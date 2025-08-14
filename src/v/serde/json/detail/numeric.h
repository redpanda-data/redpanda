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

#include <seastar/core/temporary_buffer.hh>

namespace serde::json::detail {

/// An incremental parser for JSON numeric types (ints, doubles).
class numeric_parser {
private:
    enum class state {
        start,
        first_decimal,
        decimals,
        decimals_as_double,
        decimal_point,
        fractional_part_first_digit,
        fractional_part,
        exponent_sign,
        exponent_first_digit,
        exponent_digit,
        finished_with_error,
        finished_with_int,
        finished_with_double,
    };

public:
    enum class result {
        need_more_data,
        invalid_json_string,
        done,
    };

    /// Return the number of bytes read from the buffer.
    ///
    /// The buffer is not modified but sequences of bytes might be temporarily
    /// shared with the parser so they must not be modified until the parser is
    /// done.
    ///
    /// advance() should be called as long as error is result::need_more_data.
    /// If result is result::done, the parser is done and int64() or double()
    /// can be called to get the result.
    size_t advance(ss::temporary_buffer<char>& buf, result& err);

    /// Signal to the parser that there will be no more input.
    /// After this method returns the parser will be done or in an error state.
    /// Equivalent to calling advance with input a single space character.
    size_t finalize(result& err) {
        const char space = ' ';
        ss::temporary_buffer<char> space_buf(&space, 1);
        return advance(space_buf, err);
    }

    bool is_int() const {
        if (_state == state::finished_with_int) {
            return true;
        } else if (_state == state::finished_with_double) {
            return false;
        }

        throw std::runtime_error(
          "numeric_parser is not done, cannot get value");
    }

    /// Return the result of the parsing.
    /// Can be called only if a previous call to advance() returned
    /// result::done.
    int64_t value_int64() && {
        if (_state != state::finished_with_int) {
            throw std::runtime_error(
              "numeric_parser is not done or does not hold an int");
        }

        if (_is_negative) {
            return static_cast<int64_t>(~_u64_acc + 1);
        } else {
            return static_cast<int64_t>(_u64_acc);
        }
    }

    /// Return the result of the parsing.
    /// Can be called only if a previous call to advance() returned
    /// result::done.
    double value_double() && {
        if (_state != state::finished_with_double) {
            throw std::runtime_error(
              "numeric_parser is not done or does not hold a double");
        }

        return _double_acc;
    }

private:
    state _state{state::start};

    bool _is_negative{false};
    uint8_t _significand_digits{0};

    uint64_t _u64_acc{0};

    bool _as_double{false};
    double _double_acc{0.0};

    int _exp_frac = 0;
    int _max_exp = 0;

    bool _exp_negative{false};
    int _exp = 0;
};

} // namespace serde::json::detail
