/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * This file includes code from RapidJSON (https://rapidjson.org/)
 *
 * Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
 *
 * Licensed under the MIT License (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "serde/json/detail/numeric.h"

#include "serde/json/detail/strod.h"

namespace serde::json::detail {

size_t numeric_parser::advance(ss::temporary_buffer<char>& buf, result& err) {
    size_t pos = 0;

    while (pos < buf.size()) {
        switch (_state) {
        case state::start:
            if (buf[pos] == '-') {
                _is_negative = true;
                pos += 1;
            }

            _state = state::first_decimal;
            continue;

        case state::first_decimal:
            // We parse a single leading zero for fractional values (0.1), but
            // otherwise leading zeroes are not allowed.
            if (buf[pos] == '0') {
                _state = state::decimal_point;
                pos += 1;
                continue;
            } else if (buf[pos] >= '1' && buf[pos] <= '9') {
                _state = state::decimals;
                [[fallthrough]];
            } else {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

        case state::decimals:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                // int 64 max = 9,223,372,036,854,775,807
                // int 64 min = -9,223,372,036,854,775,808
                constexpr auto max_int_value
                  = std::numeric_limits<int64_t>::max() / 10;

                // Check if the next decimal place will overflow a 64-bit
                // integer. Don't forget to take the 7 in the ones' place (8 for
                // negative) into account. (int64 max)/10 =
                // 922,337,203,685,477,580, remainder 7.
                // (-int64 min)/10 = 922,337,203,685,477,580, remainder 8.
                if (
                  _u64_acc > max_int_value
                  || (_u64_acc == max_int_value && buf[pos] - '0' > 7 + _is_negative)) {
                    // Switch to double.
                    _double_acc = static_cast<double>(_u64_acc);
                    _as_double = true;
                    _state = state::decimals_as_double;
                    continue;
                }

                _u64_acc = _u64_acc * 10
                           + static_cast<unsigned>(buf[pos] - '0');
                _significand_digits += 1;
                pos += 1;
                continue;
            } else {
                _state = state::decimal_point;
                continue;
            }

        case state::decimals_as_double:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                _double_acc = _double_acc * 10
                              + static_cast<unsigned>(buf[pos] - '0');
                _significand_digits += 1;
                pos += 1;
                continue;
            } else {
                _state = state::decimal_point;
                continue;
            }

        case state::decimal_point:
            if (buf[pos] == '.') {
                pos += 1;
                if (!_as_double) {
                    _as_double = true;
                    _double_acc = static_cast<double>(_u64_acc);
                }
                _state = state::fractional_part_first_digit;
                continue;
            } else if (buf[pos] == 'e' || buf[pos] == 'E') {
                pos += 1;
                _state = state::exponent_sign;
                continue;
            } else if (buf[pos] >= '0' && buf[pos] <= '9') {
                // We expected a decimal point but got a digit.
                // Doesn't make sense in a JSON stream.
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            } else {
                err = result::done;
                if (_as_double) {
                    _state = state::finished_with_double;
                    if (_double_acc > std::numeric_limits<double>::max()) {
                        err = result::invalid_json_string;
                        _state = state::finished_with_error;
                        return pos;
                    }
                    if (_is_negative) {
                        _double_acc = -_double_acc;
                    }
                } else {
                    _state = state::finished_with_int;

                    constexpr auto max_representable = static_cast<uint64_t>(
                      std::numeric_limits<int64_t>::max());
                    vassert(
                      _u64_acc <= max_representable + _is_negative,
                      "Unexpected value in accumulator {}",
                      _u64_acc);
                }

                return pos;
            }

        case state::fractional_part_first_digit:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                _state = state::fractional_part;
                continue;
            } else {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

        case state::fractional_part:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                if (_significand_digits < 17) {
                    _double_acc = _double_acc * 10
                                  + static_cast<unsigned>(buf[pos] - '0');
                    _significand_digits += 1;
                    --_exp_frac;
                }
                pos += 1;
                continue;
            } else if (buf[pos] == 'e' || buf[pos] == 'E') {
                pos += 1;
                _state = state::exponent_sign;
                continue;
            } else {
                err = result::done;
                _state = state::finished_with_double;

                _double_acc = strtod_normal_precision(
                  _double_acc, _exp_frac + _exp * (_exp_negative ? -1 : 1));
                if (_double_acc > std::numeric_limits<double>::max()) {
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }
                if (_is_negative) {
                    _double_acc = -_double_acc;
                }

                return pos;
            }

        case state::exponent_sign:
            if (!_as_double) {
                _as_double = true;
                _double_acc = static_cast<double>(_u64_acc);
            }

            if (buf[pos] == '-') {
                pos += 1;
                _exp_negative = true;
                _max_exp = (_exp_frac + 2147483639) / 10;
            } else {
                if (buf[pos] == '+') {
                    pos += 1;
                }

                _max_exp = 308 - _exp_frac;
            }
            _state = state::exponent_first_digit;
            continue;

        case state::exponent_first_digit:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                _state = state::exponent_digit;
                [[fallthrough]];
            } else {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

        case state::exponent_digit:
            if (buf[pos] >= '0' && buf[pos] <= '9') {
                bool skip = true;
                if (_exp <= _max_exp) {
                    _exp = _exp * 10 + static_cast<unsigned>(buf[pos] - '0');
                    pos += 1;
                    skip = false;
                }

                if (_exp > _max_exp) {
                    if (_exp_negative) {
                        // we are close to 0, drop the rest of the digits
                        pos += skip;
                        continue;
                    } else {
                        // we are maxing out the double to a number we won't be
                        // able to represent in a double. We should stop
                        // parsing.
                        err = result::invalid_json_string;
                        _state = state::finished_with_error;
                        return pos;
                    }
                }
                continue;
            } else {
                err = result::done;
                _state = state::finished_with_double;

                _double_acc = strtod_normal_precision(
                  _double_acc, _exp_frac + _exp * (_exp_negative ? -1 : 1));
                if (_double_acc > std::numeric_limits<double>::max()) {
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }

                if (_is_negative) {
                    _double_acc = -_double_acc;
                }

                return pos;
            }

        case state::finished_with_error:
            [[fallthrough]];

        case state::finished_with_int:
            [[fallthrough]];

        case state::finished_with_double:
            throw std::runtime_error(
              "numeric_parser is already done and is not reusable");
        }
    }

    err = result::need_more_data;
    return pos;
}

} // namespace serde::json::detail
