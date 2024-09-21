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

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"

#include <climits>
#include <cstdint>

namespace wasm::parser::leb128 {

template<typename int_type>
bytes encode(int_type value) {
    bytes output;

    static_assert(
      sizeof(int_type) == sizeof(uint32_t)
        || sizeof(int_type) == sizeof(uint64_t),
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned leading_bit_mask = 0x80U;
    constexpr unsigned sign_bit_mask = 0x40U;
    if constexpr (std::is_signed_v<int_type>) {
        bool more = true;
        while (more) {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= CHAR_BIT - 1;
            if (
              ((value == 0) && !(byte & sign_bit_mask))
              || ((value == -1) && (byte & sign_bit_mask))) {
                more = false;
            } else {
                byte |= leading_bit_mask;
            }
            output.push_back(byte);
        }
    } else {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while)
        do {
            uint8_t byte = value & lower_seven_bits_mask;
            value >>= CHAR_BIT - 1;
            if (value != 0) {
                byte |= leading_bit_mask;
            }
            output.push_back(byte);
        } while (value != 0);
    }

    return output;
}

class decode_exception : std::exception {};

template<typename int_type>
int_type decode(iobuf_parser_base* stream) {
    constexpr unsigned lower_seven_bits_mask = 0x7FU;
    constexpr unsigned continuation_bit_mask = 0x80U;
    static_assert(
      sizeof(int_type) == sizeof(uint32_t)
        || sizeof(int_type) == sizeof(uint64_t),
      "Only 32bit and 64bit integers are supported");
    constexpr unsigned size = sizeof(int_type) * CHAR_BIT;
    int_type result = 0;
    unsigned shift = 0;
    uint8_t byte = continuation_bit_mask;
    while ((shift < size) && (byte & continuation_bit_mask)) {
        byte = stream->consume_type<uint8_t>();
        result |= int_type(byte & lower_seven_bits_mask) << shift;
        shift += CHAR_BIT - 1;
    }

    if (byte & continuation_bit_mask) [[unlikely]] {
        // Overflow!
        throw decode_exception();
    }

    if constexpr (std::is_signed_v<int_type>) {
        constexpr unsigned size = sizeof(int_type) * CHAR_BIT;
        constexpr unsigned sign_bit_mask = 0x40U;
        if ((shift < size) && (byte & sign_bit_mask)) {
            // sign extend
            result |= ~int_type(0) << shift;
        }
    }

    return result;
}

} // namespace wasm::parser::leb128
