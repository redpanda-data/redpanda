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

#include "utils/xid.h"

#include "serde/rw/array.h"
#include "serde/rw/rw.h"
#include "ssx/sformat.h"

#include <fmt/format.h>

namespace {
static constexpr std::array<char, 32> b32_alphabet{
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
  'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
  'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v'};

using decoder_t = std::array<uint8_t, 256>;

constexpr decoder_t build_decoder_table() {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    std::array<uint8_t, 256> ret = {0xff};

    for (uint8_t i = 0; i < b32_alphabet.size(); ++i) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        ret[b32_alphabet[i]] = i;
    }
    return ret;
}

static constexpr decoder_t decoder = build_decoder_table();
} // namespace

invalid_xid::invalid_xid(std::string_view current_string)
  : _msg(ssx::sformat("String '{}' is not a valid xid", current_string)) {}

xid xid::from_string(std::string_view str) {
    if (str.size() != str_size) {
        throw invalid_xid(str);
    }
    // NOLINTBEGIN(cppcoreguidelines-pro-bounds-constant-array-index,
    // readability-magic-numbers, hicpp-signed-bitwise)
    data_t data;
    data[11] = decoder[str[17]] << 6u | decoder[str[18]] << 1u
               | decoder[str[19]] >> 4u;

    // check the last byte
    if (b32_alphabet[(data[11] << 4) & 0x1F] != str[19]) {
        throw invalid_xid(str);
    }

    data[10] = decoder[str[16]] << 3u | decoder[str[17]] >> 2u;
    data[9] = decoder[str[14]] << 5u | decoder[str[15]];
    data[8] = decoder[str[12]] << 7u | decoder[str[13]] << 2u
              | decoder[str[14]] >> 3u;
    data[7] = decoder[str[11]] << 4u | decoder[str[12]] >> 1;
    data[6] = decoder[str[9]] << 6u | decoder[str[10]] << 1u
              | decoder[str[11]] >> 4u;
    data[5] = decoder[str[8]] << 3u | decoder[str[9]] >> 2u;
    data[4] = decoder[str[6]] << 5u | decoder[str[7]];
    data[3] = decoder[str[4]] << 7u | decoder[str[5]] << 2u
              | decoder[str[6]] >> 3u;
    data[2] = decoder[str[3]] << 4u | decoder[str[4]] >> 1u;
    data[1] = decoder[str[1]] << 6u | decoder[str[2]] << 1
              | decoder[str[3]] >> 4u;
    data[0] = decoder[str[0]] << 3u | decoder[str[1]] >> 2u;
    // NOLINTEND(cppcoreguidelines-pro-bounds-constant-array-index,
    // readability-magic-numbers, hicpp-signed-bitwise)
    return xid(data);
}

fmt::format_context::iterator
fmt::formatter<xid>::format(const xid& id, format_context& ctx) const {
    static constexpr uint8_t mask = 0x1f;
    // NOLINTBEGIN(cppcoreguidelines-pro-bounds-constant-array-index,
    // readability-magic-numbers, hicpp-signed-bitwise)
    fmt::format_to(
      ctx.out(),
      "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
      b32_alphabet[id._data[0] >> 3u],
      b32_alphabet[((id._data[1] >> 6u) & mask) | ((id._data[0] << 2u) & mask)],
      b32_alphabet[(id._data[1] >> 1) & mask],
      b32_alphabet[((id._data[2] >> 4) & mask) | ((id._data[1] << 4) & mask)],
      b32_alphabet[id._data[3] >> 7 | ((id._data[2] << 1) & mask)],
      b32_alphabet[(id._data[3] >> 2) & mask],
      b32_alphabet[id._data[4] >> 5 | ((id._data[3] << 3) & mask)],
      b32_alphabet[id._data[4] & mask],
      b32_alphabet[id._data[5] >> 3],
      b32_alphabet[((id._data[6] >> 6) & mask) | ((id._data[5] << 2) & mask)],
      b32_alphabet[(id._data[6] >> 1) & mask],
      b32_alphabet[((id._data[7] >> 4) & mask) | ((id._data[6] << 4) & mask)],
      b32_alphabet[id._data[8] >> 7 | ((id._data[7] << 1) & mask)],
      b32_alphabet[(id._data[8] >> 2) & mask],
      b32_alphabet[(id._data[9] >> 5) | ((id._data[8] << 3) & mask)],
      b32_alphabet[id._data[9] & mask],
      b32_alphabet[id._data[10] >> 3],
      b32_alphabet
        [((uint8_t)(id._data[11] >> 6) & mask)
         | ((uint8_t)(id._data[10] << 2) & mask)],
      b32_alphabet[(id._data[11] >> 1) & mask],
      b32_alphabet[(id._data[11] << 4) & mask]);

    // NOLINTEND(cppcoreguidelines-pro-bounds-constant-array-index,
    // readability-magic-numbers, hicpp-signed-bitwise)
    return ctx.out();
}

void read_nested(iobuf_parser& in, xid& id, const size_t bytes_left_limit) {
    serde::read_nested(in, id._data, bytes_left_limit);
}

void write(iobuf& out, xid id) { serde::write(out, id._data); }

std::istream& operator>>(std::istream& i, xid& id) {
    ss::sstring s;
    i >> s;
    id._data = xid::from_string(s)._data;
    return i;
}
