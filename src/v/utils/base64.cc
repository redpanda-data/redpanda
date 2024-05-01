/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "utils/base64.h"

#include "base/vassert.h"

#include <seastar/core/sstring.hh>

#include <libbase64.h>

const int decode_url_table[128] = {
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, 52, 53, 54, 55, 56, 57, 58, 59, 60,
  61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
  11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1,
  63, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
  43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

// Required length is ceil(4n/3) rounded up to 4 bytes
static inline size_t encode_capacity(size_t input_size) {
    return (((4 * input_size) / 3) + 3) & ~0x3U;
}

template<typename S>
S base64_to_string_impl(std::string_view input) {
    using initialized_later = typename S::initialized_later;
    S output(initialized_later{}, input.size());
    size_t output_len; // NOLINT
    int ret = base64_decode(
      input.data(),
      input.size(),
      reinterpret_cast<char*>(output.data()), // NOLINT
      &output_len,
      0);
    if (unlikely(!ret)) {
        throw base64_decoder_exception();
    }
    vassert(
      output_len <= input.size(),
      "base64 decode overflow: {} > {}",
      output_len,
      input.size());
    output.resize(output_len);
    return output;
}

bytes base64_to_bytes(std::string_view input) {
    return base64_to_string_impl<bytes>(input);
}

ss::sstring base64_to_string(std::string_view input) {
    return base64_to_string_impl<ss::sstring>(input);
}

ss::sstring bytes_to_base64(bytes_view input) {
    const size_t output_capacity = encode_capacity(input.size());
    ss::sstring output(ss::sstring::initialized_later{}, output_capacity);
    size_t output_len; // NOLINT
    base64_encode(
      reinterpret_cast<const char*>(input.data()), // NOLINT
      input.size(),
      output.data(),
      &output_len,
      0);
    vassert(
      output_len <= output_capacity,
      "base64 encode overflow: {} > {}",
      output_len,
      output_capacity);
    output.resize(output_len);
    return output;
}

ss::sstring iobuf_to_base64(const iobuf& input) {
    const size_t output_capacity = encode_capacity(input.size_bytes());
    ss::sstring output(ss::sstring::initialized_later{}, output_capacity);
    size_t written = 0;

    base64_state state; // NOLINT
    base64_stream_encode_init(&state, 0);

    // encode each iobuf fragment
    iobuf::iterator_consumer input_it(input.cbegin(), input.cend());
    input_it.consume(
      input.size_bytes(),
      [&state, &written, &output, output_capacity](const char* src, size_t sz) {
          size_t output_len;                          // NOLINT
          char* output_ptr = output.data() + written; // NOLINT
          base64_stream_encode(&state, src, sz, output_ptr, &output_len);
          written += output_len;
          vassert(
            written <= output_capacity,
            "base64 encode overflow: {} > {}",
            written,
            output_capacity);
          return ss::stop_iteration::no;
      });

    // finalize output
    size_t output_len; // NOLINT
    base64_stream_encode_final(
      &state, output.data() + written, &output_len); // NOLINT
    written += output_len;
    vassert(
      written <= output_capacity,
      "base64 encode overflow: {} > {}",
      written,
      output_capacity);

    output.resize(written);
    return output;
}

bytes base64url_to_bytes(std::string_view data) {
    bytes rv{bytes::initialized_later{}, data.size()};
    unsigned int bits_collected = 0;
    unsigned int accumulator = 0;
    int pos = 0;
    for (const unsigned char c : data) {
        if (c > 127 || decode_url_table[c] < 0) {
            throw base64_url_decoder_exception();
        }
        accumulator = (accumulator << 6)
                      + static_cast<unsigned int>(decode_url_table[c]);
        bits_collected += 6;
        if (bits_collected >= 8) {
            auto val = static_cast<bytes::value_type>(
              (accumulator >> (bits_collected - 8)) & 0xffu);
            rv[pos++] = val;
            bits_collected -= 8;
        }
    }
    rv.resize(pos);
    return rv;
}
