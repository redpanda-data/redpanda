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
#include "thirdparty/base64/libbase64.h"

#include <seastar/core/sstring.hh>

#include <absl/strings/escaping.h>

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
    std::string srv;
    if (!absl::WebSafeBase64Unescape(data, &srv)) {
        throw base64_url_decoder_exception{};
    }
    // NOLINTNEXTLINE
    return {reinterpret_cast<bytes::const_pointer>(srv.data()), srv.size()};
}

iobuf base64_to_iobuf(const iobuf& buf) {
    base64_state state{};
    base64_stream_decode_init(&state, 0);
    iobuf out;
    for (const details::io_fragment& frag : buf) {
        iobuf::fragment out_frag{frag.size()};
        size_t written{};
        if (
          1
          != base64_stream_decode(
            &state, frag.get(), frag.size(), out_frag.get_write(), &written)) {
            throw base64_decoder_exception{};
        }
        out_frag.reserve(written);
        out.append(std::move(out_frag).release());
    }
    return out;
}
