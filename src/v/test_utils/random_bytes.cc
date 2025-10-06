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
#include "test_utils/random_bytes.h"

#include "bytes/bytes.h"
#include "bytes/details/io_fragment.h"
#include "bytes/iobuf.h"

#include <algorithm>
#include <cstdlib>
#include <memory>

namespace tests {

bytes random_bytes(size_t n, rng_t& rng) {
    bytes b(bytes::initialized_later{}, n);
    std::generate_n(
      b.begin(), n, [&] { return rng.get_int<bytes::value_type>(); });
    return b;
}

iobuf random_iobuf(size_t n, rng_t& rng) {
    const auto b = rng.gen_alphanum_string(n);
    iobuf io;
    io.append(b.data(), n);
    return io;
}

iobuf fragmented_iobuf(std::string_view str, int n_fragments, rng_t& rng) {
    auto remaining_len = str.length();
    std::vector<size_t> fragment_lengths;
    fragment_lengths.reserve(n_fragments);
    for (int i = 0; i < n_fragments - 1; ++i) {
        size_t fragment_len = rng.get_int(remaining_len);
        fragment_lengths.push_back(fragment_len);
        remaining_len -= fragment_len;
    }
    fragment_lengths.push_back(remaining_len);
    std::ranges::shuffle(fragment_lengths, rng.engine());

    iobuf res;
    auto cur = str.begin();
    for (auto fragment_len : fragment_lengths) {
        // force add a new fragment
        res.append(
          std::make_unique<details::io_fragment>(
            ss::temporary_buffer<char>(cur, fragment_len)));
        cur += fragment_len;
    }
    return res;
}

} // namespace tests
