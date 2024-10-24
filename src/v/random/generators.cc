/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/generators.h"

namespace random_generators {

void fill_buffer_randomchars(char* start, size_t amount) {
    static std::uniform_int_distribution<int> rand_fill('@', '~');
    memset(start, rand_fill(internal::gen), amount);
}

static constexpr std::string_view chars
  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

ss::sstring gen_alphanum_string(size_t n, bool use_secure_rng) {
    // do not include \0
    static constexpr std::size_t max_index = chars.size() - 2;
    std::uniform_int_distribution<size_t> dist(0, max_index);
    auto s = ss::uninitialized_string(n);

    if (use_secure_rng) {
        std::generate_n(s.begin(), n, [&dist] {
            return chars[dist(internal::secure_public_rng)];
        });
    } else {
        std::generate_n(
          s.begin(), n, [&dist] { return chars[dist(internal::gen)]; });
    }
    return s;
}

ss::sstring gen_alphanum_max_distinct(size_t cardinality) {
    static constexpr std::size_t num_chars = chars.size() - 1;
    // everything is deterministic once you choose key_num
    auto key_num = get_int(cardinality - 1);
    auto next_index = key_num % num_chars;
    auto s = ss::uninitialized_string(alphanum_max_distinct_strlen);
    std::generate_n(s.begin(), alphanum_max_distinct_strlen, [&] {
        auto c = chars[next_index];
        next_index = (next_index + key_num) % num_chars;
        return c;
    });
    return s;
}

} // namespace random_generators
