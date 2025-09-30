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

#include "random/secure_generators.h"

#include "crypto/crypto.h"
#include "random/generators.h"

#include <algorithm>

namespace random_generators {

using internal::chars;

namespace internal {
thread_local crypto::secure_private_rng secure_private_rng{};
thread_local crypto::secure_public_rng secure_public_rng{};
} // namespace internal

template<typename T>
T get_int_secure(bool use_private) {
    std::uniform_int_distribution<T> dist;
    if (use_private) {
        return dist(internal::secure_private_rng);
    } else {
        return dist(internal::secure_public_rng);
    }
}

// only explicitly instantiate the types we use
template unsigned char get_int_secure<unsigned char>(bool);

ss::sstring gen_alphanum_string_secure(size_t n) {
    // do not include \0
    static constexpr std::size_t max_index = chars.size() - 2;
    std::uniform_int_distribution<size_t> dist(0, max_index);
    auto s = ss::uninitialized_string(n);

    std::generate_n(s.begin(), n, [&dist] {
        return chars[dist(internal::secure_public_rng)];
    });
    return s;
}

} // namespace random_generators
