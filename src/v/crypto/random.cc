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

#include "crypto/crypto.h"
#include "internal.h"
#include "ssl_utils.h"
#include "thirdparty/openssl/rand.h"

namespace crypto {
bytes_span<> generate_random(bytes_span<> buf, use_private_rng private_rng) {
    if (private_rng) {
        if (1 != RAND_priv_bytes_ex(nullptr, buf.data(), buf.size(), 0)) {
            throw internal::ossl_error(
              "Failed to get random output from private RNG");
        }
    } else {
        if (1 != RAND_bytes_ex(nullptr, buf.data(), buf.size(), 0)) {
            throw internal::ossl_error(
              "Failed to get random output from public RNG");
        }
    }

    return buf;
}

bytes generate_random(size_t len, use_private_rng private_rng) {
    bytes ret(bytes::initialized_later(), len);
    generate_random(ret, private_rng);
    return ret;
}

namespace {
template<typename T>
T generate_random(use_private_rng private_rng) {
    std::array<bytes::value_type, sizeof(T)> x{};
    generate_random(x, private_rng);
    return std::bit_cast<T>(x);
}
} // namespace

template<>
secure_private_rng::result_type secure_private_rng::operator()() {
    return generate_random<secure_private_rng::result_type>(
      use_private_rng::yes);
}

template<>
secure_public_rng::result_type secure_public_rng::operator()() {
    return generate_random<secure_public_rng::result_type>(use_private_rng::no);
}
} // namespace crypto
