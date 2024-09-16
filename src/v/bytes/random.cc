/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/random.h"

#include "random/generators.h"

#include <seastar/core/sstring.hh>

namespace random_generators {

bytes get_bytes(size_t n) {
    bytes b(bytes::initialized_later{}, n);
    std::generate_n(b.begin(), n, [] { return get_int<bytes::value_type>(); });
    return b;
}

bytes get_crypto_bytes(size_t n, bool use_private_rng) {
    bytes b(bytes::initialized_later{}, n);
    std::generate_n(b.begin(), n, [use_private_rng] {
        return get_int_secure<bytes::value_type>(use_private_rng);
    });
    return b;
}

iobuf make_iobuf(size_t n) {
    const auto b = gen_alphanum_string(n);
    iobuf io;
    io.append(b.data(), n);
    return io;
}

} // namespace random_generators
