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

namespace random_generators {

bytes get_bytes(size_t n) {
    auto b = ss::uninitialized_string<bytes>(n);
    std::generate_n(b.begin(), n, [] { return get_int<bytes::value_type>(); });
    return b;
}

iobuf make_iobuf(size_t n) {
    const auto b = gen_alphanum_string(n);
    iobuf io;
    io.append(b.data(), n);
    return io;
}

} // namespace random_generators
