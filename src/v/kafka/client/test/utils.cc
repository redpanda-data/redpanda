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

#include "kafka/client/test/utils.h"

#include "random/generators.h"

#include <random>

namespace kafka::client::testing {

iobuf random_length_iobuf(size_t data_max) {
    assert(data_max > 0);
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1, data_max);

    auto data = random_generators::gen_alphanum_string(dist(rng));
    iobuf b;
    b.append(data.data(), data.size());
    return b;
}

} // namespace kafka::client::testing
