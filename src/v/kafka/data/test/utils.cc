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

#include "kafka/data/test/utils.h"

#include "random/generators.h"

namespace kafka::data::testing {

iobuf random_length_iobuf(size_t data_max) {
    assert(data_max > 0);
    auto sz = random_generators::get_int(size_t{1}, data_max);
    auto data = random_generators::gen_alphanum_string(sz);
    iobuf b;
    b.append(data.data(), data.size());
    return b;
}

} // namespace kafka::data::testing
