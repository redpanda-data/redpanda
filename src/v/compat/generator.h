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
#pragma once
#include <vector>

namespace compat {

/*
 * Protocol for building random instance generators.
 */
template<typename T>
struct instance_generator {
    /*
     * Return a random instance of T.
     */
    static T random();

    /*
     * Return instances of T which may be interesting for testing. Typically
     * this involves selecting for edge cases like min/max/empty/large values.
     */
    static std::vector<T> limits();
};

/*
 * Combine from all instance generator interfaces.
 */
template<typename T>
auto generate_instances(size_t randoms = 1) -> std::vector<T> {
    using generator = instance_generator<T>;
    auto res = generator::limits();
    for (size_t i = 0; i < randoms; i++) {
        res.push_back(generator::random());
    }
    return res;
}

} // namespace compat
