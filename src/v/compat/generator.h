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
#include <model/timeout_clock.h>

#include <vector>

// Utility to create a generator for empty objects.
#define EMPTY_COMPAT_GENERATOR(class_name)                                     \
    template<>                                                                 \
    struct instance_generator<class_name> {                                    \
        static class_name random() { return {}; }                              \
        static std::vector<class_name> limits() { return {}; }                 \
    };

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

// NOTE: the bottom 10^6 is clamped.
// This is so that roundtrip from ns->ms->ns will work as expected.
inline model::timeout_clock::duration min_duration() {
    constexpr auto min_chrono = std::chrono::milliseconds::min().count();
    return model::timeout_clock::duration(min_chrono - (min_chrono % -1000000));
}
inline model::timeout_clock::duration max_duration() {
    constexpr auto max_chrono = std::chrono::milliseconds::max().count();
    return model::timeout_clock::duration(max_chrono - max_chrono % 1000000);
}

} // namespace compat
