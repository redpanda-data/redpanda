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

#include "bytes/iobuf.h"
#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "reflection/adl.h"
#include "seastarx.h"
#include "serde/serde.h"

#include <seastar/core/sstring.hh>

#include <vector>

namespace compat {

/*
 * Specialize for types that cannot be copied.
 */
template<typename T>
std::tuple<T, T> compat_copy(T t) {
    return {t, t};
}

/*
 * Holder for binary serialization of a data structure along with the name of
 * the protocol used (e.g. adl, serde).
 */
struct compat_binary {
    ss::sstring name;
    iobuf data;

    // factory for serde
    template<typename T>
    static compat_binary serde(T v) {
        return {"serde", serde::to_iobuf(v)};
    }

    // factory for serde and adl
    template<typename T>
    static std::vector<compat_binary> serde_and_adl(T v) {
        auto&& [a, b] = compat_copy(std::move(v));
        return {
          compat_binary::serde(std::move(a)),
          compat_binary("adl", reflection::to_iobuf(std::move(b))),
        };
    }

    compat_binary(ss::sstring name, iobuf data)
      : name(std::move(name))
      , data(std::move(data)) {}

    compat_binary(compat_binary&&) noexcept = default;
    compat_binary& operator=(compat_binary&&) noexcept = default;

    /*
     * copy ctor/assignment are defined for ease of use, which would normally be
     * disabled due to the move-only iobuf data member.
     */
    compat_binary(const compat_binary& other)
      : name(other.name)
      , data(other.data.copy()) {}

    compat_binary& operator=(const compat_binary& other) {
        if (this != &other) {
            name = other.name;
            data = other.data.copy();
        }
        return *this;
    }

    ~compat_binary() = default;
};

/*
 * Interface for creating a compatibility test.
 */
template<typename T>
struct compat_check {
    /*
     * Generate test cases. It's good to build some random cases, as well as
     * convering edge cases such as min/max values or empty values, etc...
     */
    static std::vector<T> create_test_cases();

    /*
     * Serialize an instance of T to the JSON writer.
     */
    static void to_json(T, json::Writer<json::StringBuffer>&);

    /*
     * Deserialize an instance of T from the JSON value.
     */
    static T from_json(json::Value&);

    /*
     * Serialize an instance of T to supported binary formats.
     */
    static std::vector<compat_binary> to_binary(T);

    /*
     * Check compatibility. Ensure that the instance of T (from JSON) matches
     * the binary encoded instance of T.
     */
    static bool check(T, compat_binary);
};

/*
 * Helper that compares an instance of T with an adl or serde instance.
 */
template<typename T>
bool verify_adl_or_serde(T expected, compat_binary test) {
    const auto decoded = [&] {
        if (test.name == "adl") {
            return reflection::from_iobuf<T>(std::move(test.data));
        } else if (test.name == "serde") {
            return serde::from_iobuf<T>(std::move(test.data));
        } else {
            vassert(false, "unknown type {}", test.name);
        }
    }();

    if (expected != decoded) {
        fmt::print(
          "Verify of {{{}}} decoding failed:\nExpected: {}\nDecoded: {}\n",
          test.name,
          expected,
          decoded);
        return false;
    }

    return true;
}

} // namespace compat
