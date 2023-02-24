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
#include "reflection/async_adl.h"
#include "seastarx.h"
#include "serde/serde.h"

#include <seastar/core/sstring.hh>

#include <vector>

// Utility that generates compat check class for empty objects.
#define EMPTY_COMPAT_CHECK(class_name)                                         \
    template<>                                                                 \
    struct compat_check<class_name> {                                          \
        static constexpr std::string_view name = #class_name;                  \
        static std::vector<class_name> create_test_cases() {                   \
            return generate_instances<class_name>();                           \
        }                                                                      \
        static void to_json(class_name, json::Writer<json::StringBuffer>&) {}  \
        static class_name from_json(json::Value&) { return class_name{}; }     \
        static std::vector<compat_binary> to_binary(class_name obj) {          \
            return compat_binary::serde_and_adl(obj);                          \
        }                                                                      \
        static void check(class_name obj, compat_binary test) {                \
            verify_adl_or_serde(obj, std::move(test));                         \
        }                                                                      \
    };

#define EMPTY_COMPAT_CHECK_SERDE_ONLY(class_name)                              \
    template<>                                                                 \
    struct compat_check<class_name> {                                          \
        static constexpr std::string_view name = #class_name;                  \
        static std::vector<class_name> create_test_cases() {                   \
            return generate_instances<class_name>();                           \
        }                                                                      \
        static void to_json(class_name, json::Writer<json::StringBuffer>&) {}  \
        static class_name from_json(json::Value&) { return class_name{}; }     \
        static std::vector<compat_binary> to_binary(class_name obj) {          \
            return {compat_binary::serde(obj)};                                \
        }                                                                      \
        static void check(class_name obj, compat_binary test) {                \
            verify_serde_only(obj, std::move(test));                           \
        }                                                                      \
    };

namespace compat {

struct compat_error final : public std::runtime_error {
public:
    explicit compat_error(const std::string& msg)
      : std::runtime_error(msg) {}
};

/*
 * Specialize for types that cannot be copied.
 */
template<typename T>
std::pair<T, T> compat_copy(T t) {
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
        iobuf s_data;
        serde::write_async(s_data, std::move(v)).get();
        return {"serde", std::move(s_data)};
    }

    // factory for serde and adl
    template<typename T>
    static std::vector<compat_binary> serde_and_adl(T v) {
        auto&& [a, b] = compat_copy(std::move(v));

        iobuf s_data;
        serde::write_async(s_data, std::move(a)).get();

        iobuf a_data;
        reflection::async_adl<T>{}.to(a_data, std::move(b)).get();

        auto ret = std::vector<compat_binary>{
          compat_binary{"serde", std::move(s_data)},
          compat_binary{"adl", std::move(a_data)}};

        return ret;
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
    static void check(T, compat_binary);
};

/*
 * Helpers that decode and compare an instance of T with an adl or serde
 * instance.
 */
template<typename T>
T decode_adl_or_serde(compat_binary test) {
    if (test.name == "adl") {
        iobuf_parser in(std::move(test.data));
        return reflection::async_adl<T>{}.from(in).get0();
    } else if (test.name == "serde") {
        iobuf_parser in(std::move(test.data));
        return serde::read_async<T>(in).get0();
    } else {
        vassert(false, "unknown type {}", test.name);
    }
}

template<typename T>
T decode_serde_only(compat_binary test) {
    vassert(test.name == "serde", "Non serde type encountered {}", test.name);
    iobuf_parser in(std::move(test.data));
    return serde::read_async<T>(in).get0();
}

template<typename T>
void verify_adl_or_serde(T expected, compat_binary test) {
    const auto name = test.name;
    auto decoded = decode_adl_or_serde<T>(std::move(test));
    if (expected != decoded) {
        throw compat_error(fmt::format(
          "Verify of {{{}}} decoding failed:\nExpected: {}\nDecoded: {}",
          name,
          expected,
          decoded));
    }
}

template<typename T>
void verify_serde_only(T expected, compat_binary test) {
    const auto name = test.name;
    auto decoded = decode_serde_only<T>(std::move(test));
    if (expected != decoded) {
        throw compat_error(fmt::format(
          "Verify of {{{}}} decoding failed:\nExpected: {}\nDecoded: {}",
          name,
          expected,
          decoded));
    }
}

#define GEN_COMPAT_CHECK(Type, ToJson, FromJson)                               \
    template<>                                                                 \
    struct compat_check<Type> {                                                \
        static constexpr std::string_view name = #Type;                        \
                                                                               \
        static std::vector<Type> create_test_cases() {                         \
            return generate_instances<Type>();                                 \
        }                                                                      \
                                                                               \
        static void to_json(Type obj, json::Writer<json::StringBuffer>& wr) {  \
            ToJson;                                                            \
        }                                                                      \
                                                                               \
        static Type from_json(json::Value& rd) {                               \
            Type obj;                                                          \
            FromJson;                                                          \
            return obj;                                                        \
        }                                                                      \
                                                                               \
        static std::vector<compat_binary> to_binary(Type obj) {                \
            return compat_binary::serde_and_adl(obj);                          \
        }                                                                      \
                                                                               \
        static void check(Type obj, compat_binary test) {                      \
            verify_adl_or_serde(obj, std::move(test));                         \
        }                                                                      \
    };

#define GEN_COMPAT_CHECK_SERDE_ONLY(Type, ToJson, FromJson)                    \
    template<>                                                                 \
    struct compat_check<Type> {                                                \
        static constexpr std::string_view name = #Type;                        \
                                                                               \
        static std::vector<Type> create_test_cases() {                         \
            return generate_instances<Type>();                                 \
        }                                                                      \
                                                                               \
        static void to_json(Type obj, json::Writer<json::StringBuffer>& wr) {  \
            ToJson;                                                            \
        }                                                                      \
                                                                               \
        static Type from_json(json::Value& rd) {                               \
            Type obj;                                                          \
            FromJson;                                                          \
            return obj;                                                        \
        }                                                                      \
                                                                               \
        static std::vector<compat_binary> to_binary(Type obj) {                \
            return {compat_binary::serde(obj)};                                \
        }                                                                      \
                                                                               \
        static void check(Type obj, compat_binary test) {                      \
            verify_serde_only(obj, std::move(test));                           \
        }                                                                      \
    };

} // namespace compat
