/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>

// clang-format off
#define __FIXTURE_JOIN_(a, b) a ##_## b
#define __FIXTURE_JOIN(a, b) __FIXTURE_JOIN_(a, b)
// clang-format on
#define FIXTURE_TEST(method, klass)                                            \
    class __FIXTURE_JOIN(klass, method) final : klass {                        \
    public:                                                                    \
        void fixture_test();                                                   \
        template<typename... T>                                                \
        static auto info(T&&... t) {                                           \
            return g_seastar_test_log.info(std::forward<T>(t)...);             \
        }                                                                      \
        template<typename... T>                                                \
        static auto debug(T&&... t) {                                          \
            return g_seastar_test_log.debug(std::forward<T>(t)...);            \
        }                                                                      \
                                                                               \
    private:                                                                   \
        static inline ss::logger g_seastar_test_log{"" #klass "::" #method};   \
    };                                                                         \
    SEASTAR_THREAD_TEST_CASE(method) {                                         \
        BOOST_TEST_CHECKPOINT("" << #klass << "::" << #method << "()");        \
        __FIXTURE_JOIN(klass, method) _fixture_driver;                         \
        _fixture_driver.fixture_test();                                        \
        BOOST_TEST_CHECKPOINT("~" << #klass << "::" << #method << "()");       \
    }                                                                          \
    void ::__FIXTURE_JOIN(klass, method)::fixture_test()

namespace test_utils {

class setenv_helper {
public:
    using overwrite = seastar::bool_class<struct overwrite_tag>;
    setenv_helper(
      std::string_view name,
      std::string_view value,
      overwrite ovr = overwrite::no)
      : _name(name) {
        setenv(name.data(), value.data(), ovr ? 1 : 0);
    }
    ~setenv_helper() { unsetenv(_name.c_str()); }
    setenv_helper(const setenv_helper&) = delete;
    setenv_helper& operator=(const setenv_helper&) = delete;
    setenv_helper(setenv_helper&&) = delete;
    setenv_helper& operator=(setenv_helper&&) = delete;

private:
    seastar::sstring _name;
};

} // namespace test_utils
