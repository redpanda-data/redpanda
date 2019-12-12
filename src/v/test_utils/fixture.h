#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>

// clang-format off
#define __FIXTURE_JOIN_(a, b) a ##_## b
#define __FIXTURE_JOIN(a, b) __FIXTURE_JOIN_(a, b)
// clang-format on
#define FIXTURE_TEST_EXPECTED_FAILURES(method, klass, failures)                \
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
        static inline seastar::logger g_seastar_test_log{"" #klass             \
                                                         "::" #method};        \
    };                                                                         \
    SEASTAR_THREAD_TEST_CASE_EXPECTED_FAILURES(method, failures) {             \
        BOOST_TEST_CHECKPOINT("" << #klass << "::" << #method << "()");        \
        struct __FIXTURE_JOIN(klass, method) _fixture_driver;                  \
        _fixture_driver.fixture_test();                                        \
        BOOST_TEST_CHECKPOINT("~" << #klass << "::" << #method << "()");       \
    }                                                                          \
    void ::__FIXTURE_JOIN(klass, method)::fixture_test()

#define FIXTURE_TEST(method, klass)                                            \
    FIXTURE_TEST_EXPECTED_FAILURES(method, klass, 0)
