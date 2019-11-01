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
#define FIXTURE_TEST(method, klass)                                            \
    class __FIXTURE_JOIN(klass, method) final : klass {                        \
    public:                                                                    \
        void fixture_test();                                                   \
        template<typename... T>                                                \
        auto info(T&&... t) {                                                  \
            return _seastar_test_log.info(std::forward<T>(t)...);              \
        }                                                                      \
        template<typename... T>                                                \
        auto debug(T&&... t) {                                                 \
            return _seastar_test_log.debug(std::forward<T>(t)...);             \
        }                                                                      \
                                                                               \
    private:                                                                   \
        seastar::logger _seastar_test_log{"" #klass "::" #method};             \
    };                                                                         \
    SEASTAR_THREAD_TEST_CASE(method) {                                         \
        BOOST_TEST_CHECKPOINT("" << #klass << "::" << #method << "()");        \
        struct __FIXTURE_JOIN(klass, method) _fixture_driver;                  \
        _fixture_driver.fixture_test();                                        \
        BOOST_TEST_CHECKPOINT("~" << #klass << "::" << #method << "()");       \
    }                                                                          \
    void ::__FIXTURE_JOIN(klass, method)::fixture_test()
