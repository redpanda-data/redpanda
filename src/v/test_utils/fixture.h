#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

// clang-format off
#define __FIXTURE_JOIN_(a, b) a ##_## b
#define __FIXTURE_JOIN(a, b) __FIXTURE_JOIN_(a, b)
// clang-format on
#define FIXTURE_TEST(method, klass)                                            \
    struct __FIXTURE_JOIN(klass, method) final : klass {                       \
        void fixture_test();                                                   \
    };                                                                         \
    SEASTAR_THREAD_TEST_CASE(method) {                                         \
        struct __FIXTURE_JOIN(klass, method) _fixture_driver;                  \
        _fixture_driver.fixture_test();                                        \
    }                                                                          \
    void ::__FIXTURE_JOIN(klass, method)::fixture_test()
