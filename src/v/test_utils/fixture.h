#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

// clang-format off
#define __FIXTURE_JOIN_(a, b) a ##_## b
#define __FIXTURE_JOIN(a, b) __FIXTURE_JOIN_(a, b)
// clang-format on

#define FIXTURE_TEST(method, class_name)                                       \
    struct __FIXTURE_JOIN(class_name, method) final                            \
      : class_name                                                             \
      , seastar::testing::seastar_test {                                       \
        const char* get_test_file() override {                                 \
            return __FILE__;                                                   \
        }                                                                      \
        const char* get_name() override {                                      \
            return #method;                                                    \
        }                                                                      \
        seastar::future<> run_test_case() override {                           \
            return seastar::async([this] { do_run_test_case(); });             \
        }                                                                      \
        void do_run_test_case();                                               \
    };                                                                         \
    static __FIXTURE_JOIN(class_name, method)                                  \
      __FIXTURE_JOIN(method, _instance);                                       \
    void ::__FIXTURE_JOIN(class_name, method)::do_run_test_case()
