// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/remote.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_free_on_right_cpu) {
    auto p = ss::smp::submit_to(1, [] { return remote<int>(10); }).get();
    p.get() += 1;
    ss::smp::submit_to(1, [p = std::move(p)]() mutable {
        auto local = std::move(p);
        (void)local;
    }).get();
}

struct obj_with_foreign_ptr {
    int x;
    ss::foreign_ptr<std::unique_ptr<int>> ptr;

    obj_with_foreign_ptr() = default;

    obj_with_foreign_ptr(int x, ss::foreign_ptr<std::unique_ptr<int>> ptr)
      : x(x)
      , ptr(std::move(ptr)) {}

    obj_with_foreign_ptr(obj_with_foreign_ptr&& other) noexcept
      : x(other.x)
      , ptr(std::exchange(other.ptr, {})) {}

    obj_with_foreign_ptr& operator=(obj_with_foreign_ptr&& other) noexcept {
        x = other.x;
        ptr = std::exchange(other.ptr, {});
        return *this;
    }

    operator bool() const { return bool(ptr); }
};

SEASTAR_THREAD_TEST_CASE(test_free_on_right_cpu_with_foreign_ptr) {
    auto p = ss::smp::submit_to(1, [] {
                 return remote<obj_with_foreign_ptr>(
                   0, ss::make_foreign(std::make_unique<int>(42)));
             }).get();
    p.get().x += 1;
    ss::smp::submit_to(1, [p = std::move(p)]() mutable {
        auto local = std::move(p);
        (void)local;
    }).get();
}
