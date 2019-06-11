#include "utils/remote.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_free_on_right_cpu) {
    auto p = seastar::smp::submit_to(1, [] { return remote<int>(10); }).get0();
    p.get() += 1;
    seastar::smp::submit_to(1, [p = std::move(p)]() mutable {
        auto local = std::move(p);
        (void)local;
    }).get();
}

struct obj_with_foreign_ptr {
    int x;
    seastar::foreign_ptr<const char*> ptr;

    obj_with_foreign_ptr() = default;

    obj_with_foreign_ptr(int x, seastar::foreign_ptr<const char*> ptr)
      : x(x)
      , ptr(std::move(ptr)) {
    }

    obj_with_foreign_ptr(obj_with_foreign_ptr&& other) noexcept
      : x(other.x)
      , ptr(std::exchange(other.ptr, {})) {
    }

    obj_with_foreign_ptr& operator=(obj_with_foreign_ptr&& other) noexcept {
        x = other.x;
        ptr = std::exchange(other.ptr, {});
        return *this;
    }

    operator bool() const {
        return bool(ptr);
    }
};

SEASTAR_THREAD_TEST_CASE(test_free_on_right_cpu_with_foreign_ptr) {
    auto p = seastar::smp::submit_to(1, [] {
                 return remote<obj_with_foreign_ptr>(
                   0, seastar::make_foreign("la"));
             }).get0();
    p.get().x += 1;
    seastar::smp::submit_to(1, [p = std::move(p)]() mutable {
        auto local = std::move(p);
        (void)local;
    }).get();
}
