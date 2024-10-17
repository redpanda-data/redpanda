#include "errors/errors.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <exception>

enum class my_error {
    success,
    failure,
};

namespace errors {
template<>
struct exception_mapping_policy<my_error> {
    my_error operator()(std::exception_ptr&& e) noexcept {
        std::ignore = e;
        return my_error::failure;
    }
};
} // namespace errors

namespace my_test {

// Every subsystem defines some set of error codes
// as an enum class. Aliasing 'maybe' will remove a
// lot of boilerplate if this is the case.
template<class T>
using maybe = errors::maybe<T, my_error>;

seastar::future<maybe<int>> return_value(int i) { co_return i; }

seastar::future<maybe<int>> return_error() {
    throw std::runtime_error("boo");
    // without a co_return or co_await the function will not be
    // a coroutine and the corresponding coroutine_traits
    // instance will not be created/invoked for it.
    co_return 0;
}

seastar::future<maybe<ss::sstring>> return_str() {
    co_return ss::sstring("foo");
}

} // namespace my_test

TEST_CORO(test_maybe, check_exception_to_maybe_conversion) {
    auto x = co_await my_test::return_value(42);
    ASSERT_TRUE_CORO(x.is_ok());
    ASSERT_EQ_CORO(x.unwrap(), 42);
    auto y = co_await my_test::return_error();
    ASSERT_TRUE_CORO(y.is_err());
    ASSERT_TRUE_CORO(y.err().value() == my_error::failure);
    co_return;
}

TEST_CORO(test_maybe, check_and_then) {
    auto x = my_test::maybe<ss::sstring>::ok("foo");
    auto r = x.and_then([](const ss::sstring& y) { return y.size(); });
    ASSERT_EQ_CORO(r.unwrap(), 3);
    co_return;
}

TEST_CORO(test_maybe, check_and_then_maybe) {
    auto x = my_test::maybe<ss::sstring>::ok("foo");
    auto r = x.and_then([](const ss::sstring& y) noexcept {
        return my_test::maybe<size_t>::ok(y.size());
    });
    ASSERT_EQ_CORO(r.unwrap(), 3);
    co_return;
}

TEST_CORO(test_maybe, check_and_then_ref) {
    auto x = my_test::maybe<ss::sstring>::ok("foo");
    auto r = std::move(x).and_then_ref(
      [](ss::sstring&& y) { return y.size(); });
    ASSERT_EQ_CORO(r.unwrap(), 3);
    co_return;
}

TEST_CORO(test_maybe, check_and_then_err) {
    auto x = my_test::maybe<ss::sstring>::err(my_error::failure);
    auto r = x.and_then([](const ss::sstring& y) { return y.size(); });
    ASSERT_TRUE_CORO(r.is_err());
    co_return;
}

TEST_CORO(test_maybe, check_and_then_ref_maybe) {
    auto x = co_await my_test::return_str();
    auto r = std::move(x).and_then_ref([](ss::sstring&& y) noexcept {
        return my_test::maybe<size_t>::ok(y.size());
    });
    ASSERT_EQ_CORO(r.unwrap(), 3);
    co_return;
}

TEST_CORO(test_maybe, check_or_else_err) {
    auto x = my_test::maybe<ss::sstring>::err(my_error::failure);
    auto r = x.or_else([](my_error e) {
        return ss::sstring(e == my_error::failure ? "foo" : "bar");
    });
    ASSERT_EQ_CORO(r.unwrap(), "foo");
    co_return;
}

TEST_CORO(test_maybe, check_or_else_ok) {
    auto x = my_test::maybe<ss::sstring>::ok("foo");
    auto r = x.or_else([](my_error) { return ss::sstring("bar"); });
    ASSERT_EQ_CORO(r.unwrap(), "foo");
    co_return;
}
