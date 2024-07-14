#include "base/seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <string>
#include <utility>

namespace {

static constexpr size_t ITERS = 10000;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static bool always_false_global = false;

// always return false, but the compiler can't prove it
bool always_false() {
    perf_tests::do_not_optimize(always_false_global);
    return always_false_global;
}

template<typename F>
ss::future<size_t> co_await_in_loop(F f) {
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < ITERS; i++) {
        co_await f();
    }
    perf_tests::stop_measuring_time();
    co_return ITERS;
}

template<typename F>
ss::future<size_t> collect_futures(F f) {
    std::vector<ss::future<>> futs;
    futs.reserve(ITERS);
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < ITERS; i++) {
        futs.emplace_back(f());
    }
    perf_tests::stop_measuring_time();

    for (auto& fut : futs) {
        co_await std::move(fut);
    }
    co_return ITERS;
}

[[gnu::noinline]] ss::future<> ss_now() { return ss::now(); }

[[gnu::noinline]] ss::future<> empty_cont() { return ss::maybe_yield(); }

[[gnu::noinline]] static auto ready_future() {
    return ss::make_ready_future<int>(1);
}

ss::future<int> always_ready_coro() {
    int x = 0;
    if (always_false()) {
        x = co_await seastar::coroutine::without_preemption_check(
          ready_future());
    }
    co_return x;
}

/**
 * @brief Returns always ready future, but across an optimization barrier.
 */
inline ss::future<> always_ready() {
    if (always_false()) [[unlikely]] {
        return ss::yield();
    }
    return ss::now();
}

template<typename T>
inline ss::future<T> always_ready(T&& t) {
    if (always_false()) [[unlikely]] {
        return ss::yield().then([=]() mutable { return std::move(t); });
    }
    return ssx::now(std::forward<T>(t));
}

/**
 * @brief Returns an always ready future after co_awaiting it.
 */
ss::future<> never_awaits() {
    if (always_false()) {
        co_await ss::make_ready_future<>();
    }
}

struct coro_bench {};

volatile int some_int;

void do_work() { some_int = 1; }

template<typename T>
auto do_work(T& t) {
    return always_ready(std::move(t));
}

inline ss::future<> co_await_ready() {
    co_await always_ready();
    do_work();
}

template<size_t depth>
inline ss::future<> co_await_ready_nested() {
    static_assert(depth > 0);

    if constexpr (depth == 1) {
        co_await always_ready();
    } else {
        co_await co_await_ready_nested<depth - 1>();
    }
    do_work();
}

struct small_object {
    char x;
};

struct large_object {
    char x[33];
};

/**
 * Does an unconditional yield, followed by series of
 * non-yielding then continuations, but which are all tested
 * inside the single then() chained off of the yield.
 *
 * Compared to the chained_after_yield immediately after, where
 * all the continuations are directly chained, it shows the
 * efficiency of the nesting here, because the yield causes
 * each continuation in the attached chain to become a separate
 * task, even if they would never yield themselves. The nesting
 * avoids this as it isn't even evaluated until after the yield
 * finishes.
 */
ss::future<> nested_after_yield() {
    using T = small_object;
    T t{};
    return yield().then([t = t]() mutable {
        return do_work(t)
          .then([](T t) { return do_work(t); })
          .then([](T t) { return do_work(t); })
          .then([](T t) { return do_work(t); })
          .then([](T t) { return do_work(t); })
          .then([](T) { return always_ready(); });
    });
}

ss::future<> chained_after_yield() {
    using T = small_object;
    T t{};
    return yield()
      .then([t = t]() mutable { return do_work(t); })
      .then([](T t) { return do_work(t); })
      .then([](T t) { return do_work(t); })
      .then([](T t) { return do_work(t); })
      .then([](T t) { return do_work(t); })
      .then([](T) { return always_ready(); })
      .finally([] {})
      .finally([] {});
}

ss::future<> coro_after_yield() {
    small_object t{};
    co_await yield();

    t = co_await do_work(t);
    t = co_await do_work(t);
    t = co_await do_work(t);
    t = co_await do_work(t);
    t = co_await do_work(t);
}

ss::future<> chain_then5() {
    using T = std::string;
    T t{};
    return do_work(t)
      .then([](T t) { return t; })
      .then([](T t) { return t; })
      .then([](T t) { return t; })
      .then([](T t) { return t; })
      .then([](T t) { return t; })
      .then([](T) { return always_ready(); });
}

inline ss::future<> nested_then5() {
    return always_ready().then([] {
        return always_ready().then([] {
            return always_ready().then([] {
                return always_ready().then(
                  [] { return always_ready().then([] { do_work(); }); });
            });
        });
    });
}

} // namespace

PERF_TEST_F(coro_bench, empty_cont) { return co_await_in_loop(empty_cont); }

PERF_TEST_F(coro_bench, ss_now) { return co_await_in_loop(ss_now); }

PERF_TEST_F(coro_bench, ss_now_collect) { return collect_futures(ss_now); }

PERF_TEST_F(coro_bench, co_await_ready) {
    return co_await_in_loop(co_await_ready);
}

PERF_TEST_F(coro_bench, co_await_ready_collect) {
    return collect_futures(co_await_ready);
}

PERF_TEST_F(coro_bench, nested_then5) { return co_await_in_loop(nested_then5); }

PERF_TEST_F(coro_bench, chain_then5) { return co_await_in_loop(chain_then5); }

PERF_TEST_F(coro_bench, nested_after_yield) {
    return co_await_in_loop(nested_after_yield);
}

PERF_TEST_F(coro_bench, chained_after_yield) {
    return co_await_in_loop(chained_after_yield);
}

PERF_TEST_F(coro_bench, coro_after_yield) {
    return co_await_in_loop(coro_after_yield);
}

PERF_TEST_F(coro_bench, co_await_ready_nested3) {
    return co_await_in_loop(co_await_ready_nested<3>);
}

PERF_TEST_F(coro_bench, co_await_ready_nested5) {
    return co_await_in_loop(co_await_ready_nested<5>);
}

PERF_TEST_F(coro_bench, empty_coro) {
    return co_await_in_loop(always_ready_coro);
}

PERF_TEST_F(coro_bench, never_awaits_collect) {
    return collect_futures(never_awaits);
}
