#include "base/seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

bool always_false_coro = false;

namespace {

static constexpr size_t ITERS = 10000;

template<typename F>
ss::future<size_t> co_await_in_loop(F f) {
    perf_tests::start_measuring_time();
    for (int i = 0; i < ITERS; i++) {
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
    for (int i = 0; i < ITERS; i++) {
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

ss::future<int> empty_coro() {
    int x = 0;
    if (always_false_coro) {
        x = co_await seastar::coroutine::without_preemption_check(
          ready_future());
    }
    co_return x;
}

ss::future<> never_awaits() {
    if (always_false_coro) {
        co_await ss::make_ready_future<>();
    }
}

struct coro_bench {};

inline ss::future<> co_await_ready() { co_await ss::make_ready_future<>(); }
inline ss::future<> co_await_ready_nest2() { co_await co_await_ready(); }
inline ss::future<> co_await_ready_nest3() { co_await co_await_ready_nest2(); }

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

PERF_TEST_F(coro_bench, co_await_ready_nest2) {
    return co_await_in_loop(co_await_ready_nest2);
}

PERF_TEST_F(coro_bench, co_await_ready_nest2_collect) {
    return collect_futures(co_await_ready_nest2);
}

PERF_TEST_F(coro_bench, co_await_ready_nest3) {
    return co_await_in_loop(co_await_ready_nest3);
}

PERF_TEST_F(coro_bench, co_await_ready_nest3_collect) {
    return collect_futures(co_await_ready_nest3);
}

PERF_TEST_F(coro_bench, empty_coro) { return co_await_in_loop(empty_coro); }

PERF_TEST_F(coro_bench, never_awaits) { return co_await_in_loop(never_awaits); }

PERF_TEST_F(coro_bench, never_awaits_collect) {
    return collect_futures(never_awaits);
}
