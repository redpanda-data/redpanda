#include "raft/mutex_buffer.h"
#include "ssx/sformat.h"
#include "test_utils/fixture.h"
#include "utils/mutex.h"

#include <seastar/testing/thread_test_case.hh>

#include <fmt/core.h>

struct request {
    ss::sstring content;
};

struct response {
    ss::sstring content;
};

struct fixture {
    fixture()
      : buf(lock, 20) {}

    static ss::future<response> add_postfix(request r) {
        return ss::make_ready_future<response>(
          response{r.content + "-response"});
    }

    mutex lock{"mutex_buffer_test"};
    raft::details::mutex_buffer<request, response> buf;
};

FIXTURE_TEST(test_single_request_dispatch, fixture) {
    buf.start(&fixture::add_postfix);

    auto f = buf.enqueue(request{"test"});
    BOOST_REQUIRE_EQUAL(f.get().content, "test-response");
    buf.stop().get();
}

FIXTURE_TEST(test_requests_buffering, fixture) {
    buf.start(&fixture::add_postfix);

    std::vector<ss::future<response>> enqueued;
    enqueued.reserve(5);
    {
        auto u = lock.get_units().get();
        for (auto i = 0; i < 5; ++i) {
            enqueued.push_back(
              buf.enqueue(request{ssx::sformat("test-{}", i)}));
        }
        BOOST_REQUIRE(enqueued[0].available() == false);
    }

    auto i = 0;
    for (auto& f : enqueued) {
        BOOST_REQUIRE_EQUAL(
          f.get().content, ssx::sformat("test-{}-response", i));
        i++;
    }

    buf.stop().get();
}

FIXTURE_TEST(after_close_buffer_should_stop_accepting_requests, fixture) {
    // stop buffer first
    buf.stop().get();

    BOOST_REQUIRE_THROW(
      buf.enqueue(request{"should-fail"}).get(), ss::gate_closed_exception);
}

FIXTURE_TEST(should_propagate_exception, fixture) {
    int cnt = 0;
    buf.start([&cnt](request r) {
        cnt++;
        if (cnt % 2 != 0) {
            return ss::make_exception_future<response>(
              std::runtime_error("error"));
        }

        return add_postfix(std::move(r));
    });

    std::vector<ss::future<response>> enqueued;
    enqueued.reserve(5);
    {
        auto u = lock.get_units().get();

        for (auto i = 0; i < 5; ++i) {
            enqueued.push_back(
              buf.enqueue(request{ssx::sformat("test-{}", i)}));
        }
    }
    auto i = 0;

    for (auto& f : enqueued) {
        i++;
        if (i % 2 != 0) {
            BOOST_REQUIRE_THROW(f.get(), std::runtime_error);
        } else {
            BOOST_REQUIRE_EQUAL(
              f.get().content, ssx::sformat("test-{}-response", i - 1));
        }
    }

    buf.stop().get();
}
