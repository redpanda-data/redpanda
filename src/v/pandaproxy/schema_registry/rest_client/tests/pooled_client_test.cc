/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/pooled_client.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/later.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

namespace rc = pandaproxy::schema_registry::rest_client;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;

namespace {

class fake_transport;

// Shared across a test's fake transports to observe pool-wide behavior.
struct pool_observer {
    // When true, requests park until release_one() or transport shutdown.
    bool park{false};
    int inflight{0};
    int max_inflight{0};
    // Request targets in dispatch order.
    std::vector<ss::sstring> dispatched;
    struct parked_request {
        ss::promise<http::downloaded_response> promise;
        fake_transport* transport;
    };
    std::deque<parked_request> parked;

    // Completes the oldest parked request successfully.
    void release_one() {
        ASSERT_FALSE(parked.empty());
        auto req = std::move(parked.front());
        parked.pop_front();
        req.promise.set_value(
          http::downloaded_response{.status = bh::status::ok, .body = iobuf{}});
    }
};

// A single-connection transport double. Requests either complete immediately
// (with the configured response or an injected failure) or park on the
// observer until released; shutdown fails this transport's parked requests,
// mirroring how stopping a real http::client fails the request it is
// servicing.
class fake_transport final : public http::abstract_client {
public:
    explicit fake_transport(pool_observer& observer)
      : _observer(observer) {}

    ss::future<http::downloaded_response> request_and_collect_response(
      bh::request_header<>&& request,
      std::optional<iobuf> payload,
      ss::lowres_clock::duration timeout) final {
        // The pool must lease a transport to one request at a time.
        EXPECT_EQ(_inflight, 0) << "transport leased to two requests at once";
        EXPECT_FALSE(_stopped) << "request dispatched to a stopped transport";
        ++_inflight;
        ++_observer.inflight;
        _observer.max_inflight = std::max(
          _observer.max_inflight, _observer.inflight);
        _observer.dispatched.emplace_back(
          request.target().begin(), request.target().end());
        _last_payload_size = payload.has_value() ? payload->size_bytes() : 0;
        _last_timeout = timeout;

        auto response = ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = _status, .body = iobuf::from(_body)});
        if (_fail_next) {
            _fail_next = false;
            response = ss::make_exception_future<http::downloaded_response>(
              std::runtime_error("injected transport failure"));
        } else if (_observer.park) {
            _observer.parked.push_back(
              {.promise = ss::promise<http::downloaded_response>{},
               .transport = this});
            response = _observer.parked.back().promise.get_future();
        }
        return response.finally([this] {
            --_inflight;
            --_observer.inflight;
        });
    }

    ss::future<> shutdown_and_stop() final {
        _stopped = true;
        // Like a real transport, fail the request this transport is servicing.
        for (auto& parked : _observer.parked) {
            if (parked.transport == this) {
                parked.promise.set_exception(
                  std::make_exception_ptr(
                    std::runtime_error("transport stopped")));
            }
        }
        std::erase_if(_observer.parked, [this](const auto& parked) {
            return parked.transport == this;
        });
        return ss::make_ready_future<>();
    }

    void set_response(bh::status status, ss::sstring body) {
        _status = status;
        _body = std::move(body);
    }
    void fail_next() { _fail_next = true; }
    bool stopped() const { return _stopped; }
    size_t last_payload_size() const { return _last_payload_size; }
    ss::lowres_clock::duration last_timeout() const { return _last_timeout; }

private:
    pool_observer& _observer;
    int _inflight{0};
    bool _stopped{false};
    bool _fail_next{false};
    bh::status _status{bh::status::ok};
    ss::sstring _body;
    size_t _last_payload_size{0};
    ss::lowres_clock::duration _last_timeout{};
};

// A pool of n fake transports plus non-owning handles to each fake.
struct pool_fixture {
    explicit pool_fixture(size_t n) {
        std::vector<std::unique_ptr<http::abstract_client>> owned;
        owned.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            auto transport = std::make_unique<fake_transport>(observer);
            transports.push_back(transport.get());
            owned.push_back(std::move(transport));
        }
        pool = std::make_unique<rc::pooled_client>(std::move(owned));
    }

    pool_observer observer;
    std::vector<fake_transport*> transports;
    std::unique_ptr<rc::pooled_client> pool;
};

// A coroutine so the header outlives the request: the pool may suspend the
// request while it waits for a free transport.
ss::future<http::downloaded_response>
issue(rc::pooled_client& pool, std::string_view target) {
    bh::request_header<> header;
    header.method(bh::verb::get);
    header.target(target);
    co_return co_await pool.request_and_collect_response(std::move(header));
}

} // namespace

TEST(pooled_client, forwards_request_response_and_arguments) {
    pool_fixture fx(1);
    fx.transports[0]->set_response(bh::status::created, "hello response");

    bh::request_header<> header;
    header.method(bh::verb::get);
    header.target("/subjects");
    auto res = fx.pool
                 ->request_and_collect_response(
                   std::move(header), iobuf::from("payload!"), 123ms)
                 .get();

    EXPECT_EQ(res.status, bh::status::created);
    EXPECT_EQ(res.body, iobuf::from("hello response"));
    EXPECT_EQ(fx.observer.dispatched, (std::vector<ss::sstring>{"/subjects"}));
    EXPECT_EQ(fx.transports[0]->last_payload_size(), 8);
    EXPECT_EQ(fx.transports[0]->last_timeout(), 123ms);
    fx.pool->shutdown_and_stop().get();
}

TEST(pooled_client, bounds_concurrency_and_queues_fifo) {
    pool_fixture fx(1);
    fx.observer.park = true;

    std::vector<ss::future<http::downloaded_response>> futs;
    futs.reserve(5);
    for (int i = 0; i < 5; ++i) {
        futs.push_back(issue(*fx.pool, fmt::format("/r{}", i)));
    }

    // One request holds the transport; the other four wait for the slot.
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return fx.observer.inflight == 1; });
    EXPECT_EQ(fx.observer.dispatched.size(), 1);

    // Complete the in-flight request one at a time; each freed slot is granted
    // to the earliest queued waiter (semaphore FIFO fairness).
    for (int released = 0; released < 5; ++released) {
        RPTEST_REQUIRE_EVENTUALLY(
          5s, [&] { return !fx.observer.parked.empty(); });
        fx.observer.release_one();
    }
    for (auto& fut : futs) {
        EXPECT_EQ(fut.get().status, bh::status::ok);
    }

    EXPECT_EQ(fx.observer.max_inflight, 1);
    // Waiters got slots in issue order.
    EXPECT_EQ(
      fx.observer.dispatched,
      (std::vector<ss::sstring>{"/r0", "/r1", "/r2", "/r3", "/r4"}));
    fx.pool->shutdown_and_stop().get();
}

TEST(pooled_client, transport_returned_after_failure) {
    pool_fixture fx(1);
    fx.transports[0]->fail_next();

    auto failed = issue(*fx.pool, "/fail");
    EXPECT_THROW(failed.get(), std::runtime_error);

    // The transport and its slot were recycled after the failure.
    auto res = issue(*fx.pool, "/ok").get();
    EXPECT_EQ(res.status, bh::status::ok);
    EXPECT_EQ(
      fx.observer.dispatched, (std::vector<ss::sstring>{"/fail", "/ok"}));
    fx.pool->shutdown_and_stop().get();
}

TEST(pooled_client, shutdown_fails_inflight_ejects_waiters_rejects_new) {
    pool_fixture fx(2);
    fx.observer.park = true;

    auto in_flight_a = issue(*fx.pool, "/a");
    auto in_flight_b = issue(*fx.pool, "/b");
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return fx.observer.parked.size() == 2; });
    auto waiter = issue(*fx.pool, "/c");
    // Let the third request reach the slot queue (its coroutine may start
    // asynchronously) so shutdown ejects a waiter, not an unstarted request.
    for (int i = 0; i < 3; ++i) {
        ss::yield().get();
    }
    EXPECT_EQ(fx.observer.dispatched.size(), 2);

    fx.pool->shutdown_and_stop().get();

    // In-flight requests fail when their transport stops; the queued waiter
    // is ejected with an abort.
    EXPECT_THROW(in_flight_a.get(), std::runtime_error);
    EXPECT_THROW(in_flight_b.get(), std::runtime_error);
    EXPECT_THROW(waiter.get(), ss::abort_requested_exception);
    EXPECT_TRUE(fx.transports[0]->stopped());
    EXPECT_TRUE(fx.transports[1]->stopped());
    // New requests are rejected once shut down.
    EXPECT_THROW(issue(*fx.pool, "/d").get(), ss::gate_closed_exception);
}
