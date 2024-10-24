// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "kafka/server/usage_aggregator.h"
#include "storage/tests/kvstore_fixture.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

static ss::logger af_logger{"test-accounting-fiber"};

class test_accounting_fiber final
  : public kafka::usage_aggregator<ss::manual_clock> {
public:
    test_accounting_fiber(
      storage::kvstore& kvstore,
      size_t usage_num_windows,
      std::chrono::seconds usage_window_width_interval,
      std::chrono::seconds usage_disk_persistance_interval)
      : kafka::usage_aggregator<ss::manual_clock>(
          kvstore,
          usage_num_windows,
          usage_window_width_interval,
          usage_disk_persistance_interval) {}

    void add_bytes(size_t sent, size_t recv) {
        vlog(af_logger.info, "Adding bytes sent: {} recv: {}", sent, recv);
        _bytes_sent += sent;
        _bytes_recv += recv;
    }

    template<typename Duration>
    ss::future<> advance_clock(Duration d) {
        if (_window_closed_promise) {
            return ss::make_exception_future<>(
              std::logic_error("Already a waiter on data fetching"));
        }
        _window_closed_promise = ss::promise<>();
        auto f = _window_closed_promise->get_future();
        ss::manual_clock::advance(d);
        return ss::with_timeout(ss::lowres_clock::now() + d, std::move(f))
          .handle_exception_type([this, d](const ss::timed_out_error&) {
              vlog(
                af_logger.info,
                "Clock advanced {}s but window didn't close",
                d.count());
              _window_closed_promise = std::nullopt;
          });
    }

protected:
    ss::future<kafka::usage> close_current_window() final {
        vlog(af_logger.info, "Data taken...");
        auto u = kafka::usage{
          .bytes_sent = _bytes_sent, .bytes_received = _bytes_recv};
        _bytes_sent = 0;
        _bytes_recv = 0;
        co_return u;
    }

    void window_closed() final {
        vlog(af_logger.info, "Window closed...");
        if (_window_closed_promise) {
            _window_closed_promise->set_value();
            _window_closed_promise = std::nullopt;
        }
    }

private:
    std::optional<ss::promise<>> _window_closed_promise;
    size_t _bytes_sent{0};
    size_t _bytes_recv{0};
};

namespace {
std::vector<kafka::usage>
strip_window_data(const std::vector<kafka::usage_window>& v) {
    std::vector<kafka::usage> vv;
    std::transform(
      v.begin(),
      v.end(),
      std::back_inserter(vv),
      [](const kafka::usage_window& uw) { return uw.u; });
    return vv;
}

ss::sstring print_window_data(const std::vector<kafka::usage_window>& v) {
    std::stringstream ss;
    ss << "\n[\n";
    for (const auto& vs : v) {
        ss << "\t" << vs.u << ",\n";
    }
    ss << "]";
    return ss.str();
}
} // namespace

FIXTURE_TEST(test_usage, kvstore_test_fixture) {
    using namespace std::chrono_literals;
    auto kvstore = make_kvstore();
    kvstore->start().get();

    const auto num_windows = 10;
    const auto window_width = 10s;
    const auto disk_write_interval = 1s;

    auto usage_fiber = std::make_unique<test_accounting_fiber>(
      *kvstore, num_windows, window_width, disk_write_interval);
    usage_fiber->start().get();

    /// Create expected data set
    std::vector<kafka::usage> data;
    for (auto i = 0; i < num_windows - 1; ++i) {
        const uint64_t sent = i * 100;
        const uint64_t recv = i * 200;
        data.emplace_back(
          kafka::usage{.bytes_sent = sent, .bytes_received = recv});
    }

    /// Publish some data as the windows move across in time, assert observed is
    /// as expected
    for (const auto& e : data) {
        usage_fiber->add_bytes(e.bytes_sent, e.bytes_received);
        usage_fiber->advance_clock(window_width).get();
        vlog(
          af_logger.info,
          "Clock advanced: by {}s, data: {}",
          window_width.count(),
          print_window_data(usage_fiber->get_usage_stats().get()));
    }
    auto result = strip_window_data(usage_fiber->get_usage_stats().get());

    /// Compare the expected to the observed, must re-order expected and insert
    /// an empty window to properly compare
    data.emplace_back(kafka::usage{.bytes_sent = 0, .bytes_received = 0});
    std::reverse(data.begin(), data.end());
    BOOST_CHECK_EQUAL(result, data);

    /// Add to open window
    usage_fiber->add_bytes(10, 10);
    usage_fiber->advance_clock(2s).get();
    usage_fiber->add_bytes(10, 10);
    usage_fiber->advance_clock(2s).get();
    const auto open_windows = usage_fiber->get_usage_stats().get();
    vlog(
      af_logger.info,
      "Clock advanced 4s, data: {}",
      print_window_data(open_windows));
    const auto open_window = open_windows[0];
    BOOST_CHECK_EQUAL(open_window.u.bytes_sent, 20);
    BOOST_CHECK_EQUAL(open_window.u.bytes_received, 20);

    /// Grab most recent result before shutdown
    result = strip_window_data(usage_fiber->get_usage_stats().get());

    /// Shut it down, note clean shutdowns persist data to kvstore
    usage_fiber->stop().get();
    usage_fiber = nullptr;

    /// Restart the instance, and verify expected behavior
    usage_fiber = std::make_unique<test_accounting_fiber>(
      *kvstore, num_windows, window_width, disk_write_interval);
    usage_fiber->start().get();

    // Ensure open window is consistent
    auto result_after_restart = strip_window_data(
      usage_fiber->get_usage_stats().get());
    const auto& new_open_window = result_after_restart[0];
    BOOST_CHECK_EQUAL(new_open_window.bytes_sent, 20);
    BOOST_CHECK_EQUAL(new_open_window.bytes_received, 20);
    BOOST_CHECK_EQUAL(result, result_after_restart);

    usage_fiber->stop().get();
    kvstore->stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_round_to_interval_method) {
    using namespace std::chrono_literals;
    /// Aug 3rd, 2023 4PM GMT
    const auto ts = ss::lowres_system_clock::time_point(
      std::chrono::seconds(1691078400));
    /// 15min, 30min, 1hr, 2hrs
    const auto intervals = std::to_array({900s, 1800s, 3600s, 7200s});

    for (const auto& ival : intervals) {
        /// Underneath the 2min threshold the method returns the input rounded
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts + 10s));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts - 10s));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts + 1min));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts - 1min));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts + 2min));
        BOOST_CHECK(ts == kafka::detail::round_to_interval(ival, ts - 2min));
        /// Past the 2min threshold the method returns the input unmodified
        BOOST_CHECK(ts != kafka::detail::round_to_interval(ival, ts + 3min));
        BOOST_CHECK(ts != kafka::detail::round_to_interval(ival, ts - 3min));
        BOOST_CHECK(ts != kafka::detail::round_to_interval(ival, ts + 10min));
        BOOST_CHECK(ts != kafka::detail::round_to_interval(ival, ts - 10min));
    }
}
