/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/configuration.h"
#include "debug_bundle/debug_bundle_service.h"
#include "debug_bundle/error.h"
#include "debug_bundle/types.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

struct debug_bundle_service_fixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        const char* script_path = std::getenv("RPK_SHIM");
        ASSERT_NE_CORO(script_path, nullptr)
          << "Missing 'RPK_SHIM' env variable";
        ASSERT_TRUE_CORO(co_await ss::file_exists(script_path))
          << script_path << " does not exist";
        _rpk_shim_path = script_path;

        _data_dir = "test_dir_" + random_generators::gen_alphanum_string(6);
        ASSERT_NO_THROW_CORO(
          co_await ss::recursive_touch_directory(_data_dir.native()))
          << "Failed to create " << _data_dir;

        config::shard_local_cfg().rpk_path.set_value(_rpk_shim_path);
        co_await _service.start(_data_dir);
    }

    ss::future<> TearDownAsync() override { co_await _service.stop(); }

    std::filesystem::path _rpk_shim_path;
    std::filesystem::path _data_dir;
    ss::sharded<debug_bundle::service> _service;
};

struct debug_bundle_service_started_fixture
  : public debug_bundle_service_fixture {
    ss::future<> SetUpAsync() override {
        co_await debug_bundle_service_fixture::SetUpAsync();
        ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
          [](debug_bundle::service& s) -> ss::future<> {
              co_await s.start();
          }));
    }
};

TEST_F_CORO(debug_bundle_service_fixture, basic_start_stop) {
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    EXPECT_FALSE(co_await ss::file_exists(debug_bundle_dir.native()));
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
    EXPECT_TRUE(co_await ss::file_exists(debug_bundle_dir.native()));
}

TEST_F_CORO(debug_bundle_service_fixture, bad_rpk_path) {
    config::shard_local_cfg().rpk_path.set_value("/no/such/bin");
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    EXPECT_FALSE(co_await ss::file_exists(debug_bundle_dir.native()));
    // We should still expect the service to start, but it will emit a warning
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
    EXPECT_TRUE(co_await ss::file_exists(debug_bundle_dir.native()));
}

ss::future<debug_bundle::result<debug_bundle::debug_bundle_status_data>>
wait_for_process_to_finish(
  ss::sharded<debug_bundle::service>& service,
  const std::chrono::seconds timeout) {
    const auto start_time = debug_bundle::clock::now();
    while (debug_bundle::clock::now() - start_time <= timeout) {
        auto status = co_await service.local().rpk_debug_bundle_status();
        if (status.has_failure()) {
            throw std::runtime_error("status contains error");
        }
        if (
          status.assume_value().status
          != debug_bundle::debug_bundle_status::running) {
            co_return status;
        }
    }
    throw std::runtime_error("Timed out waiting for process to exit");
}

TEST_F_CORO(debug_bundle_service_started_fixture, run_process) {
    using namespace std::chrono_literals;

    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_FALSE_CORO(res.has_failure()) << res.as_failure().error().message();

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_FALSE_CORO(status.has_failure())
      << res.as_failure().error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::running);
    EXPECT_EQ(status.assume_value().job_id, job_id);
    EXPECT_EQ(status.assume_value().file_name, fmt::format("{}.zip", job_id));

    ASSERT_NO_THROW_CORO(
      status = co_await wait_for_process_to_finish(_service, 10s));

    ASSERT_FALSE_CORO(status.has_failure())
      << res.as_failure().error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::success);
    const auto& cout = status.assume_value().cout;
    EXPECT_FALSE(cout.empty());
    auto expected_args = fmt::format(
      "debug bundle --output {}/{}.zip --verbose\n",
      (_data_dir / debug_bundle::service::debug_bundle_dir_name).native(),
      job_id);
    EXPECT_EQ(cout[0], expected_args) << cout[0] << " != " << expected_args;
}

TEST_F_CORO(debug_bundle_service_started_fixture, test_all_parameters) {
    using namespace std::chrono_literals;

    debug_bundle::job_id_t job_id(uuid_t::create());

    ss::sstring username = "test";
    ss::sstring password = "test12345";
    ss::sstring mechanism = "SCRAM-SHA-256";
    uint64_t controller_logs_size_limit_bytes = 123456;
    std::chrono::seconds cpu_profiler_wait_seconds{60};
    debug_bundle::special_date logs_since
      = debug_bundle::special_date::yesterday;
    uint64_t logs_size_limit = 987654;
    const std::string_view logs_until = "2024-09-05T14:34:02";
    debug_bundle::clock::time_point logs_until_tp;
    {
        std::istringstream ss(logs_until.data());
        std::tm tmp{};
        ASSERT_NO_THROW_CORO(ss >> std::get_time(&tmp, "%FT%T"));
        ASSERT_FALSE_CORO(ss.fail());
        tmp.tm_isdst = -1;
        std::time_t tt = std::mktime(&tmp);
        logs_until_tp = debug_bundle::clock::from_time_t(tt);
    }
    std::chrono::seconds metrics_interval_seconds{54};
    model::topic_namespace tn1(model::ns{"kafka"}, model::topic{"t1"});
    model::topic_namespace tn2(model::ns{"internal"}, model::topic{"t2"});
    std::vector<debug_bundle::partition_selection> partition{
      debug_bundle::partition_selection{
        .tn = tn1,
        .partitions
        = {model::partition_id{1}, model::partition_id{2}, model::partition_id{3}}},
      debug_bundle::partition_selection{
        .tn = tn2,
        .partitions = {
          model::partition_id{4},
          model::partition_id{5},
          model::partition_id{6}}}};

    debug_bundle::debug_bundle_parameters params{
      .authn_options = debug_bundle::
        scram_creds{security::credential_user{username}, security::credential_password{password}, mechanism},
      .controller_logs_size_limit_bytes = controller_logs_size_limit_bytes,
      .cpu_profiler_wait_seconds = cpu_profiler_wait_seconds,
      .logs_since = logs_since,
      .logs_size_limit_bytes = logs_size_limit,
      .logs_until = logs_until_tp,
      .metrics_interval_seconds = metrics_interval_seconds,
      .partition = partition};

    ss::sstring expected_params(fmt::format(
      "debug bundle --output {}/{}.zip --verbose -Xuser={} -Xpass={} "
      "-Xsasl.mechanism={} --controller-logs-size-limit {}B "
      "--cpu-profiler-wait {}s --logs-since {} --logs-size-limit {}B "
      "--logs-until {} --metrics-interval {}s --partition {}/{}/1,2,3 "
      "{}/{}/4,5,6\n",
      (_data_dir / debug_bundle::service::debug_bundle_dir_name).native(),
      job_id,
      username,
      password,
      mechanism,
      controller_logs_size_limit_bytes,
      cpu_profiler_wait_seconds.count(),
      logs_since,
      logs_size_limit,
      logs_until,
      metrics_interval_seconds.count(),
      tn1.ns,
      tn1.tp,
      tn2.ns,
      tn2.tp));

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, std::move(params));
    ASSERT_FALSE_CORO(res.has_failure()) << res.assume_error().message();

    const auto max_retries = 5;
    const auto timeout_time = 1s;

    auto num_retries = 0;
    while (num_retries < max_retries) {
        auto status = co_await _service.local().rpk_debug_bundle_status();
        ASSERT_FALSE_CORO(status.has_failure()) << res.assume_error().message();

        EXPECT_EQ(
          status.assume_value().status,
          debug_bundle::debug_bundle_status::running);

        if (status.assume_value().cout.empty()) {
            // Sleep a second to allow the cout to be captured
            co_await ss::sleep(timeout_time);
        } else {
            EXPECT_EQ(status.assume_value().cout[0], expected_params)
              << status.assume_value().cout[0] << " != " << expected_params;
            break;
        }

        ASSERT_NE_CORO(++num_retries, max_retries)
          << "Maximum number of retries reached!";
    }
}

TEST_F_CORO(debug_bundle_service_started_fixture, try_running_multiple) {
    auto res = co_await _service.invoke_on(
      debug_bundle::service::service_shard, [](debug_bundle::service& s) {
          return s.initiate_rpk_debug_bundle_collection(
            debug_bundle::job_id_t(uuid_t::create()), {});
      });
    ASSERT_FALSE_CORO(res.has_failure()) << res.as_failure().error().message();

    auto res2 = co_await _service.invoke_on(
      (debug_bundle::service::service_shard + 1) % ss::smp::count,
      [](debug_bundle::service& s) {
          return s.initiate_rpk_debug_bundle_collection(
            debug_bundle::job_id_t(uuid_t::create()), {});
      });

    ASSERT_TRUE_CORO(res2.has_failure());
    EXPECT_EQ(
      res2.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_running);
}

TEST_F_CORO(debug_bundle_service_started_fixture, run_no_rpk_binary) {
    config::shard_local_cfg().rpk_path.set_value("/no/such/bin");

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      debug_bundle::job_id_t(uuid_t::create()), {});

    ASSERT_TRUE_CORO(res.has_failure());
    EXPECT_EQ(
      res.assume_error().code(),
      debug_bundle::error_code::rpk_binary_not_present);
}

TEST_F_CORO(debug_bundle_service_started_fixture, rpk_binary_no_exec) {
    // This will use /proc/cmdline as the rpk binary, which is unable to be
    // executed since it's executable flag is not set
    config::shard_local_cfg().rpk_path.set_value("/proc/cmdline");

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      debug_bundle::job_id_t(uuid_t::create()), {});
    ASSERT_TRUE_CORO(res.has_failure());
    EXPECT_EQ(
      res.assume_error().code(), debug_bundle::error_code::internal_error);
}

TEST_F_CORO(debug_bundle_service_started_fixture, status_no_run) {
    auto res = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(res.has_failure());
    EXPECT_EQ(
      res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}

TEST_F_CORO(debug_bundle_service_started_fixture, terminate_process) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_FALSE_CORO(res.has_failure()) << res.as_failure().error().message();

    {
        auto status = co_await _service.local().rpk_debug_bundle_status();
        ASSERT_FALSE_CORO(status.has_failure())
          << res.as_failure().error().message();

        EXPECT_EQ(
          status.assume_value().status,
          debug_bundle::debug_bundle_status::running);
    }

    {
        auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
          job_id);
        ASSERT_FALSE_CORO(term_res.has_failure())
          << term_res.as_failure().error().message();
    }

    {
        std::optional<debug_bundle::debug_bundle_status_data> status{};

        // Retry status check with a generous timeout to mitigate inherent
        // (but benign) race condition agains the status-setter background
        // fiber.
        // If we blow the timeout, that is probably indicative of a more
        // pernicious, system-level bug in the process management module
        // and will need further investigation.
        using namespace std::chrono_literals;
        constexpr auto timeout = 30s;
        constexpr auto interval = 200ms;
        auto expiry = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < expiry) {
            auto st = co_await _service.local().rpk_debug_bundle_status();
            ASSERT_FALSE_CORO(st.has_failure())
              << st.as_failure().error().message();
            status.emplace(std::move(st).assume_value());
            if (
              status.value().status
              != debug_bundle::debug_bundle_status::running) {
                break;
            }
            co_await ss::sleep(interval);
        }

        ASSERT_TRUE_CORO(status.has_value());
        EXPECT_EQ(
          status.value().status, debug_bundle::debug_bundle_status::error);
    }

    {
        auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
          job_id);
        ASSERT_TRUE_CORO(term_res.has_failure());
        EXPECT_EQ(
          term_res.assume_error().code(),
          debug_bundle::error_code::debug_bundle_process_not_running);
    }
}

TEST_F_CORO(
  debug_bundle_service_started_fixture, terminate_process_bad_job_id) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_FALSE_CORO(res.has_failure()) << res.as_failure().error().message();

    {
        auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
          debug_bundle::job_id_t(uuid_t::create()));
        ASSERT_TRUE_CORO(term_res.has_failure());
        EXPECT_EQ(
          term_res.assume_error().code(),
          debug_bundle::error_code::job_id_not_recognized);
    }
}

TEST_F_CORO(debug_bundle_service_started_fixture, termiate_never_ran) {
    auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_TRUE_CORO(term_res.has_failure());
    EXPECT_EQ(
      term_res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}
