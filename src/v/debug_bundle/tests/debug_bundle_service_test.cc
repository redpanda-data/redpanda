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

#include "bytes/iostream.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/property.h"
#include "debug_bundle/debug_bundle_service.h"
#include "debug_bundle/error.h"
#include "debug_bundle/metadata.h"
#include "debug_bundle/types.h"
#include "debug_bundle/utils.h"
#include "features/feature_table.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "storage/file_sanitizer_types.h"
#include "storage/kvstore.h"
#include "storage/storage_resources.h"
#include "test_utils/test.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/process.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <variant>

using namespace std::chrono_literals;

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

        config::node().data_directory.set_value(_data_dir);
        config::shard_local_cfg().rpk_path.set_value(_rpk_shim_path);

        _kvconfig = get_kvstore_config();
        co_await _feature_table.start();
        co_await _feature_table.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
        _kvstore = std::make_unique<storage::kvstore>(
          *_kvconfig, ss::this_shard_id(), _resources, _feature_table);
        co_await _kvstore->start();
        co_await _service.start(_kvstore.get());
    }

    ss::future<> TearDownAsync() override {
        co_await _service.stop();
        co_await _kvstore->stop();
        co_await _feature_table.stop();
    }

    std::unique_ptr<storage::kvstore_config> get_kvstore_config() {
        return std::make_unique<storage::kvstore_config>(
          8192,
          config::mock_binding(std::chrono::milliseconds(10)),
          _data_dir.native(),
          storage::make_sanitized_file_config());
    }

    ss::future<> restart_service() {
        ASSERT_NO_THROW_CORO(co_await _service.stop());
        ASSERT_NO_THROW_CORO(co_await _service.start(_kvstore.get()));
        ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
          [](debug_bundle::service& s) { return s.start(); }));
    }

    ss::future<> wait_for_status(
      const debug_bundle::debug_bundle_status expected_status,
      const std::chrono::seconds timeout = 10s,
      const std::chrono::milliseconds backoff = 100ms) {
        const auto start_time = debug_bundle::clock::now();
        while (debug_bundle::clock::now() - start_time <= timeout) {
            auto status = co_await _service.local().rpk_debug_bundle_status();
            if (!status.has_value()) {
                throw std::system_error(
                  status.assume_error().code(),
                  status.assume_error().message());
            }
            if (status.assume_value().status == expected_status) {
                co_return;
            }
            co_await ss::sleep(backoff);
        }
        throw std::runtime_error(
          fmt::format("Timed out waiting for status {}", expected_status));
    }

    ss::future<debug_bundle::result<debug_bundle::debug_bundle_status_data>>
    wait_for_process_to_finish(
      const std::chrono::seconds timeout = std::chrono::seconds{10},
      const std::chrono::milliseconds backoff = std::chrono::milliseconds{
        100}) {
        const auto start_time = debug_bundle::clock::now();
        while (debug_bundle::clock::now() - start_time <= timeout) {
            auto status = co_await _service.local().rpk_debug_bundle_status();
            if (!status.has_value()) {
                throw std::system_error(
                  status.assume_error().code(),
                  status.assume_error().message());
            }
            if (
              status.assume_value().status
              != debug_bundle::debug_bundle_status::running) {
                co_return status;
            }
            co_await ss::sleep(backoff);
        }
        throw std::runtime_error("Timed out waiting for process to exit");
    }

    std::filesystem::path
    generate_job_file_path(debug_bundle::job_id_t job_id) {
        return _data_dir / debug_bundle::service::debug_bundle_dir_name
               / fmt::format("{}.zip", job_id);
    }

    std::filesystem::path
    generate_process_output_file_path(debug_bundle::job_id_t job_id) {
        return _data_dir / debug_bundle::service::debug_bundle_dir_name
               / fmt::format("{}.out", job_id);
    }

    std::filesystem::path _rpk_shim_path;
    std::filesystem::path _data_dir;
    std::unique_ptr<storage::kvstore_config> _kvconfig;
    storage::storage_resources _resources{};
    std::unique_ptr<storage::kvstore> _kvstore;
    ss::sharded<features::feature_table> _feature_table;
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

    ss::future<> run_bundle(
      debug_bundle::job_id_t job_id,
      debug_bundle::debug_bundle_parameters params = {},
      std::chrono::seconds timeout = std::chrono::seconds{10}) {
        auto res
          = co_await _service.local().initiate_rpk_debug_bundle_collection(
            job_id, std::move(params));
        ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

        ASSERT_NO_THROW_CORO(
          std::ignore = co_await wait_for_process_to_finish(timeout));

        auto status = co_await _service.local().rpk_debug_bundle_status();
        ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();
        ASSERT_EQ_CORO(
          status.assume_value().status,
          debug_bundle::debug_bundle_status::success);
        co_return co_await ss::check_for_io_immediately();
    }
};

TEST_F_CORO(debug_bundle_service_fixture, basic_start_stop) {
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
}

TEST_F_CORO(debug_bundle_service_fixture, bad_rpk_path) {
    config::shard_local_cfg().rpk_path.set_value("/no/such/bin");
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    // We should still expect the service to start, but it will emit a warning
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
}

TEST_F_CORO(debug_bundle_service_started_fixture, change_bundle_dir) {
    EXPECT_EQ(
      _service.local().get_debug_bundle_output_directory(),
      _data_dir / debug_bundle::service::debug_bundle_dir_name);
    std::filesystem::path test_path = "my-test-path";
    config::shard_local_cfg().debug_bundle_storage_dir.set_value(test_path);
    EXPECT_EQ(_service.local().get_debug_bundle_output_directory(), test_path);
    config::shard_local_cfg().debug_bundle_storage_dir.set_value(std::nullopt);
    EXPECT_EQ(
      _service.local().get_debug_bundle_output_directory(),
      _data_dir / debug_bundle::service::debug_bundle_dir_name);
    co_return;
}

TEST_F_CORO(debug_bundle_service_started_fixture, run_process) {
    using namespace std::chrono_literals;

    debug_bundle::job_id_t job_id(uuid_t::create());

    auto expected_file_path = _data_dir
                              / debug_bundle::service::debug_bundle_dir_name
                              / fmt::format("{}.zip", job_id);

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::running);
    EXPECT_EQ(status.assume_value().job_id, job_id);
    EXPECT_EQ(status.assume_value().file_name, fmt::format("{}.zip", job_id));
    EXPECT_FALSE(status.assume_value().file_size.has_value());

    ASSERT_NO_THROW_CORO(status = co_await wait_for_process_to_finish());

    ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::success);
    const auto& cout = status.assume_value().cout;
    EXPECT_FALSE(cout.empty());
    auto expected_args = fmt::format(
      "debug bundle --output {} --verbose\n", expected_file_path.native());
    EXPECT_EQ(cout[0], expected_args) << cout[0] << " != " << expected_args;

    ASSERT_TRUE_CORO(co_await ss::file_exists(expected_file_path.native()));
    auto expected_file_size = co_await ss::file_size(
      expected_file_path.native());
    ASSERT_TRUE_CORO(status.assume_value().file_size.has_value());
    EXPECT_EQ(status.assume_value().file_size.value(), expected_file_size);
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
    uint64_t metrics_samples = 3;
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
    bool tls_enabled = true;
    bool tls_insecure_skip_verify = false;
    ss::sstring k8s_namespace_name = "redpanda-namespace";
    std::vector<debug_bundle::label_selection> label_select{
      debug_bundle::label_selection{.key = "test/key1", .value = "value1"},
      debug_bundle::label_selection{.key = "key2", .value = "value2"}};

    debug_bundle::debug_bundle_parameters params{
      .authn_options = debug_bundle::
        scram_creds{security::credential_user{username}, security::credential_password{password}, mechanism},
      .controller_logs_size_limit_bytes = controller_logs_size_limit_bytes,
      .cpu_profiler_wait_seconds = cpu_profiler_wait_seconds,
      .logs_since = logs_since,
      .logs_size_limit_bytes = logs_size_limit,
      .logs_until = logs_until_tp,
      .metrics_interval_seconds = metrics_interval_seconds,
      .metrics_samples = metrics_samples,
      .partition = partition,
      .tls_enabled = tls_enabled,
      .tls_insecure_skip_verify = tls_insecure_skip_verify,
      .k8s_namespace = k8s_namespace_name,
      .label_selector = label_select};

    ss::sstring expected_params(fmt::format(
      "debug bundle --output {}/{}.zip --verbose -Xuser={} -Xpass={} "
      "-Xsasl.mechanism={} --controller-logs-size-limit {}B "
      "--cpu-profiler-wait {}s --logs-since {} --logs-size-limit {}B "
      "--logs-until {} --metrics-interval {}s --metrics-samples {} --partition "
      "{}/{}/1,2,3 {}/{}/4,5,6 -Xtls.enabled=true "
      "-Xtls.insecure_skip_verify=false --namespace {} --label-selector "
      "{}={},{}={}\n",
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
      metrics_samples,
      tn1.ns,
      tn1.tp,
      tn2.ns,
      tn2.tp,
      k8s_namespace_name,
      label_select[0].key,
      label_select[0].value,
      label_select[1].key,
      label_select[1].value));

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, std::move(params));
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    const auto max_retries = 5;
    const auto timeout_time = 1s;

    auto num_retries = 0;
    while (num_retries < max_retries) {
        auto status = co_await _service.local().rpk_debug_bundle_status();
        ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

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
      debug_bundle::service_shard, [](debug_bundle::service& s) {
          return s.initiate_rpk_debug_bundle_collection(
            debug_bundle::job_id_t(uuid_t::create()), {});
      });
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto res2 = co_await _service.invoke_on(
      (debug_bundle::service_shard + 1) % ss::smp::count,
      [](debug_bundle::service& s) {
          return s.initiate_rpk_debug_bundle_collection(
            debug_bundle::job_id_t(uuid_t::create()), {});
      });

    ASSERT_FALSE_CORO(res2.has_value());
    EXPECT_EQ(
      res2.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_running);
}

TEST_F_CORO(debug_bundle_service_started_fixture, run_no_rpk_binary) {
    config::shard_local_cfg().rpk_path.set_value("/no/such/bin");

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      debug_bundle::job_id_t(uuid_t::create()), {});

    ASSERT_FALSE_CORO(res.has_value());
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
    ASSERT_FALSE_CORO(res.has_value());
    EXPECT_EQ(
      res.assume_error().code(), debug_bundle::error_code::internal_error);
}

TEST_F_CORO(debug_bundle_service_started_fixture, status_no_run) {
    auto res = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_FALSE_CORO(res.has_value());
    EXPECT_EQ(
      res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}

TEST_F_CORO(debug_bundle_service_started_fixture, terminate_process) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    {
        auto status = co_await _service.local().rpk_debug_bundle_status();
        ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

        EXPECT_EQ(
          status.assume_value().status,
          debug_bundle::debug_bundle_status::running);
    }

    {
        auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
          job_id);
        ASSERT_TRUE_CORO(term_res.has_value())
          << term_res.assume_error().message();
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
            ASSERT_TRUE_CORO(st.has_value()) << st.assume_error().message();
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
        ASSERT_FALSE_CORO(term_res.has_value());
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
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    {
        auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
          debug_bundle::job_id_t(uuid_t::create()));
        ASSERT_FALSE_CORO(term_res.has_value());
        EXPECT_EQ(
          term_res.assume_error().code(),
          debug_bundle::error_code::job_id_not_recognized);
    }
}

TEST_F_CORO(debug_bundle_service_started_fixture, termiate_never_ran) {
    auto term_res = co_await _service.local().cancel_rpk_debug_bundle(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_FALSE_CORO(term_res.has_value());
    EXPECT_EQ(
      term_res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}

TEST_F_CORO(debug_bundle_service_started_fixture, get_file_path) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    auto expected_path = _data_dir
                         / debug_bundle::service::debug_bundle_dir_name
                         / fmt::format("{}.zip", job_id);

    auto file_path = co_await _service.local().rpk_debug_bundle_path(job_id);
    ASSERT_TRUE_CORO(file_path.has_value())
      << file_path.assume_error().message();
    EXPECT_EQ(file_path.assume_value(), expected_path);
}

TEST_F_CORO(debug_bundle_service_started_fixture, get_file_path_not_started) {
    auto file_path = co_await _service.local().rpk_debug_bundle_path(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_FALSE_CORO(file_path.has_value());
    EXPECT_EQ(
      file_path.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}

TEST_F_CORO(
  debug_bundle_service_started_fixture, get_file_path_process_failed) {
    using namespace std::chrono_literals;
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::running);

    auto term_res = co_await _service.local().cancel_rpk_debug_bundle(job_id);
    ASSERT_TRUE_CORO(term_res.has_value()) << term_res.assume_error().message();

    ASSERT_NO_THROW_CORO(status = co_await wait_for_process_to_finish());

    ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::error);

    auto file_path = co_await _service.local().rpk_debug_bundle_path(job_id);
    ASSERT_FALSE_CORO(file_path.has_value());
    EXPECT_EQ(
      file_path.assume_error().code(),
      debug_bundle::error_code::process_failed);
}

TEST_F_CORO(debug_bundle_service_started_fixture, get_file_path_running) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto file_path = co_await _service.local().rpk_debug_bundle_path(job_id);
    ASSERT_FALSE_CORO(file_path.has_value());
    EXPECT_EQ(
      file_path.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_running);
}

TEST_F_CORO(debug_bundle_service_started_fixture, get_file_path_bad_job_id) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    auto file_path = co_await _service.local().rpk_debug_bundle_path(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_FALSE_CORO(file_path.has_value());
    EXPECT_EQ(
      file_path.assume_error().code(),
      debug_bundle::error_code::job_id_not_recognized);
}

TEST_F_CORO(debug_bundle_service_started_fixture, delete_file) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    auto expected_path = _data_dir
                         / debug_bundle::service::debug_bundle_dir_name
                         / fmt::format("{}.zip", job_id);

    ASSERT_TRUE_CORO(co_await ss::file_exists(expected_path.native()));

    auto del_res = co_await _service.local().delete_rpk_debug_bundle(job_id);
    ASSERT_TRUE_CORO(del_res.has_value()) << del_res.assume_error().message();

    EXPECT_FALSE(co_await ss::file_exists(expected_path.native()));
}

TEST_F_CORO(debug_bundle_service_started_fixture, delete_file_not_started) {
    auto del_res = co_await _service.local().delete_rpk_debug_bundle(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_FALSE_CORO(del_res.has_value());
    EXPECT_EQ(
      del_res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);
}

TEST_F_CORO(debug_bundle_service_started_fixture, delete_file_running) {
    debug_bundle::job_id_t job_id(uuid_t::create());
    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto del_res = co_await _service.local().delete_rpk_debug_bundle(job_id);
    ASSERT_FALSE_CORO(del_res.has_value());
    EXPECT_EQ(
      del_res.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_running);
}

TEST_F_CORO(debug_bundle_service_started_fixture, delete_file_bad_job_id) {
    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    auto del_res = co_await _service.local().delete_rpk_debug_bundle(
      debug_bundle::job_id_t(uuid_t::create()));
    ASSERT_FALSE_CORO(del_res.has_value());
    EXPECT_EQ(
      del_res.assume_error().code(),
      debug_bundle::error_code::job_id_not_recognized);
}

ss::future<> wait_for_file_to_be_created(
  const std::filesystem::path& file,
  const std::chrono::seconds timeout = std::chrono::seconds{10}) {
    const auto start_time = debug_bundle::clock::now();
    while (debug_bundle::clock::now() - start_time <= timeout) {
        if (co_await ss::file_exists(file.native())) {
            co_return;
        }
        co_await ss::check_for_io_immediately();
    }
    throw std::runtime_error(
      fmt::format("Timed out waiting for process file '{}' to exist", file));
}
TEST_F_CORO(debug_bundle_service_started_fixture, check_clean_up) {
    using namespace std::chrono_literals;
    debug_bundle::job_id_t job1(uuid_t::create());
    auto job1_file = _data_dir / debug_bundle::service::debug_bundle_dir_name
                     / fmt::format("{}.zip", job1);
    auto job1_out_file = _data_dir
                         / debug_bundle::service::debug_bundle_dir_name
                         / fmt::format("{}.out", job1);
    debug_bundle::job_id_t job2(uuid_t::create());
    auto job2_file = _data_dir / debug_bundle::service::debug_bundle_dir_name
                     / fmt::format("{}.zip", job2);
    auto job2_out_file = _data_dir
                         / debug_bundle::service::debug_bundle_dir_name
                         / fmt::format("{}.out", job2);
    {
        auto res
          = co_await _service.local().initiate_rpk_debug_bundle_collection(
            job1, {});
        ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();
        ASSERT_NO_THROW_CORO(
          co_await wait_for_file_to_be_created(job1_file, 10s));
        ASSERT_NO_THROW_CORO(
          co_await wait_for_file_to_be_created(job1_out_file, 10s));
    }
    {
        auto res
          = co_await _service.local().initiate_rpk_debug_bundle_collection(
            job2, {});
        ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();
        ASSERT_NO_THROW_CORO(
          co_await wait_for_file_to_be_created(job2_file, 10s));
        ASSERT_NO_THROW_CORO(
          co_await wait_for_file_to_be_created(job2_out_file, 10s));
    }
    EXPECT_FALSE(co_await ss::file_exists(job1_file.native()));
    EXPECT_FALSE(co_await ss::file_exists(job1_out_file.native()));
}

ss::future<> wait_for_kvstore_to_populate(
  storage::kvstore* kvstore,
  std::chrono::seconds timeout = std::chrono::seconds{10}) {
    const auto start_time = debug_bundle::clock::now();
    while (debug_bundle::clock::now() - start_time <= timeout) {
        auto metadata_buf = kvstore->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key));
        if (metadata_buf.has_value()) {
            co_return;
        }
        co_await ss::check_for_io_immediately();
    }
    throw std::runtime_error("Timed out waiting for metadata to be written");
}

ss::future<iobuf> read_file_contents(std::string_view file_path) {
    auto file = co_await ss::open_file_dma(
      file_path.data(), ss::open_flags::ro);
    auto h = ss::defer([file]() mutable { ssx::background = file.close(); });
    auto istrm = ss::make_file_input_stream(file);
    iobuf buf;
    auto ostrm = make_iobuf_ref_output_stream(buf);
    co_await ss::copy(istrm, ostrm);
    co_return buf;
}

TEST_F_CORO(debug_bundle_service_started_fixture, validate_metadata) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    std::filesystem::path job_file
      = _data_dir
        / fmt::format(
          "{}/{}.zip", debug_bundle::service::debug_bundle_dir_name, job_id);
    ASSERT_TRUE_CORO(co_await ss::file_exists(job_file.native()));

    std::filesystem::path process_output_file
      = _data_dir / debug_bundle::service::debug_bundle_dir_name
        / fmt::format("{}.out", job_id);

    ASSERT_NO_THROW_CORO(co_await wait_for_kvstore_to_populate(_kvstore.get()));

    auto metadata_buf = _kvstore->get(
      storage::kvstore::key_space::debug_bundle,
      bytes::from_string(debug_bundle::service::debug_bundle_metadata_key));
    ASSERT_TRUE_CORO(metadata_buf.has_value());

    iobuf_parser parser(std::move(metadata_buf.value()));
    auto metadata = serde::read<debug_bundle::metadata>(parser);
    EXPECT_EQ(metadata.job_id, job_id);
    EXPECT_EQ(metadata.debug_bundle_file_path, job_file);
    EXPECT_EQ(metadata.process_output_file_path, process_output_file);
    auto wait_result = metadata.get_wait_status();
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(
        wait_result));
    EXPECT_EQ(
      std::get<ss::experimental::process::wait_exited>(wait_result).exit_code,
      0);
    EXPECT_EQ(
      metadata.sha256_checksum,
      co_await debug_bundle::calculate_sha256_sum(job_file.native()));

    ASSERT_TRUE_CORO(co_await ss::file_exists(process_output_file.native()));
    auto process_output_buf = co_await read_file_contents(
      process_output_file.native());

    iobuf_parser process_output_parser(std::move(process_output_buf));
    auto po = serde::read<debug_bundle::process_output>(process_output_parser);
    ASSERT_FALSE_CORO(po.cout.empty());
    auto expected = ssx::sformat(
      "debug bundle --output {} --verbose\n", job_file.native());
    EXPECT_EQ(po.cout[0], expected) << po.cout[0] << " != " << expected;
}

TEST_F_CORO(debug_bundle_service_started_fixture, validate_restart) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status_restart.has_value())
      << status_restart.assume_error().message();

    EXPECT_EQ(status.assume_value(), status_restart.assume_value());
}

TEST_F_CORO(
  debug_bundle_service_started_fixture, validate_restart_remove_file) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    auto file_path = co_await _service.local().rpk_debug_bundle_path(job_id);
    ASSERT_TRUE_CORO(file_path.has_value())
      << file_path.assume_error().message();

    // Remove the file and restart the service - the metadata should be removed
    co_await ss::remove_file(file_path.assume_value().native());

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_FALSE_CORO(status_restart.has_value());
    EXPECT_EQ(
      status_restart.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);

    EXPECT_FALSE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}

TEST_F_CORO(debug_bundle_service_started_fixture, validate_invalid_sha) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    co_await run_bundle(job_id);

    ASSERT_NO_THROW_CORO(co_await wait_for_kvstore_to_populate(_kvstore.get()));

    {
        auto metadata_buf = _kvstore->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key));
        ASSERT_TRUE_CORO(metadata_buf.has_value());
        iobuf_parser parser(std::move(metadata_buf.value()));
        auto metadata = serde::read<debug_bundle::metadata>(parser);
        metadata.sha256_checksum = bytes::from_string("invalid");
        iobuf buf;
        serde::write(buf, std::move(metadata));
        co_await _kvstore->put(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key),
          std::move(buf));
    }

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_FALSE_CORO(status_restart.has_value());
    EXPECT_EQ(
      status_restart.assume_error().code(),
      debug_bundle::error_code::debug_bundle_process_never_started);

    EXPECT_FALSE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}

TEST_F_CORO(debug_bundle_service_started_fixture, restart_unsuccessful_run) {
    debug_bundle::job_id_t job_id(uuid_t::create());

    auto res = co_await _service.local().initiate_rpk_debug_bundle_collection(
      job_id, {});
    ASSERT_TRUE_CORO(res.has_value()) << res.assume_error().message();

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << res.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::running);

    auto term_res = co_await _service.local().cancel_rpk_debug_bundle(job_id);
    ASSERT_TRUE_CORO(term_res.has_value()) << term_res.assume_error().message();

    ASSERT_NO_THROW_CORO(status = co_await wait_for_process_to_finish());

    ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::error);

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status_restart.has_value())
      << status_restart.assume_error().message();
    EXPECT_EQ(
      status_restart.assume_value().status,
      debug_bundle::debug_bundle_status::error);

    EXPECT_TRUE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}

TEST_F_CORO(debug_bundle_service_started_fixture, restart_remove_out_file) {
    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    std::filesystem::path process_output_file
      = _data_dir / debug_bundle::service::debug_bundle_dir_name
        / fmt::format("{}.out", job_id);

    ASSERT_NO_THROW_CORO(
      co_await wait_for_file_to_be_created(process_output_file));

    co_await ss::remove_file(process_output_file.native());

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status_restart.has_value())
      << status_restart.assume_error().message();

    EXPECT_TRUE(status_restart.assume_value().cout.empty());
    EXPECT_TRUE(status_restart.assume_value().cerr.empty());
}

TEST_F_CORO(debug_bundle_service_started_fixture, restart_garbage_out_file) {
    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    std::filesystem::path process_output_file
      = _data_dir / debug_bundle::service::debug_bundle_dir_name
        / fmt::format("{}.out", job_id);

    ASSERT_NO_THROW_CORO(
      co_await wait_for_file_to_be_created(process_output_file));

    co_await ss::remove_file(process_output_file.native());

    auto h = co_await ss::open_file_dma(
      process_output_file.native().c_str(),
      ss::open_flags::rw | ss::open_flags::create);

    const auto data = random_generators::gen_alphanum_string(1024);
    auto istrm = make_iobuf_input_stream(iobuf::from(data));
    auto ostrm = co_await ss::make_file_output_stream(h);
    co_await ss::copy(istrm, ostrm);
    co_await ostrm.flush();
    co_await ostrm.close();

    co_await restart_service();

    auto status_restart = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status_restart.has_value())
      << status_restart.assume_error().message();

    EXPECT_TRUE(status_restart.assume_value().cout.empty());
    EXPECT_TRUE(status_restart.assume_value().cerr.empty());
}

TEST_F_CORO(debug_bundle_service_started_fixture, test_timeout_during_restart) {
    using namespace std::chrono_literals;
    const auto ttl = 2s;
    config::shard_local_cfg().debug_bundle_auto_removal_seconds.set_value(
      ttl.count());

    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    ASSERT_NO_THROW_CORO(co_await wait_for_kvstore_to_populate(_kvstore.get()));
    ASSERT_NO_THROW_CORO(co_await _service.stop());

    co_await ss::sleep(ttl + 1s);
    ASSERT_NO_THROW_CORO(co_await _service.start(_kvstore.get()));
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) { return s.start(); }));

    std::filesystem::path job_file
      = _data_dir
        / fmt::format(
          "{}/{}.zip", debug_bundle::service::debug_bundle_dir_name, job_id);
    EXPECT_FALSE(co_await ss::file_exists(job_file.native()));

    std::filesystem::path process_output_file
      = _data_dir / debug_bundle::service::debug_bundle_dir_name
        / fmt::format("{}.out", job_id);
    EXPECT_FALSE(co_await ss::file_exists(process_output_file.native()));
    EXPECT_FALSE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}

TEST_F_CORO(debug_bundle_service_started_fixture, test_debug_bundle_expired) {
    using namespace std::chrono_literals;
    const auto ttl = 4s;
    config::shard_local_cfg().debug_bundle_auto_removal_seconds.set_value(
      ttl.count());

    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    auto status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();

    ASSERT_EQ_CORO(
      status.assume_value().status, debug_bundle::debug_bundle_status::success);

    co_await ss::sleep(2s);
    status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();

    auto job_file = generate_job_file_path(job_id);
    auto process_output_file = generate_process_output_file_path(job_id);

    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::success);
    EXPECT_TRUE(co_await ss::file_exists(job_file.native()));
    EXPECT_TRUE(co_await ss::file_exists(process_output_file.native()));
    EXPECT_TRUE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());

    co_await ss::sleep(3s);

    status = co_await _service.local().rpk_debug_bundle_status();
    ASSERT_TRUE_CORO(status.has_value()) << status.assume_error().message();
    EXPECT_EQ(
      status.assume_value().status, debug_bundle::debug_bundle_status::expired);
    EXPECT_FALSE(co_await ss::file_exists(job_file.native()));
    EXPECT_FALSE(co_await ss::file_exists(process_output_file.native()));
    EXPECT_FALSE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}

TEST_F_CORO(
  debug_bundle_service_started_fixture,
  test_debug_bundle_expired_after_config_set) {
    using namespace std::chrono_literals;
    const auto ttl = 1s;

    debug_bundle::job_id_t job_id(uuid_t::create());
    co_await run_bundle(job_id);

    auto job_file = generate_job_file_path(job_id);
    auto process_output_file = generate_process_output_file_path(job_id);

    ASSERT_NO_THROW_CORO(
      co_await wait_for_file_to_be_created(job_file.native()));
    ASSERT_NO_THROW_CORO(
      co_await wait_for_file_to_be_created(process_output_file.native()));
    ASSERT_NO_THROW_CORO(co_await wait_for_kvstore_to_populate(_kvstore.get()));

    // wait for twice the TTL and then set the config
    co_await ss::sleep(ttl * 2);
    config::shard_local_cfg().debug_bundle_auto_removal_seconds.set_value(
      ttl.count());

    // we need to permit the scheduled background fiber to run
    ASSERT_NO_THROW_CORO(
      co_await wait_for_status(debug_bundle::debug_bundle_status::expired));
    co_await ss::check_for_io_immediately();
    EXPECT_FALSE(co_await ss::file_exists(job_file.native()));
    EXPECT_FALSE(co_await ss::file_exists(process_output_file.native()));
    EXPECT_FALSE(
      _kvstore
        ->get(
          storage::kvstore::key_space::debug_bundle,
          bytes::from_string(debug_bundle::service::debug_bundle_metadata_key))
        .has_value());
}
