// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/node_config.h"
#include "redpanda/debug_bundle.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/file.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>

inline ss::logger test_log("test");

using namespace std::chrono_literals;

struct bundle_fixture : public redpanda_thread_fixture {
    bundle_fixture()
      : redpanda_thread_fixture(
        model::node_id(1),
        9092,
        33145,
        8082,
        8081,
        43189,
        {},
        ssx::sformat("test.dir_{}", time(0)),
        std::nullopt,
        true,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        configure_node_id::yes,
        empty_seed_starts_cluster::yes,
        std::nullopt,
        true) {}
};

FIXTURE_TEST(check_environment, bundle_fixture) {
    wait_for_controller_leadership().get();

    // Check making rpk error value
    {
        auto err = make_error_code(debug_bundle::errc::success);
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err), debug_bundle::errc::success);
    }

    auto write_dir_conf = config::node().debug_bundle_write_dir.value();
    auto rpk_path = config::node().rpk_path.value();
    // The checks in this test do not use futures, therefore a sharded bundle is
    // not needed.
    debug_bundle::debug_bundle bundle{write_dir_conf, rpk_path};

    // Check creating full bundle filename
    {
        BOOST_CHECK_EQUAL(bundle.get_write_dir(), write_dir_conf);
        auto write_dir_size = bundle.get_write_dir().string().size();
        auto full_file_path = debug_bundle::make_bundle_filename(
          bundle.get_write_dir(),
          ss::sstring{debug_bundle::default_bundle_name});
        BOOST_CHECK_EQUAL(
          full_file_path.substr(0, write_dir_size),
          bundle.get_write_dir().string());
        // Ignore the slash in the file path
        BOOST_CHECK_EQUAL(
          full_file_path.substr(write_dir_size + 1),
          debug_bundle::default_bundle_name);
    }

    // Check debug bundle params default constructor
    {
        debug_bundle::debug_bundle_params params;
        BOOST_CHECK(params.logs_since == std::nullopt);
        BOOST_CHECK(params.logs_until == std::nullopt);
        BOOST_CHECK(params.logs_size_limit == std::nullopt);
        BOOST_CHECK(params.metrics_interval == std::nullopt);
    }

    // Check rpk args with bundle params and credentials
    {
        std::chrono::year_month_day logs_since_ymd{
          std::chrono::year{2023}, std::chrono::month{5}, std::chrono::day{31}};
        std::chrono::year_month_day logs_until_ymd{
          std::chrono::year{2023}, std::chrono::month{6}, std::chrono::day{11}};
        debug_bundle::debug_bundle_params params;
        params.logs_since = std::make_optional(logs_since_ymd);
        params.logs_until = std::make_optional(logs_until_ymd);
        params.logs_size_limit = std::make_optional<ss::sstring>("100MiB");
        debug_bundle::metric_interval_t metric{10s, "ms"};
        params.metrics_interval = std::make_optional(metric);

        debug_bundle::debug_bundle_credentials creds{
          .username = security::credential_user{"admin"},
          .password = security::credential_password{"admin"},
          .mechanism = ss::sstring{"SCRAM-SHA-256"}};

        auto full_file_path = debug_bundle::make_bundle_filename(
          bundle.get_write_dir(),
          ss::sstring{debug_bundle::default_bundle_name});
        auto is_cred_flag = [&creds](const ss::sstring& flag) {
            return flag == "--user" || flag == "--password"
                   || flag == "--sasl-mechanism" || flag == creds.username
                   || flag == creds.password || flag == creds.mechanism;
        };
        auto is_params_flag = [&params](const ss::sstring& flag) {
            return flag == "--logs-since" || flag == "--logs-until"
                   || flag == "--logs-size-limit"
                   || flag == "--metrics-interval"
                   || flag
                        == debug_bundle::to_journalctl_fmt(
                          params.logs_since.value())
                   || flag
                        == debug_bundle::to_journalctl_fmt(
                          params.logs_until.value())
                   || flag == params.logs_size_limit.value()
                   || flag
                        == debug_bundle::to_time_fmt(
                          params.metrics_interval.value());
        };
        auto is_flag_ok = [&is_cred_flag,
                           &is_params_flag,
                           &rpk_path,
                           &full_file_path](const ss::sstring& flag) {
            if (flag == rpk_path.string()) {
                vlog(test_log.info, "Found rpk path {}", flag);
                return true;
            }

            // These flags are hardcoded within "make_rpk_args"
            if (flag == "debug" || flag == "bundle" || flag == "--output") {
                return true;
            }

            if (flag == full_file_path) {
                vlog(test_log.info, "Found file path {}", flag);
                return true;
            }

            if (is_params_flag(flag)) {
                vlog(test_log.info, "Found param {}", flag);
                return true;
            }

            if (is_cred_flag(flag)) {
                vlog(test_log.info, "Found cred {}", flag);
                return true;
            }

            return false;
        };

        std::optional<debug_bundle::debug_bundle_credentials> creds_opt
          = std::make_optional(creds);
        debug_bundle::rpk_debug_bundle rpk{
          ss::sstring{""},
          ss::sstring{""},
          write_dir_conf,
          rpk_path,
          params,
          creds_opt};
        auto rpk_argv = rpk.make_rpk_args();
        for (const auto& flag : rpk_argv) {
            BOOST_CHECK(is_flag_ok(flag));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(check_wait_status_handler) {
    // const std::filesystem::path true_cmd{"/bin/true"},
    // false_cmd{"/bin/false"}, sleep_cmd{"/bin/sleep"};

    debug_bundle::rpk_debug_bundle rpk;

    auto make_exit_code = [](int code) {
        return ss::experimental::process::wait_exited{code};
    };

    auto make_exit_signal = [](int signal) {
        return ss::experimental::process::wait_signaled{signal};
    };

    // Check failed cmd
    {
        // Expect to see an error log with exit status 1
        auto err = rpk.handle_wait_status(make_exit_code(1));
        BOOST_CHECK_EQUAL(err, debug_bundle::errc::process_fail);
    }

    // Check sucessful cmd
    {
        auto err = rpk.handle_wait_status(make_exit_code(0));
        BOOST_CHECK_EQUAL(err, debug_bundle::errc::success);
    }

    // std::vector<ss::sstring> sleep_argv{sleep_cmd.string(), "30"};

    // Check SIGTERM'd cmd
    {
        // Expect to see an error log with signal 15
        auto err = rpk.handle_wait_status(make_exit_signal(15));
        BOOST_CHECK_EQUAL(err, debug_bundle::errc::process_fail);
    }

    // Check SIGKILL'd cmd
    {
        // Expect to see an error log with signal 9
        auto err = rpk.handle_wait_status(make_exit_signal(9));
        BOOST_CHECK_EQUAL(err, debug_bundle::errc::process_fail);
    }
}

SEASTAR_THREAD_TEST_CASE(ready_debug_bundle) {
    ss::sstring invalid_file{"xxx.zip"};
    ss::sharded<debug_bundle::debug_bundle> bundle;
    bundle
      .start(
        config::node().debug_bundle_write_dir.value(),
        config::node().rpk_path.value())
      .get();
    auto action = ss::defer([&bundle] { bundle.stop().get(); });

    // Check for invalid file name
    {
        auto err = bundle.local().is_ready(invalid_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err),
          debug_bundle::errc::invalid_filename);

        err = bundle.local().remove_bundle(invalid_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err),
          debug_bundle::errc::invalid_filename);
    }

    // Check that the bundle is not on disk
    {
        auto in_progress_file = ss::sstring{debug_bundle::default_bundle_name};
        auto err = bundle.local().is_ready(in_progress_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err),
          debug_bundle::errc::bundle_not_on_disk);

        err = bundle.local().remove_bundle(in_progress_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err),
          debug_bundle::errc::bundle_not_on_disk);
    }

    // Put zip archive on disk
    auto zip_filename = debug_bundle::make_bundle_filename(
      bundle.local().get_write_dir(),
      ss::sstring{debug_bundle::default_bundle_name});
    auto zip_file
      = ss::open_file_dma(zip_filename, ss::open_flags::create).get0();
    auto exists = ss::file_exists(zip_filename).get();
    BOOST_CHECK(exists);

    // Check that the bundle is "ready" since it is on disk, then remove it
    {
        auto in_progress_file = ss::sstring{debug_bundle::default_bundle_name};
        auto err = bundle.local().is_ready(in_progress_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err), debug_bundle::errc::success);

        err = bundle.local().remove_bundle(in_progress_file).get();
        BOOST_CHECK_EQUAL(
          debug_bundle::make_rpk_err_value(err), debug_bundle::errc::success);

        exists = ss::file_exists(zip_filename).get();
        BOOST_CHECK(!exists);
    }

    zip_file.close().get();
}
