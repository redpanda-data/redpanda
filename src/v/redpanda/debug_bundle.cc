/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/debug_bundle.h"

#include "config/configuration.h"
#include "ssx/future-util.h"
#include "utils/gate_guard.h"
#include "utils/string_switch.h"
#include "vlog.h"

#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/log.hh>
#include <seastar/util/process.hh>

#include <fmt/format.h>

#include <filesystem>
#include <optional>

static ss::logger bundle_log{"debug_bundle"};

using consumption_result_type
  = debug_bundle::rpk_consumer::consumption_result_type;
using stop_consuming_type = debug_bundle::rpk_consumer::stop_consuming_type;
using tmp_buf = stop_consuming_type::tmp_buf;

ss::future<consumption_result_type>
debug_bundle::rpk_consumer::operator()(tmp_buf buf) {
    std::string str_buf{buf.begin(), buf.end()}, line;
    vlog(bundle_log.trace, "Consumer read {}", str_buf);
    std::stringstream ss(str_buf);
    while (!ss.eof()) {
        std::getline(ss, line);
        if (line.starts_with("Debug bundle saved to")) {
            vlog(bundle_log.trace, "Stop condition reached on line {}", line);
            status = debug_bundle_status::not_running;
            return make_ready_future<consumption_result_type>(
              stop_consuming_type({}));
        }
    }
    return make_ready_future<consumption_result_type>(ss::continue_consuming{});
}

constexpr std::string_view to_string_view(debug_bundle_status bundle_status) {
    switch (bundle_status) {
    case debug_bundle_status::not_running:
        return "not-running";
    case debug_bundle_status::running:
        return "running";
    }
    return "unknown";
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<debug_bundle_status>
from_string_view<debug_bundle_status>(std::string_view sv) {
    return string_switch<std::optional<debug_bundle_status>>(sv)
      .match(
        to_string_view(debug_bundle_status::not_running),
        debug_bundle_status::not_running)
      .match(
        to_string_view(debug_bundle_status::running),
        debug_bundle_status::running)
      .default_match(std::nullopt);
}

debug_bundle::debug_bundle(
  const std::filesystem::path& write_dir, const std::filesystem::path& rpk_path)
  : _status{debug_bundle_status::not_running}
  , _write_dir{write_dir}
  , _in_progress_filename{"debug-bundle.zip"}
  , _rpk_cmd{rpk_path}
  , _transfer_handle{
      detail::make_bundle_filename(get_write_dir(), _in_progress_filename),
      nullptr,
      false} {}

ss::future<> debug_bundle::stop() {
    using namespace std::chrono_literals;
    vlog(bundle_log.info, "Stopping debug bundle ...");
    co_await _rpk_gate.close();
}

ss::future<> debug_bundle::start_creating_bundle(
  const request_auth_result& auth_state, const debug_bundle_params params) {
    if (_status == debug_bundle_status::running) {
        throw ss::httpd::base_exception(
          "Too many requests: bundle is already running",
          ss::http::reply::status_type::too_many_requests);
    }

    if (ss::this_shard_id() != debug_bundle_shard_id) {
        return container().invoke_on(
          debug_bundle_shard_id,
          [&auth_state, params{std::move(params)}](debug_bundle& b) {
              return b.start_creating_bundle(auth_state, std::move(params));
          });
    }

    ss::sstring host_path;
    auto host_env = std::getenv("PATH");
    if (!host_env) {
        vlog(
          bundle_log.warn,
          "Failed to get 'PATH' environmental variable, the debug bundle may "
          "be incomplete due to missing dependencies");
    } else {
        host_path = fmt::format("PATH={}", host_env);
    }

    auto filename = detail::make_bundle_filename(
      get_write_dir(), _in_progress_filename);
    std::vector<ss::sstring> rpk_argv{
      _rpk_cmd.string(), "debug", "bundle", "--output", filename};
    // Add SASL creds to RPK flags if SASL is enabled.
    // No need to check for sasl on the broker endpoint because that is for
    // Kafka brokers where as the bundle is managed by the Admin server only.
    if (config::shard_local_cfg().enable_sasl()) {
        rpk_argv.push_back("--user");
        rpk_argv.push_back(auth_state.get_username());
        rpk_argv.push_back("--password");
        rpk_argv.push_back(auth_state.get_password());
        rpk_argv.push_back("--sasl-mechanism");
        rpk_argv.push_back(auth_state.get_mechanism());
    }

    if (params.logs_since.has_value()) {
        rpk_argv.push_back("--logs-since");
        rpk_argv.push_back(params.logs_since.value());
    }

    if (params.logs_until.has_value()) {
        rpk_argv.push_back("--logs-until");
        rpk_argv.push_back(params.logs_until.value());
    }

    if (params.logs_size_limit.has_value()) {
        rpk_argv.push_back("--logs-size-limit");
        rpk_argv.push_back(params.logs_size_limit.value());
    }

    if (params.metrics_interval.has_value()) {
        rpk_argv.push_back("--metrics-interval");
        rpk_argv.push_back(params.metrics_interval.value());
    }

    ssx::background
      = ss::experimental::spawn_process(
          _rpk_cmd, {.argv = std::move(rpk_argv), .env = {host_path}})
          .then([this](auto process) {
              auto stdout = process.stdout();
              _status = debug_bundle_status::running;
              return ss::do_with(
                std::move(process),
                std::move(stdout),
                [this](auto& p, auto& stdout) {
                    gate_guard guard{_rpk_gate};
                    return stdout.consume(rpk_consumer(_status))
                      .finally([this, &p, guard{std::move(guard)}]() mutable {
                          return p.wait()
                            .then([this](ss::experimental::process::wait_status
                                           wstatus) {
                                auto* exit_status = std::get_if<
                                  ss::experimental::process::wait_exited>(
                                  &wstatus);
                                if (exit_status == nullptr) {
                                    vlog(
                                      bundle_log.error,
                                      "Failed to run RPK, exit status is "
                                      "undefined");
                                }

                                if (exit_status->exit_code != 0) {
                                    vlog(
                                      bundle_log.error,
                                      "Failed to run RPK, exit code {}",
                                      exit_status->exit_code);
                                } else {
                                    vlog(
                                      bundle_log.debug,
                                      "RPK successfully created debug bundle "
                                      "{}",
                                      _in_progress_filename);
                                }
                            })
                            .finally([&p, guard{std::move(guard)}]() mutable {
                                // Make sure the process dies, first gracefully
                                // and then forcefully.
                                // Please note: Seastar reports an ignored
                                // exceptional future when SIGTERM or SIGKILL is
                                // called on an already dead process.
                                p.terminate();
                                p.kill();
                            });
                      });
                });
          });

    return ss::make_ready_future<>();
}

ss::future<debug_bundle_status> debug_bundle::get_status() {
    if (ss::this_shard_id() != debug_bundle_shard_id) {
        co_return co_await container().invoke_on(
          debug_bundle_shard_id,
          [](debug_bundle& b) mutable { return b.get_status(); });
    }

    co_return _status;
}

ss::sstring detail::make_bundle_filename(
  const std::filesystem::path& write_dir, ss::sstring& filename) {
    return fmt::format("{}/{}", write_dir.string(), filename);
}

void detail::throw_if_bundle_dne(
  debug_bundle_status bundle_status,
  const std::filesystem::path& write_dir,
  ss::sstring& filename) {
    auto bundle_name = detail::make_bundle_filename(write_dir, filename);
    if (!std::filesystem::exists(bundle_name.c_str())) {
        if (bundle_status == debug_bundle_status::running) {
            throw ss::httpd::not_found_exception(
              "The debug bundle is running but not yet ready");
        } else {
            throw ss::httpd::base_exception(
              "The debug bundle is not running or it is not on disk",
              ss::http::reply::status_type::gone);
        }
    } else {
        if (bundle_status == debug_bundle_status::running) {
            throw ss::httpd::not_found_exception(
              "The debug bundle is running but not yet ready");
        }
    }
}

ss::future<std::unique_ptr<ss::http::reply>> debug_bundle::fetch_bundle(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    if (ss::this_shard_id() != debug_bundle_shard_id) {
        co_return co_await container().invoke_on(
          debug_bundle_shard_id,
          [req{std::move(req)}, rep{std::move(rep)}](debug_bundle& b) mutable {
              return b.fetch_bundle(std::move(req), std::move(rep));
          });
    }

    debug_bundle_status bundle_status{debug_bundle_status::not_running};
    auto bundle_name = req->param["filename"];
    if (bundle_name == _in_progress_filename) {
        bundle_status = co_await get_status();
    }
    detail::throw_if_bundle_dne(bundle_status, get_write_dir(), bundle_name);

    co_return co_await _transfer_handle.handle(
      {}, std::move(req), std::move(rep));
}

ss::future<> debug_bundle::remove_bundle(ss::sstring& filename) {
    if (ss::this_shard_id() != debug_bundle_shard_id) {
        co_await container().invoke_on(
          debug_bundle_shard_id,
          [&filename](debug_bundle& b) { return b.remove_bundle(filename); });
    }

    debug_bundle_status bundle_status{debug_bundle_status::not_running};
    if (filename == _in_progress_filename) {
        bundle_status = co_await get_status();
    };
    detail::throw_if_bundle_dne(bundle_status, get_write_dir(), filename);

    co_await ss::remove_file(
      detail::make_bundle_filename(get_write_dir(), filename));
    co_await ss::sync_directory(get_write_dir().c_str());
}
