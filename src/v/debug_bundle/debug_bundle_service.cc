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

#include "debug_bundle_service.h"

#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "debug_bundle/error.h"
#include "debug_bundle/types.h"
#include "security/acl.h"
#include "ssx/future-util.h"
#include "utils/external_process.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/process.hh>
#include <seastar/util/variant_utils.hh>

#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>
#include <re2/re2.h>

#include <optional>
#include <string_view>

using namespace std::chrono_literals;

namespace debug_bundle {
static ss::logger lg{"debug-bundle-service"};

namespace {
constexpr std::string_view output_variable = "--output";
constexpr std::string_view verbose_variable = "--verbose";
constexpr std::string_view username_variable = "-Xuser";
constexpr std::string_view password_variable = "-Xpass";
constexpr std::string_view sasl_mechanism_variable = "-Xsasl.mechanism";
constexpr std::string_view controller_logs_size_limit_variable
  = "--controller-logs-size-limit";
constexpr std::string_view cpu_profiler_wait_variable = "--cpu-profiler-wait";
constexpr std::string_view logs_since_variable = "--logs-since";
constexpr std::string_view logs_size_limit_variable = "--logs-size-limit";
constexpr std::string_view logs_until_variable = "--logs-until";
constexpr std::string_view metrics_interval_variable = "--metrics-interval";
constexpr std::string_view partition_variable = "--partition";
constexpr std::string_view tls_enabled_variable = "-Xtls.enabled";
constexpr std::string_view tls_insecure_skip_verify_variable
  = "-Xtls.insecure_skip_verify";
constexpr std::string_view k8s_namespace_variable = "--namespace";

bool contains_sensitive_info(const ss::sstring& arg) {
    if (arg.find(password_variable) != ss::sstring::npos) {
        return true;
    }
    return false;
}
void print_arguments(const std::vector<ss::sstring>& args) {
    auto msg = boost::algorithm::join_if(args, " ", [](const ss::sstring& arg) {
        return !contains_sensitive_info(arg);
    });
    vlog(lg.debug, "Starting RPK debug bundle: {}", msg);
}

std::filesystem::path form_debug_bundle_file_path(
  const std::filesystem::path& base_path, job_id_t job_id) {
    return base_path / fmt::format("{}.zip", job_id);
}

bool is_valid_rfc1123(std::string_view ns) {
    // Regular expression for RFC1123 hostname validation
    constexpr std::string_view rfc1123_pattern
      = R"(^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?))";

    // Validate the hostname against the regular expression using RE2
    return RE2::FullMatch(ns, RE2(rfc1123_pattern));
}

bool is_valid_k8s_namespace(std::string_view ns) {
    constexpr auto max_ns_length = 63;
    return !ns.empty() && ns.size() <= max_ns_length && is_valid_rfc1123(ns);
}
} // namespace

struct service::output_handler {
    using consumption_result_type =
      typename ss::input_stream<char>::consumption_result_type;
    using stop_consuming_type =
      typename consumption_result_type::stop_consuming_type;
    using tmp_buf = stop_consuming_type::tmp_buf;

    chunked_vector<ss::sstring>& output_buffer;

    ss::future<consumption_result_type> operator()(tmp_buf buf) {
        output_buffer.emplace_back(ss::sstring{buf.begin(), buf.end()});
        co_return ss::continue_consuming{};
    }
};

class service::debug_bundle_process {
public:
    debug_bundle_process(
      job_id_t job_id,
      std::unique_ptr<external_process::external_process> rpk_process,
      std::filesystem::path output_file_path)
      : _job_id(job_id)
      , _rpk_process(std::move(rpk_process))
      , _output_file_path(std::move(output_file_path))
      , _created_time(clock::now()) {
        _rpk_process->set_stdout_consumer(
          output_handler{.output_buffer = _cout});
        _rpk_process->set_stderr_consumer(
          output_handler{.output_buffer = _cerr});
    }

    debug_bundle_process() = delete;
    debug_bundle_process(debug_bundle_process&&) = default;
    debug_bundle_process& operator=(debug_bundle_process&&) = default;
    debug_bundle_process(const debug_bundle_process&) = delete;
    debug_bundle_process& operator=(const debug_bundle_process&) = delete;

    ~debug_bundle_process() {
        if (_rpk_process) {
            vassert(
              !_rpk_process->is_running(),
              "Destroying process struct without waiting for process to "
              "finish");
        }
    }

private:
    job_id_t _job_id;
    std::unique_ptr<external_process::external_process> _rpk_process;
    std::optional<ss::experimental::process::wait_status> _wait_result;
    std::filesystem::path _output_file_path;
    chunked_vector<ss::sstring> _cout;
    chunked_vector<ss::sstring> _cerr;
    clock::time_point _created_time;

    friend class service;
};

service::service(const std::filesystem::path& data_dir)
  : _debug_bundle_dir(data_dir / debug_bundle_dir_name)
  , _rpk_path_binding(config::shard_local_cfg().rpk_path.bind())
  , _process_control_mutex("debug_bundle_service::process_control") {}

service::~service() noexcept = default;

ss::future<> service::start() {
    if (ss::this_shard_id() != service_shard) {
        co_return;
    }

    try {
        lg.trace("Creating {}", _debug_bundle_dir);
        co_await ss::recursive_touch_directory(_debug_bundle_dir.native());
    } catch (const std::exception& e) {
        throw std::system_error(error_code::internal_error, e.what());
    }

    if (!co_await ss::file_exists(_rpk_path_binding().native())) {
        lg.error(
          "Current specified RPK location {} does not exist!  Debug "
          "bundle creation is not available until this is fixed!",
          _rpk_path_binding().native());
    }

    lg.debug("Service started");
}

ss::future<> service::stop() {
    lg.debug("Service stopping");
    if (ss::this_shard_id() == service_shard) {
        if (is_running()) {
            try {
                co_await _rpk_process->_rpk_process->terminate(1s);
            } catch (const std::exception& e) {
                lg.warn(
                  "Failed to terminate running process while stopping service: "
                  "{}",
                  e.what());
            }
        }
    }
    co_await _gate.close();
}

ss::future<result<void>> service::initiate_rpk_debug_bundle_collection(
  job_id_t job_id, debug_bundle_parameters params) {
    auto hold = _gate.hold();
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard,
          [job_id, params = std::move(params)](service& s) mutable {
              return s.initiate_rpk_debug_bundle_collection(
                job_id, std::move(params));
          });
    }
    auto units = co_await _process_control_mutex.get_units();
    if (!co_await ss::file_exists(_rpk_path_binding().native())) {
        co_return error_info(
          error_code::rpk_binary_not_present,
          fmt::format("{} not present", _rpk_path_binding().native()));
    }

    if (_rpk_process) {
        // Must wait for both the process to no longer be running and for the
        // wait result to be populated
        if (is_running()) {
            co_return error_info(
              error_code::debug_bundle_process_running,
              "Debug process already running");
        }
    }

    auto args_res = build_rpk_arguments(job_id, std::move(params));
    if (!args_res.has_value()) {
        co_return args_res.assume_error();
    }
    auto args = std::move(args_res.assume_value());
    if (lg.is_enabled(ss::log_level::debug)) {
        print_arguments(args);
    }

    try {
        _rpk_process = std::make_unique<debug_bundle_process>(
          job_id,
          co_await external_process::external_process::create_external_process(
            std::move(args)),
          form_debug_bundle_file_path(_debug_bundle_dir, job_id));
    } catch (const std::exception& e) {
        _rpk_process.reset();
        co_return error_info(
          error_code::internal_error,
          fmt::format("Starting rpk debug bundle failed: {}", e.what()));
    }

    // Kick off the wait by waiting for the process to finish and then emplacing
    // the result
    ssx::spawn_with_gate(_gate, [this]() {
        return _rpk_process->_rpk_process->wait()
          .then([this](auto res) { _rpk_process->_wait_result.emplace(res); })
          .handle_exception_type([this](const std::exception& e) {
              lg.error(
                "wait() failed while running rpk debug bundle: {}", e.what());
              _rpk_process->_wait_result.emplace(
                ss::experimental::process::wait_exited{1});
          });
    });

    co_return outcome::success();
}

ss::future<result<void>> service::cancel_rpk_debug_bundle(job_id_t job_id) {
    auto hold = _gate.hold();
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard,
          [job_id](service& s) { return s.cancel_rpk_debug_bundle(job_id); });
    }
    auto units = co_await _process_control_mutex.get_units();
    auto status = process_status();
    if (!status.has_value()) {
        co_return error_info(error_code::debug_bundle_process_never_started);
    } else if (!is_running()) {
        co_return error_info(error_code::debug_bundle_process_not_running);
    }

    vassert(
      _rpk_process,
      "_rpk_process should be populated if the process has been executed");

    if (job_id != _rpk_process->_job_id) {
        co_return error_info(error_code::job_id_not_recognized);
    }

    try {
        co_await _rpk_process->_rpk_process->terminate(1s);
    } catch (const std::system_error& e) {
        if (
          e.code() == external_process::error_code::process_already_completed) {
            co_return error_info(error_code::debug_bundle_process_not_running);
        }
        co_return (error_info(error_code::internal_error, e.what()));
    } catch (const std::exception& e) {
        co_return error_info(error_code::internal_error, e.what());
    }

    co_return outcome::success();
}

ss::future<result<debug_bundle_status_data>>
service::rpk_debug_bundle_status() {
    auto hold = _gate.hold();
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(service_shard, [](service& s) {
            return s.rpk_debug_bundle_status();
        });
    }
    auto status = process_status();
    if (!status.has_value()) {
        co_return error_info(error_code::debug_bundle_process_never_started);
    }

    vassert(
      _rpk_process,
      "_rpk_process should be populated if the process has been executed");

    co_return debug_bundle_status_data{
      .job_id = _rpk_process->_job_id,
      .status = status.value(),
      .created_timestamp = _rpk_process->_created_time,
      .file_name = _rpk_process->_output_file_path.filename().native(),
      .cout = _rpk_process->_cout.copy(),
      .cerr = _rpk_process->_cerr.copy()};
}

ss::future<result<std::filesystem::path>> service::rpk_debug_bundle_path() {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(
          service_shard, [](service& s) { return s.rpk_debug_bundle_path(); });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

ss::future<result<void>> service::delete_rpk_debug_bundle() {
    if (ss::this_shard_id() != service_shard) {
        co_return co_await container().invoke_on(service_shard, [](service& s) {
            return s.delete_rpk_debug_bundle();
        });
    }
    co_return error_info(error_code::debug_bundle_process_never_started);
}

result<std::vector<ss::sstring>>
service::build_rpk_arguments(job_id_t job_id, debug_bundle_parameters params) {
    std::vector<ss::sstring> rv{
      _rpk_path_binding().native(), "debug", "bundle"};
    rv.emplace_back(output_variable);
    rv.emplace_back(form_debug_bundle_file_path(_debug_bundle_dir, job_id));
    rv.emplace_back(verbose_variable);
    if (params.authn_options.has_value()) {
        ss::visit(
          params.authn_options.value(),
          [&rv](const scram_creds& creds) mutable {
              rv.emplace_back(
                ssx::sformat("{}={}", username_variable, creds.username));
              rv.emplace_back(
                ssx::sformat("{}={}", password_variable, creds.password));
              rv.emplace_back(ssx::sformat(
                "{}={}", sasl_mechanism_variable, creds.mechanism));
          });
    }
    if (params.controller_logs_size_limit_bytes.has_value()) {
        rv.emplace_back(controller_logs_size_limit_variable);
        rv.emplace_back(
          ssx::sformat("{}B", params.controller_logs_size_limit_bytes.value()));
    }
    if (params.cpu_profiler_wait_seconds.has_value()) {
        rv.emplace_back(cpu_profiler_wait_variable);
        rv.emplace_back(ssx::sformat(
          "{}s", params.cpu_profiler_wait_seconds.value().count()));
    }
    if (params.logs_since.has_value()) {
        rv.emplace_back(logs_since_variable);
        rv.emplace_back(ssx::sformat("{}", params.logs_since.value()));
    }
    if (params.logs_size_limit_bytes.has_value()) {
        rv.emplace_back(logs_size_limit_variable);
        rv.emplace_back(
          ssx::sformat("{}B", params.logs_size_limit_bytes.value()));
    }
    if (params.logs_until.has_value()) {
        rv.emplace_back(logs_until_variable);
        rv.emplace_back(ssx::sformat("{}", params.logs_until.value()));
    }
    if (params.metrics_interval_seconds.has_value()) {
        rv.emplace_back(metrics_interval_variable);
        rv.emplace_back(
          ssx::sformat("{}s", params.metrics_interval_seconds.value().count()));
    }
    if (params.partition.has_value()) {
        rv.emplace_back(partition_variable);
        rv.emplace_back(
          ssx::sformat("{}", fmt::join(params.partition.value(), " ")));
    }
    if (params.tls_enabled.has_value()) {
        rv.emplace_back(
          ssx::sformat("{}={}", tls_enabled_variable, *params.tls_enabled));
    }
    if (params.tls_insecure_skip_verify.has_value()) {
        rv.emplace_back(ssx::sformat(
          "{}={}",
          tls_insecure_skip_verify_variable,
          *params.tls_insecure_skip_verify));
    }
    if (params.k8s_namespace.has_value()) {
        if (!is_valid_k8s_namespace(params.k8s_namespace.value()())) {
            return error_info(
              error_code::invalid_parameters, "Invalid k8s namespace name");
        }
        rv.emplace_back(k8s_namespace_variable);
        rv.emplace_back(*params.k8s_namespace);
    }

    return rv;
}

std::optional<debug_bundle_status> service::process_status() const {
    if (_rpk_process == nullptr) {
        return std::nullopt;
    }
    if (
      _rpk_process->_rpk_process->is_running()
      || !_rpk_process->_wait_result.has_value()) {
        return debug_bundle_status::running;
    }
    return ss::visit(
      _rpk_process->_wait_result.value(),
      [](ss::experimental::process::wait_exited e) {
          if (e.exit_code == 0) {
              return debug_bundle_status::success;
          } else {
              return debug_bundle_status::error;
          }
      },
      [](ss::experimental::process::wait_signaled) {
          return debug_bundle_status::error;
      });
}

bool service::is_running() const {
    if (auto status = process_status();
        status.has_value() && *status == debug_bundle_status::running) {
        return true;
    } else {
        return false;
    }
}

} // namespace debug_bundle
