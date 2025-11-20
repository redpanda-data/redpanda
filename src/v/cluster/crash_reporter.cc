/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/crash_reporter.h"

#include "base/units.h"
#include "bytes/iobuf.h"
#include "cluster/controller_stm.h"
#include "cluster/metrics_reporter.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "crash_tracker/recorder.h"
#include "crash_tracker/types.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "storage/kvstore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <fmt/core.h>

#include <exception>
#include <iterator>

namespace cluster {

static ss::logger logger("crash-reporter");

/// Key used to rate limiting metadata in the kvstore
static constexpr std::string_view rate_limiter_kvs_key
  = "rate_limiting_metadata";

/// We can upload multiple crash reports per-request in batch, but want to limit
/// the total request size to avoid large allocations or too large requests.
static constexpr auto upload_req_bound = 100_KiB;
static constexpr auto max_reports_per_request = std::max<size_t>(
  1,
  upload_req_bound / crash_tracker::crash_description::serde_size_overestimate);

crash_reporter::crash_reporter(
  storage::kvstore& kvs,
  ss::sharded<controller_stm>& stm,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<metrics_reporter>& mr)
  : _rate_limiter(kvs)
  , _controller_stm(stm)
  , _metrics_reporter(mr)
  , _as(as)
  , _client_logger{logger, "crash-reporter"} {}

ss::future<> crash_reporter::start() {
    vlog(logger.trace, "Starting Crash Reporter");
    _address = details::parse_url(
      config::shard_local_cfg().metrics_reporter_url() + "/crash_reports");

    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return report_crashes();
                      }).handle_exception([](std::exception_ptr e) {
        vlog(logger.warn, "Exception reporting crashes: {}", e);
    });

    co_return;
}

ss::future<> crash_reporter::stop() {
    vlog(logger.info, "Stopping Crash Reporter...");
    co_await _gate.close();
}

/// Called before an upload
ss::future<> crash_reporter::rate_limiter::record() {
    auto md = crash_reporter_rate_limiting_metadata{
      .last_upload_time = model::to_timestamp(clock::now())};

    iobuf buf;
    serde::write(buf, md);

    vlog(
      logger.trace,
      "Recording rate limiter last upload time as: {}",
      md.last_upload_time);
    co_await _kvstore.put(
      storage::kvstore::key_space::crash_tracker,
      bytes::from_string(rate_limiter_kvs_key),
      std::move(buf));
}

/// Called before an upload to get how long to wait before uploading
crash_reporter::rate_limiter::clock::duration
crash_reporter::rate_limiter::wait_time() {
    auto buf = _kvstore.get(
      storage::kvstore::key_space::crash_tracker,
      bytes::from_string(rate_limiter_kvs_key));
    if (!buf) {
        return 0s;
    }
    try {
        auto md = serde::from_iobuf<crash_reporter_rate_limiting_metadata>(
          std::move(buf.value()));
        auto rate_limit_time = model::to_time_point(md.last_upload_time)
                               + upload_rate;
        auto remaining = rate_limit_time - clock::now();
        return std::clamp<clock::duration>(remaining, 0s, upload_rate);
    } catch (const serde::serde_exception& e) {
        vlog(
          logger.info,
          "Error while reading crash reporter rate limiting metadata: {}",
          e);
        return upload_rate;
    }
}

ss::future<crash_reporter::crash_report_payload>
crash_reporter::build_crash_report_payload(
  const std::vector<crash_tracker::recorder::recorded_crash>& reports) {
    crash_report_payload result;

    result.cluster_uuid
      = _controller_stm.local().get_metrics_reporter_cluster_info().uuid;

    for (const auto& report : reports) {
        crash_report_payload::report r;
        r.node_id = config::node().node_id().value_or(
          model::unassigned_node_id);
        r.timestamp = report.timestamp().time_since_epoch() / 1ms;
        if (report.crash) {
            r.stacktrace = ss::sstring{report.crash.value().stacktrace.c_str()};
            r.reason = fmt::format("{}", report.crash.value().type);
            r.app_version = report.crash.value().app_version;
            r.arch = report.crash.value().arch;

            if (
              report.crash.value().type
              != crash_tracker::crash_type::startup_exception) {
                r.description = fmt::format(
                  "{}", report.crash.value().crash_message.c_str());
            }
        }
        result.items.emplace_back(std::move(r));
    }

    co_return result;
}

ss::future<> crash_reporter::report_crashes() {
    // If reporting is disabled, drop out here
    if (!config::shard_local_cfg().enable_metrics_reporter()) {
        co_return;
    }

    vlog(logger.debug, "Waiting for cluster UUID to be initialized");
    co_await _metrics_reporter.local().wait_cluster_info_initialized(
      _as.local());

    if (_as.local().abort_requested()) {
        co_return; // shortcut shutdown if requested
    }

    vlog(logger.debug, "Uploading crash reports");
    auto reports
      = co_await crash_tracker::get_recorder().get_recorded_crashes();
    auto it = reports.begin();

    while (!_as.local().abort_requested()) {
        // Collect a batch of crash reports to upload
        auto current_batch = report_batch{};
        while (current_batch.size() < max_reports_per_request
               && it != reports.end()) {
            auto& report = *(it++);
            if (!co_await report.is_uploaded()) {
                current_batch.emplace_back(std::move(report));
            }
        }

        if (current_batch.empty()) {
            vlog(logger.trace, "No crash reports to upload");
            co_return;
        }

        // Retry uploading crash reports until they succeed. Use rate limiting
        // while retrying to avoid overloading the telemetry server.
        constexpr auto max_upload_attempts = 5;
        bool success = false;
        for (size_t i = 0; i < max_upload_attempts && !success
                           && !_as.local().abort_requested();
             ++i) {
            // Record the upload to the rate limiter before sending the report
            // to ensure that if there is a crash while sending telemetry data,
            // the rate limiter will still limit subsequent uploads
            auto rl_wait = _rate_limiter.wait_time();
            if (rl_wait > 0s) {
                vlog(
                  logger.trace,
                  "Sleeping for {}ms for rate limit to pass",
                  rl_wait / 1ms);
                co_await ss::sleep_abortable(rl_wait, _as.local());
            }
            co_await _rate_limiter.record();

            success = co_await try_report_crashes(current_batch);
        }

        if (!success) {
            vlog(
              logger.info,
              "Failed to upload crash reports after {} upload attempts",
              max_upload_attempts);
            co_return;
        } else {
            for (auto& report : current_batch) {
                co_await report.mark_uploaded();
            }
        }
    }
}

// Build, serialize and upload the payload
// Returns true on success
ss::future<bool>
crash_reporter::try_report_crashes(const report_batch& current_batch) {
    auto payload = co_await build_crash_report_payload(current_batch);
    auto out = serialize_payload(payload);
    try {
        details::metrics_http_client::configs conf(
          _address, _client_logger, _as.local());
        co_await details::metrics_http_client::send_metrics(
          conf, std::move(out));
        vlog(
          logger.info,
          "Successfully uploaded {} crash reports",
          current_batch.size());
        co_return true;
    } catch (...) {
        vlog(
          logger.info,
          "Exception thrown while reporting crashes - {}",
          std::current_exception());
        co_return false;
    }
}

iobuf crash_reporter::serialize_payload(
  const crash_reporter::crash_report_payload& payload) {
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> writer(sb);

    json::rjson_serialize(writer, payload);
    iobuf out;
    out.append(sb.GetString(), sb.GetSize());

    return out;
}

} // namespace cluster

namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::crash_reporter::crash_report_payload& payload) {
    w.StartObject();

    w.Key("cluster_uuid");
    w.String(payload.cluster_uuid);

    w.Key("items");
    w.StartArray();
    for (const auto& r : payload.items) {
        rjson_serialize(w, r);
    }
    w.EndArray();

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::crash_reporter::crash_report_payload::report& report) {
    w.StartObject();
    w.Key("timestamp");
    w.Uint64(report.timestamp);
    w.Key("node_id");
    w.Int(report.node_id);
    w.Key("stacktrace");
    w.String(report.stacktrace);
    w.Key("reason");
    w.String(report.reason);
    w.Key("description");
    w.String(report.description);
    w.Key("app_version");
    w.String(report.app_version);
    w.Key("arch");
    w.String(report.arch);
    w.EndObject();
}

} // namespace json
