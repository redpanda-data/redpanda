/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/scrubber.h"

#include "cluster/archival/logger.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/errc.h"

namespace archival {

scrubber::scrubber(
  ntp_archiver& archiver,
  cloud_storage::remote& remote,
  features::feature_table& feature_table,
  config::binding<bool> config_enabled,
  config::binding<std::chrono::milliseconds> partial_interval,
  config::binding<std::chrono::milliseconds> full_interval,
  config::binding<std::chrono::milliseconds> jitter)
  : _root_rtc(_as)
  , _logger(archival_log, _root_rtc, archiver.get_ntp().path())
  , _config_enabled(std::move(config_enabled))
  , _archiver(archiver)
  , _remote(remote)
  , _feature_table(feature_table)
  , _detector{_archiver.get_bucket_name(), _archiver.get_ntp(), _archiver.get_revision_id(), _archiver.remote_path_provider(),  _remote, _logger, _as}
  , _scheduler(
      [this] {
          const auto at = _archiver.manifest().last_partition_scrub();
          const auto offset = _archiver.manifest().last_scrubbed_offset();
          cloud_storage::scrub_status status;
          if (!offset && at != model::timestamp::missing()) {
              status = cloud_storage::scrub_status::full;
          } else {
              status = cloud_storage::scrub_status::partial;
          }

          return std::make_tuple(at, status);
      },
      std::move(partial_interval),
      std::move(full_interval),
      std::move(jitter)) {
    ssx::spawn_with_gate(_gate, [this] { return await_feature_enabled(); });
}

ss::future<> scrubber::await_feature_enabled() {
    try {
        co_await _feature_table.await_feature(
          features::feature::cloud_storage_scrubbing, _as);
    } catch (const ss::abort_requested_exception&) {
        vlog(
          _logger.warn,
          "Scrubber abort request while awaiting feature activation");
        co_return;
    } catch (...) {
        vlog(
          _logger.error,
          "Unexpected exception while awaiting feature activation: {}",
          std::current_exception());
        co_return;
    }
    _scheduler.pick_next_scrub_time();
}

ss::future<scrubber::run_result> scrubber::run(run_quota_t quota) {
    ss::gate::holder holder{_gate};

    if (auto [skip, reason] = should_skip(); skip) {
        vlog(_logger.debug, "Skipping cloud partition scrub: {}", *reason);
        co_return run_result{
          .status = run_status::skipped,
          .consumed = run_quota_t{0},
          .remaining = quota};
    }

    const auto scrub_from = _archiver.manifest().last_scrubbed_offset();
    vlog(
      _logger.info,
      "Starting scrub with {} quota from offset {}",
      quota(),
      scrub_from);

    retry_chain_node anomaly_detection_rtc(5min, 100ms, &_root_rtc);
    auto detect_result = co_await _detector.run(
      anomaly_detection_rtc, quota, scrub_from);

    // The quota accounting below compensates for the fact that
    // `run_quota_t` is signed, but `result::ops` is unsigned. Avoid
    // overflow when computing `consumed` and underflow when computing
    // `remaining`.
    run_quota_t consumed = [&detect_result]() {
        if (detect_result.ops > std::numeric_limits<run_quota_t::type>::max()) {
            return run_quota_t{std::numeric_limits<run_quota_t::type>::max()};
        }

        return run_quota_t{static_cast<run_quota_t::type>(detect_result.ops)};
    }();

    run_quota_t remaining = [&quota, &consumed]() {
        if (consumed >= quota) {
            return run_quota_t{0};
        }

        return quota - consumed;
    }();

    if (detect_result.status == cloud_storage::scrub_status::failed) {
        vlog(
          _logger.info,
          "Scrub failed after {} operations. Will retry ...",
          detect_result.ops);
        co_return run_result{
          .status = run_status::failed,
          .consumed = consumed,
          .remaining = remaining};
    }

    if (_as.abort_requested()) {
        co_return run_result{
          .status = run_status::failed,
          .consumed = consumed,
          .remaining = remaining};
    }

    vlog(
      _logger.info,
      "Scrub which started at {} finished at {} with status {} and detected {} "
      "and used {} quota",
      scrub_from,
      detect_result.last_scrubbed_offset,
      detect_result.status,
      detect_result.detected,
      detect_result.ops);

    auto replicate_result = co_await _archiver.process_anomalies(
      model::timestamp::now(),
      detect_result.last_scrubbed_offset,
      detect_result.status,
      std::move(detect_result.detected));

    _scheduler.pick_next_scrub_time();

    co_return run_result{
      .status = replicate_result == cluster::errc::success ? run_status::ok
                                                           : run_status::failed,
      .consumed = consumed,
      .remaining = remaining};
}

void scrubber::interrupt() { _as.request_abort(); }

bool scrubber::interrupted() const { return _as.abort_requested(); }

void scrubber::set_enabled(bool e) { _job_enabled = e; }

void scrubber::acquire() {
    vassert(
      !_holder.has_value(), "scrubber::acquire called on an active instance");
    _holder = ss::gate::holder(_gate);
}

void scrubber::release() {
    vassert(
      _holder.has_value(), "scrubber::release called before scrubber::acquire");
    _holder->release();
}

ss::future<> scrubber::stop() {
    vlog(_logger.info, "Stopping scrubber ({})...", _gate.get_count());
    _as.request_abort();
    return _gate.close();
}

retry_chain_node* scrubber::get_root_retry_chain_node() { return &_root_rtc; }

ss::sstring scrubber::name() const { return "scrubber"; }

std::pair<bool, std::optional<ss::sstring>> scrubber::should_skip() const {
    if (!_feature_table.is_active(features::feature::cloud_storage_scrubbing)) {
        return {true, "cloud_storage_scrubbing feature not active"};
    }

    if (!_job_enabled) {
        return {true, "scrubber housekeeping job disabled"};
    }

    if (!_config_enabled()) {
        return {true, "scrubber disabled via cluster config"};
    }

    const bool not_yet = !_scheduler.should_scrub();
    if (not_yet) {
        const auto until_next = _scheduler.until_next_scrub();
        if (!until_next.has_value()) {
            return {true, "next scrub not scheduled"};
        }

        return {
          true,
          ssx::sformat(
            "next scrub in {}",
            std::chrono::duration_cast<std::chrono::minutes>(*until_next))};
    }

    return {false, std::nullopt};
}

void scrubber::reset_scheduler() { _scheduler.pick_next_scrub_time(); }

} // namespace archival
