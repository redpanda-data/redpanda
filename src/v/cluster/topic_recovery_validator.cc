// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "topic_recovery_validator.h"

#include "cloud_storage/anomalies_detector.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "cluster/logger.h"

#include <seastar/coroutine/as_future.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

// Each partition gets a separate retry_chain_node and a logger tied to it.
// From this a common retry_chain_logger is created
partition_validator::partition_validator(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as,
  model::ntp ntp,
  model::initial_revision_id rev_id,
  const cloud_storage::remote_path_provider& path_provider,
  recovery_checks checks)
  : remote_{&remote}
  , bucket_{&bucket}
  , as_{&as}
  , ntp_{std::move(ntp)}
  , rev_id_{rev_id}
  , remote_path_provider_(path_provider)
  , op_rtc_{retry_chain_node{
      as,
      300s,
      config::shard_local_cfg().cloud_storage_initial_backoff_ms.value()}}
  , op_logger_{retry_chain_logger{
      clusterlog,
      op_rtc_,
      ssx::sformat("recovery validation of {}/{}", ntp_, rev_id_)}}
  , checks_{checks} {}

ss::future<validation_result> partition_validator::run() {
    op_logger_.debug(
      "will perform checks: mode={}, max_segment_depth={}",
      checks_.mode,
      checks_.max_segment_depth);

    switch (checks_.mode) {
        using enum model::recovery_validation_mode;
    case no_check:
        // Handle this for switch-completeness, should be already
        // handled by the caller
        return ss::make_ready_future<validation_result>(
          validation_result::passed);
    case check_manifest_existence:
        return do_validate_manifest_existence();
    case check_manifest_and_segment_metadata:
        return do_validate_manifest_metadata();
    }
}

ss::future<validation_result>
partition_validator::do_validate_manifest_existence() {
    auto dl = cloud_storage::partition_manifest_downloader(
      *bucket_, remote_path_provider_, ntp_, rev_id_, *remote_);
    auto download_res = co_await dl.manifest_exists(op_rtc_);
    if (download_res.has_error()) {
        // Abnormal failure mode: could be a configuration issue or an external
        // service issue.
        op_logger_.error(
          "manifest download error: download_result: {}, validation not ok",
          download_res.error());
        co_return validation_result::download_issue;
    }
    // Note that missing manifests is okay -- it may mean that the partition
    // didn't live long enough to upload a manifest. In that case, recovery can
    // proceed with an empty partition.
    switch (download_res.value()) {
    case cloud_storage::find_partition_manifest_outcome::no_matching_manifest:
        op_logger_.info("no manifest, validation ok");
        co_return validation_result::missing_manifest;
    case cloud_storage::find_partition_manifest_outcome::success:
        op_logger_.info("manifest found, validation ok");
        co_return validation_result::passed;
    }
}

ss::future<validation_result>
partition_validator::do_validate_manifest_metadata() {
    // allow enough quota just to check existence and self metadata check
    // and few segments existence
    // TODO ensure that future evolution of anomalies detector operate on a
    // "metadata first" behaviour
    auto detector = cloud_storage::anomalies_detector{
      *bucket_,
      ntp_,
      rev_id_,
      remote_path_provider_,
      *remote_,
      op_logger_,
      *as_,
    };

    // spin up detector
    auto anomalies_fut = co_await ss::coroutine::as_future(detector.run(
      op_rtc_,
      cloud_storage::anomalies_detector::segment_depth_t{
        static_cast<cloud_storage::anomalies_detector::segment_depth_t::type>(
          checks_.max_segment_depth)},
      std::nullopt,
      true));

    if (anomalies_fut.failed()) {
        // propagate shutdown exceptions, but treat other exceptions as hard
        // failures. likely could be serde exceptions for a corrupt file
        auto ex = anomalies_fut.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return ss::coroutine::exception(ex);
        }
        op_logger_.error(
          "manifest {} metadata check threw exception: {}, "
          "validation not ok",
          get_path(),
          ex);
        co_return validation_result::anomaly_detected;
    }

    // examine anomalies, some are real failures while other can be
    // tolerated
    auto anomalies = anomalies_fut.get();
    if (anomalies.status == cloud_storage::scrub_status::failed) {
        // download issue. could be a configuration issue or a s3 issue
        op_logger_.error(
          "manifest {} metadata check download failure, validation not ok",
          get_path());
        co_return validation_result::download_issue;
    }

    if (!anomalies.detected.has_value()) {
        // no anomalies detected, everything is ok up to segment depth
        op_logger_.info("manifest metadata check passed, validation ok");
        co_return validation_result::passed;
    }

    // this unpack is to trigger a compilation error if a new field is added
    // to cloud_storage::anomalies. If a new anomalies is added, it should
    // be also handled here
    const auto& [no_partition_manifest, missing_spillovers, missing_segments, segment_anomalies, ignore1, ignore2, ignore3, ignore4, ignore5]
      = anomalies.detected;

    if (no_partition_manifest) {
        // missing manifest anomaly can be ok: this means the partition did
        // not have time to upload data
        op_logger_.info("manifest metadata check: no manifest, validation ok");
        co_return validation_result::missing_manifest;
    }

    if (!missing_segments.empty()) {
        // missing segments could be problematic and need an operator fixing
        // the manifest before proceeding
        op_logger_.error(
          "manifest metadata check: missing segment, validation not ok");
        co_return validation_result::anomaly_detected;
    }

    if (!missing_spillovers.empty()) {
        // missing spillover manifest are not as problematic as missing
        // segments, but still require manual intervention to fix the
        // manifest for a correct operation
        op_logger_.error("manifest metadata check: missing spillover "
                         "manifests, validation not ok");
        co_return validation_result::anomaly_detected;
    }

    // see src/v/cluster/partition_probe.cc for categorization of sev-high and
    // sev-low classify anomaly_meta as failure or ok (not problematic for the
    // recovery use case) true: failure, false: passed
    constexpr static auto is_fatal_anomaly =
      [](const cloud_storage::anomaly_meta& am) {
          using enum cloud_storage::anomaly_type;
          switch (am.type) {
          case offset_gap:
              return true;
          case offset_overlap:
          case missing_delta:
          case non_monotonical_delta:
          case end_delta_smaller:
          case committed_smaller:
              return false;
          }
      };

    if (std::ranges::any_of(segment_anomalies, is_fatal_anomaly)) {
        op_logger_.error("manifest metadata check: fatal segment "
                         "anomalies, validation not ok");
        co_return validation_result::anomaly_detected;
    } else {
        op_logger_.warn("manifest metadata check: minor segment anomalies, "
                        "validation ok");
        co_return validation_result::passed;
    }
}

cloud_storage::remote_manifest_path partition_validator::get_path() {
    return cloud_storage::remote_manifest_path{
      remote_path_provider_.partition_manifest_path(ntp_, rev_id_)};
}

// wrap allocation and execution of partition_validation,
ss::future<validation_result> do_validate_recovery_partition(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as,
  model::ntp ntp,
  model::initial_revision_id rev_id,
  const cloud_storage::remote_path_provider& path_provider,
  model::recovery_validation_mode mode,
  size_t max_segment_depth) {
    auto p_validator = partition_validator{
      remote,
      bucket,
      as,
      std::move(ntp),
      rev_id,
      path_provider,
      {mode, max_segment_depth}};
    co_return co_await p_validator.run();
}

/// for each partition implicitly referenced by assignable_config, validate
/// its partition manifest. perform checks based on recovery_checks.mode
ss::future<absl::flat_hash_map<model::partition_id, validation_result>>
maybe_validate_recovery_topic(
  const custom_assignable_topic_configuration& assignable_config,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  ss::abort_source& as) {
    auto [initial_rev_id, num_partitions]
      = assignable_config.cfg.properties.remote_topic_properties.value();

    if (num_partitions <= 0) {
        vlog(
          clusterlog.error,
          "invalid num_partition {} for {}",
          num_partitions,
          assignable_config.cfg.tp_ns);
        co_return absl::flat_hash_map<model::partition_id, validation_result>{};
    }

    auto checks_mode = config::shard_local_cfg()
                         .cloud_storage_recovery_topic_validation_mode.value();
    auto checks_depth
      = config::shard_local_cfg()
          .cloud_storage_recovery_topic_validation_depth.value();

    if (checks_mode == model::recovery_validation_mode::no_check) {
        // skip validation, return an empty map. this is interpreted as "no
        // failures"
        co_return absl::flat_hash_map<model::partition_id, validation_result>{};
    }

    vlog(
      clusterlog.info,
      "Performing validation mode={} max_semgment_depth={} on topic {} with {} "
      "partitions",
      checks_mode,
      checks_depth,
      assignable_config.cfg.tp_ns,
      num_partitions);

    auto enumerate_partitions = std::views::iota(0, num_partitions)
                                | std::views::transform([](auto p) {
                                      return model::partition_id{p};
                                  });
    // container for the results of each separate validation procedure.
    // default value: passed
    auto results = [&] {
        auto init = enumerate_partitions
                    | std::views::transform([](model::partition_id id) {
                          return std::pair{id, validation_result::passed};
                      })
                    | std::views::common;
        return absl::flat_hash_map<model::partition_id, validation_result>{
          init.begin(), init.end()};
    }();

    auto [ns, topic] = assignable_config.cfg.tp_ns;

    // cap the concurrency, we could be dealing with 100+ partitions for a
    // topic
    constexpr static auto concurrency = 64;

    // start validation for each partition, collect the results and return
    // them
    const cloud_storage::remote_path_provider path_provider(
      assignable_config.cfg.properties.remote_label,
      assignable_config.cfg.properties.remote_topic_namespace_override);

    co_await ss::max_concurrent_for_each(
      enumerate_partitions, concurrency, [&](model::partition_id p) {
          return do_validate_recovery_partition(
                   remote,
                   bucket,
                   as,
                   model::ntp{ns, topic, p},
                   initial_rev_id,
                   path_provider,
                   checks_mode,
                   checks_depth)
            .then([&results, p](validation_result res) { results[p] = res; });
      });

    co_return results;
}

} // namespace cluster
