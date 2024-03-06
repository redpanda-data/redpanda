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
#include "cluster/logger.h"

#include <seastar/coroutine/as_future.hh>

namespace cluster {

class partition_validator {
public:
    // Each partition gets a separate retry_chain_node and a logger tied to it.
    // From this a common retry_chain_logger is created
    partition_validator(
      cloud_storage::remote& remote,
      cloud_storage_clients::bucket_name const& bucket,
      ss::abort_source& as,
      model::ntp ntp,
      model::initial_revision_id rev_id,
      recovery_checks checks)
      : remote_{&remote}
      , bucket_{&bucket}
      , as_{&as}
      , ntp_{std::move(ntp)}
      , rev_id_{rev_id}
      , op_rtc_{retry_chain_node{
          as,
          300s,
          config::shard_local_cfg().cloud_storage_initial_backoff_ms.value()}}
      , op_logger_{retry_chain_logger{
          clusterlog,
          op_rtc_,
          ssx::sformat("recovery validation of {}/{}", ntp_, rev_id_)}}
      , checks_{checks} {}

    /// Perform validation on the ntp as specified with checks
    ss::future<validation_result> run() {
        op_logger_.debug("will perform checks: {}", checks_);

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

        // Unhandled cases, compilation should prevent this. Ensure some
        // level of forward compatibility
        op_logger_.warn("{} not handled", checks_.mode);
        return ss::make_ready_future<validation_result>(
          validation_result::passed);
    }

private:
    // check manifest existence, handle download_result::success as Found
    // and the rest as NotFound (but warn for failed and timedout).
    // This existence check could be flattened in anomalies_detector, but
    // it's not a perfect overlap with the functionalities, so it's
    // performed externally
    ss::future<validation_result> do_validate_manifest_existence() {
        auto [download_res, _] = co_await remote_->partition_manifest_exists(
          *bucket_, ntp_, rev_id_, op_rtc_);
        // only res==success (manifest found) or res==notfound (manifest NOT
        // found) make sense. warn for timedout and failed
        if (download_res == cloud_storage::download_result::success) {
            op_logger_.info("manifest found, validation OK");
            co_return validation_result::passed;
        }

        if (download_res == cloud_storage::download_result::notfound) {
            op_logger_.info("no manifest, validation OK");
            co_return validation_result::missing_manifest;
        }

        // abnormal failure mode: could be a configuration issue or an
        // external service issue (note that the manifest path will end in
        // .bin but the value is just a hint of the HEAD request that
        // generated the abnormal result)
        op_logger_.error(
          "manifest {} download error: download_result: {}, validation NOT OK",
          get_path(),
          download_res);

        co_return validation_result::download_issue;
    }

    // delegate to anomalies_detector to run few checks on the remote partition,
    // interpret the result to produce a validation_result
    ss::future<validation_result> do_validate_manifest_metadata() {
        // allow enough quota just to check existence and self metadata check
        // and few segments existence
        // TODO ensure that future evolution of anomalies detector operate on a
        // "metadata first" behaviour
        auto detector = cloud_storage::anomalies_detector{
          *bucket_,
          ntp_,
          rev_id_,
          *remote_,
          op_logger_,
          *as_,
        };

        // spin up detector
        auto anomalies_fut = co_await ss::coroutine::as_future(detector.run(
          op_rtc_,
          cloud_storage::anomalies_detector::segment_depth_t{
            checks_.max_segment_depth}));

        if (anomalies_fut.failed()) {
            // propagate shutdown exceptions, but treat other exceptions as hard
            // failures. likely could be serde exceptions for a corrupt file
            auto ex = anomalies_fut.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                co_return ss::coroutine::exception(ex);
            }
            op_logger_.error(
              "manifest {} metadata check threw exception: {}, validation NOT "
              "OK",
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
              "manifest {} metadata check download failure, validation NOT OK",
              get_path());
            co_return validation_result::download_issue;
        }

        if (!anomalies.detected.has_value()) {
            // no anomalies detected, everything is ok up to segment depth
            op_logger_.info("manifest metadata check passed, validation OK");
            co_return validation_result::passed;
        }

        // this unpack is to trigger a compilation error if a new field is added
        // to cloud_storage::anomalies. If a new anomalies is added, it should
        // be also handled here
        auto const& [no_partition_manifest, missing_spillovers, missing_segments, segment_anomalies, ignore1, ignore2, ignore3, ignore4]
          = anomalies.detected;

        if (no_partition_manifest) {
            // missing manifest anomaly can be ok: this means the partition did
            // not have time to upload data
            op_logger_.info(
              "manifest metadata check: no manifest, validation OK");
            co_return validation_result::missing_manifest;
        }

        if (!missing_segments.empty()) {
            // missing segments could be problematic and need an operator fixing
            // the manifest before proceeding
            op_logger_.error(
              "manifest metadata check: missing segment, validation NOT OK");
            co_return validation_result::anomaly_detected;
        }

        if (!missing_spillovers.empty()) {
            // missing spillover manifest are not as problematic as missing
            // segments, but still require manual intervention to fix the
            // manifest for a correct operation
            op_logger_.error("manifest metadata check: missing spillover "
                             "manifests, validation NOT OK");
            co_return validation_result::anomaly_detected;
        }

        // classify anomaly_meta as failure or ok (not problematic for the
        // recovery use case) true: failure, false: passed
        constexpr static auto is_fatal_anomaly =
          [](cloud_storage::anomaly_meta const& am) {
              using enum cloud_storage::anomaly_type;
              switch (am.type) {
              case missing_delta:
              case non_monotonical_delta:
              case end_delta_smaller:
              case committed_smaller:
                  return true;
              case offset_gap:
              case offset_overlap:
                  return false;
              }
          };

        if (std::ranges::any_of(segment_anomalies, is_fatal_anomaly)) {
            op_logger_.error("manifest metadata check: fatal segment "
                             "anomalies, validation NOT OK");
            co_return validation_result::anomaly_detected;
        } else {
            op_logger_.warn("manifest metadata check: minor segment anomalies, "
                            "validation OK");
            co_return validation_result::passed;
        }
    }

    cloud_storage::remote_manifest_path get_path() {
        return cloud_storage::generate_partition_manifest_path(
          ntp_, rev_id_, cloud_storage::manifest_format::serde);
    }

    cloud_storage::remote* remote_;
    cloud_storage_clients::bucket_name const* bucket_;
    ss::abort_source* as_;
    model::ntp ntp_;
    model::initial_revision_id rev_id_;
    retry_chain_node op_rtc_;
    retry_chain_logger op_logger_;
    recovery_checks checks_;
};

// wrap allocation and execution of partition_validation,
ss::future<validation_result> do_validate_recovery_partition(
  cloud_storage::remote& remote,
  cloud_storage_clients::bucket_name const& bucket,
  ss::abort_source& as,
  model::ntp ntp,
  model::initial_revision_id rev_id,
  recovery_checks checks) {
    auto p_validator = partition_validator{
      remote, bucket, as, std::move(ntp), rev_id, checks};
    co_return co_await p_validator.run();
}

/// for each partition implicitly referenced by assignable_config, validate
/// its partition manifest. perform checks based on recovery_checks.mode
auto maybe_validate_recovery_topic(
  custom_assignable_topic_configuration const& assignable_config,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  ss::abort_source& as)
  -> ss::future<absl::flat_hash_map<model::partition_id, validation_result>> {
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

    // use topic cfg + cluster config to discover whick checks to perform
    auto checks = [&] {
        auto& cfg = config::shard_local_cfg();
        auto cluster_default = cluster::recovery_checks{
          .mode = cfg.cloud_storage_recovery_topic_validation_mode,
          .max_segment_depth
          = cfg.cloud_storage_recovery_topic_validation_depth};
        if (cfg.cloud_storage_recovery_topic_force_override_cfg) {
            // escape hatch
            return cluster_default;
        }
        return assignable_config.cfg.properties.recovery_checks.value_or(
          cluster_default);
    }();

    vlog(
      clusterlog.info,
      "Performing validation {} on topic {} with {} partitions",
      checks,
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

    if (checks.mode == model::recovery_validation_mode::no_check) {
        // skip validation
        co_return std::move(results);
    }

    auto [ns, topic] = assignable_config.cfg.tp_ns;

    // cap the concurrency, we could be dealing with 100+ partitions for a
    // topic
    constexpr static auto concurrency = 64;

    // start validation for each partition, collect the results and return
    // them

    co_await ss::max_concurrent_for_each(
      enumerate_partitions, concurrency, [&](model::partition_id p) {
          return do_validate_recovery_partition(
                   remote,
                   bucket,
                   as,
                   model::ntp{ns, topic, p},
                   initial_rev_id,
                   checks)
            .then([&results, p](validation_result res) { results[p] = res; });
      });

    co_return results;
}

} // namespace cluster
