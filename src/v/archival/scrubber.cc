/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/scrubber.h"

#include "archival/logger.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "hashing/xx.h"
#include "vlog.h"

namespace {
static constexpr std::string_view serde_extension = ".bin";
static constexpr std::string_view json_extension = ".json";

static constexpr auto partition_purge_timeout = 20s;
} // namespace

namespace archival {

using cloud_storage::download_result;
using cloud_storage::upload_result;

scrubber::scrubber(
  cloud_storage::remote& r,
  cluster::topic_table& tt,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::members_table>& mt)
  : _api(r)
  , _topic_table(tt)
  , _topics_frontend(tf)
  , _members_table(mt) {}

ss::future<scrubber::purge_result> scrubber::purge_partition(
  const cluster::nt_lifecycle_marker& lifecycle_marker,
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  retry_chain_node& parent_rtc) {
    retry_chain_node partition_purge_rtc(
      partition_purge_timeout, 100ms, &parent_rtc);
    retry_chain_logger ctxlog(archival_log, partition_purge_rtc);

    if (lifecycle_marker.config.is_read_replica()) {
        // Paranoia check: should never happen.
        // It never makes sense to have written a lifecycle marker that
        // has read replica flag set, something isn't right here, do not
        // delete.
        vlog(
          ctxlog.error,
          "Read replica mode set in tombstone on {}, refusing to purge",
          ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    if (!lifecycle_marker.config.properties.remote_delete) {
        // Paranoia check: should never happen.
        // We should not have been called for a topic with remote delete
        // disabled, but double-check just in case.
        vlog(
          ctxlog.error,
          "Remote delete disabled in tombstone on {}, refusing to purge",
          ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    auto collected = co_await collect_manifest_paths(
      bucket, ntp, remote_revision, partition_purge_rtc);
    if (!collected) {
        co_return purge_result{
          .status = purge_status::retryable_failure, .ops = 0};
    } else if (collected->empty()) {
        vlog(ctxlog.debug, "Nothing to purge for {}", ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    const auto [manifests_to_purge, legacy_manifest_path]
      = collected->flatten();

    size_t ops_performed = 0;
    size_t permanent_failure = 0;
    for (auto rit = manifests_to_purge.rbegin();
         rit != manifests_to_purge.rend();
         ++rit) {
        auto format = cloud_storage::manifest_format::serde;
        if (std::string_view{*rit}.ends_with(json_extension)) {
            format = cloud_storage::manifest_format::json;
        }

        const auto local_res = co_await purge_manifest(
          bucket,
          ntp,
          remote_revision,
          remote_manifest_path{*rit},
          format,
          partition_purge_rtc);

        ops_performed += local_res.ops;

        switch (local_res.status) {
        case purge_status::retryable_failure:
            // Drop out when encountering a retry-able failure. These are often
            // back-offs from cloud storage, so it's wise to terminate the scrub
            // and retry later.
            vlog(
              ctxlog.info,
              "Retryable failures encountered while purging partition {}. Will "
              "retry ...",
              ntp);

            co_return purge_result{
              .status = purge_status::retryable_failure, .ops = ops_performed};
            break;
        case purge_status::permanent_failure:
            // Keep going when encountering a permanent failure. We might still
            // be able to purge subsequent manifests.
            ++permanent_failure;
            break;
        case purge_status::success:
            break;
        }
    }

    if (legacy_manifest_path) {
        vlog(
          ctxlog.debug,
          "Erasing legacy partition manifest {}",
          *legacy_manifest_path);
        const auto manifest_delete_result = co_await _api.delete_object(
          bucket,
          cloud_storage_clients::object_key(*legacy_manifest_path),
          partition_purge_rtc);
        if (manifest_delete_result != upload_result::success) {
            vlog(
              ctxlog.info,
              "Retryable failures encountered while purging partition legacy "
              "manifest at {}. Will "
              "retry ...",
              legacy_manifest_path.value());

            co_return purge_result{
              .status = purge_status::retryable_failure, .ops = ops_performed};
        }
    }

    if (permanent_failure > 0) {
        vlog(
          ctxlog.error,
          "Permanent failures encountered while purging partition {}. Giving "
          "up ...",
          ntp);

        co_return purge_result{
          .status = purge_status::permanent_failure, .ops = ops_performed};
    } else {
        vlog(
          ctxlog.info,
          "Finished erasing partition {} from object storage in {} requests",
          ntp,
          ops_performed);

        co_return purge_result{
          .status = purge_status::success, .ops = ops_performed};
    }
}

ss::future<std::optional<scrubber::collected_manifests>>
scrubber::collect_manifest_paths(
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  retry_chain_node& parent_rtc) {
    retry_chain_node collection_rtc(&parent_rtc);
    retry_chain_logger ctxlog(archival_log, collection_rtc);

    cloud_storage::partition_manifest manifest(ntp, remote_revision);
    auto path = manifest.get_manifest_path(
      cloud_storage::manifest_format::serde);

    std::string_view base_path{path().native()};

    vassert(
      base_path.ends_with(serde_extension)
        && base_path.length() > serde_extension.length(),
      "Generated manifest path should end in .bin");

    base_path.remove_suffix(serde_extension.length());
    auto list_result = co_await _api.list_objects(
      bucket,
      collection_rtc,
      cloud_storage_clients::object_key{std::filesystem::path{base_path}});

    if (list_result.has_error()) {
        vlog(
          ctxlog.warn,
          "Failed to collect manifests to scrub partition {}",
          ntp);

        co_return std::nullopt;
    }

    collected_manifests collected{};
    collected.spillover.reserve(list_result.value().contents.size());
    for (auto& item : list_result.value().contents) {
        std::string_view path{item.key};
        if (path.ends_with(".bin")) {
            collected.current_serde = std::move(item.key);
            continue;
        }

        if (path.ends_with(".json")) {
            collected.current_json = std::move(item.key);
            continue;
        }

        collected.spillover.push_back(std::move(item.key));
    }

    co_return collected;
}

ss::future<scrubber::purge_result> scrubber::purge_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  remote_manifest_path manifest_key,
  cloud_storage::manifest_format format,
  retry_chain_node& parent_rtc) {
    retry_chain_node manifest_purge_rtc(&parent_rtc);
    retry_chain_logger ctxlog(archival_log, manifest_purge_rtc);

    purge_result result{.status = purge_status::success, .ops = 0};

    vlog(
      ctxlog.info,
      "Purging manifest at {} and its contents for partition {}",
      manifest_key(),
      ntp);

    cloud_storage::partition_manifest manifest(ntp, remote_revision);
    auto manifest_get_result = co_await _api.download_manifest(
      bucket, {format, manifest_key}, manifest, manifest_purge_rtc);

    if (manifest_get_result == download_result::notfound) {
        vlog(ctxlog.debug, "Partition manifest get {} not found", manifest_key);
        result.status = purge_status::permanent_failure;
        co_return result;
    } else if (manifest_get_result != download_result::success) {
        vlog(
          ctxlog.debug,
          "Partition manifest get {} failed: {}",
          manifest_key(),
          manifest_get_result);
        result.status = purge_status::retryable_failure;
        co_return result;
    }

    // A rough guess at how many ops will be involved in deletion, so that
    // we don't have to plumb ops-counting all the way down into object store
    // clients' implementations of plural object delete (different
    // implementations may do bigger/smaller batches).  This 1000 number
    // reflects the number of objects per S3 DeleteObjects.
    const auto estimate_delete_ops = std::max(
      static_cast<size_t>(manifest.size() / 1000), size_t{1});

    const auto erase_result = co_await cloud_storage::remote_partition::erase(
      _api, bucket, std::move(manifest), manifest_key, manifest_purge_rtc);

    result.ops += estimate_delete_ops;

    if (erase_result != cloud_storage::remote_partition::erase_result::erased) {
        vlog(
          ctxlog.debug,
          "One or more objects deletions failed for manifest {} of {}",
          manifest_key(),
          ntp);
        result.status = purge_status::retryable_failure;
        co_return result;
    }

    co_return result;
}

scrubber::global_position scrubber::get_global_position() {
    const auto& nodes = _members_table.local().nodes();
    auto self = config::node().node_id();

    // members_table doesn't store nodes in sorted container, so
    // we compose a sorted order first.
    auto node_ids = _members_table.local().node_ids();
    std::sort(node_ids.begin(), node_ids.end());

    uint32_t result = 0;
    uint32_t total = 0;

    // Iterate over node IDs earlier than ours, sum their core counts
    for (auto i : node_ids) {
        const auto cores = nodes.at(i).broker.properties().cores;
        if (i < self) {
            result += cores;
        }
        total += cores;
    }

    result += ss::this_shard_id();

    return global_position{.self = result, .total = total};
}

ss::future<housekeeping_job::run_result>
scrubber::run(retry_chain_node& parent_rtc, run_quota_t quota) {
    auto gate_holder = _gate.hold();

    run_result result{
      .status = run_status::skipped,
      .consumed = run_quota_t(0),
      .remaining = quota,
    };

    if (!_enabled) {
        co_return result;
    }

    // Take a copy, as we will iterate over it asynchronously
    cluster::topic_table::lifecycle_markers_t markers
      = _topic_table.get_lifecycle_markers();

    const auto my_global_position = get_global_position();

    vlog(
      archival_log.info,
      "Running with {} quota, {} topic lifecycle markers",
      result.remaining,
      markers.size());
    for (auto& [nt_revision, marker] : markers) {
        // Double check the topic config is elegible for remote deletion
        if (!marker.config.properties.requires_remote_erase()) {
            vlog(
              archival_log.warn,
              "Dropping lifecycle marker {}, is not suitable for remote purge",
              marker.config.tp_ns);

            co_await _topics_frontend.local().purged_topic(nt_revision, 5s);
            continue;
        }

        // Check if the grace period has elapsed. The intent here is to
        // avoid races with `remote_partition::finalize`.
        const auto now = ss::lowres_system_clock::now();
        if (
          marker.timestamp.has_value()
          && now - marker.timestamp.value()
               < config::shard_local_cfg()
                   .cloud_storage_topic_purge_grace_period_ms()) {
            vlog(
              archival_log.debug,
              "Grace period for {} is still in effect for. Skipping scrub.",
              marker.config.tp_ns);

            continue;
        }

        auto& bucket_config_property
          = cloud_storage::configuration::get_bucket_config();
        if (!bucket_config_property().has_value()) {
            vlog(
              archival_log.error,
              "Lifecycle marker exists but cannot be purged because "
              "{} is not set.",
              bucket_config_property.name());
            co_return result;
        }

        auto bucket = cloud_storage_clients::bucket_name{
          bucket_config_property().value()};

        // TODO: share work at partition granularity, not topic.  Requires
        // a feedback mechanism for the work done on partitions to be made
        // visible to the shard handling the total topic.

        // Map topics to shards based on simple hash, to distribute work
        // if there are many topics to clean up.
        incremental_xxhash64 inc_hash;
        inc_hash.update(nt_revision.nt.ns);
        inc_hash.update(nt_revision.nt.tp);
        inc_hash.update(nt_revision.initial_revision_id);
        uint32_t hash = static_cast<uint32_t>(inc_hash.digest() & 0xffffffff);

        if (my_global_position.self == hash % my_global_position.total) {
            vlog(
              archival_log.info,
              "Processing topic lifecycle marker {} ({} partitions)",
              marker.config.tp_ns,
              marker.config.partition_count);
            auto& topic_config = marker.config;

            // Persist a record that we have started purging: this is useful for
            // anything reading from the bucket that wants to distinguish
            // corruption from in-progress deletion.
            auto marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              cloud_storage::lifecycle_status::purging,
              parent_rtc);
            if (marker_r != cloud_storage::upload_result::success) {
                vlog(
                  archival_log.warn,
                  "Failed to write lifecycle marker, not purging {}",
                  nt_revision);
                result.status = run_status::failed;
                co_return result;
            }

            for (model::partition_id i = model::partition_id{0};
                 i < marker.config.partition_count;
                 ++i) {
                model::ntp ntp(nt_revision.nt.ns, nt_revision.nt.tp, i);

                if (result.remaining <= run_quota_t(0)) {
                    // Exhausted quota, drop out.
                    vlog(archival_log.debug, "Exhausted quota, dropping out");
                    co_return result;
                }

                auto purge_r = co_await purge_partition(
                  marker, bucket, ntp, marker.initial_revision_id, parent_rtc);

                result.consumed += run_quota_t(purge_r.ops);
                result.remaining
                  = result.remaining
                    - std::min(run_quota_t(purge_r.ops), result.remaining);

                if (purge_r.status == purge_status::success) {
                    result.status = run_status::ok;
                } else if (purge_r.status == purge_status::permanent_failure) {
                    // If we permanently fail to purge a partition, we pretend
                    // to succeed and proceed to clean up the tombstone, to
                    // avoid remote storage issues blocking us from cleaning
                    // up tombstones
                    result.status = run_status::ok;
                } else {
                    vlog(
                      archival_log.info,
                      "Failed to purge {}, will retry on next scrub",
                      ntp);
                    result.status = run_status::failed;
                    co_return result;
                }
            }

            // At this point, all partition deletions either succeeded or
            // permanently failed: clean up the topic manifest and erase
            // the controller tombstone.
            auto topic_manifest_path
              = cloud_storage::topic_manifest::get_topic_manifest_path(
                topic_config.tp_ns.ns, topic_config.tp_ns.tp);
            vlog(
              archival_log.debug,
              "Erasing topic manifest {}",
              topic_manifest_path);
            retry_chain_node topic_manifest_rtc(5s, 1s, &parent_rtc);
            auto manifest_delete_result = co_await _api.delete_object(
              bucket,
              cloud_storage_clients::object_key(topic_manifest_path),
              topic_manifest_rtc);
            if (manifest_delete_result != upload_result::success) {
                vlog(
                  archival_log.info,
                  "Failed to erase topic manifest {}, will retry on next scrub",
                  nt_revision.nt);
                result.status = run_status::failed;
                co_return result;
            }

            // Before purging the topic from the controller, write a permanent
            // lifecycle marker to object storage, enabling readers to
            // unambiguously understand that this topic is gone due to an
            // intentional deletion, and that any stray objects belonging to
            // this topic may be purged.
            marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              cloud_storage::lifecycle_status::purged,
              parent_rtc);
            if (marker_r != cloud_storage::upload_result::success) {
                vlog(
                  archival_log.warn,
                  "Failed to write lifecycle marker, not purging {}",
                  nt_revision);
                result.status = run_status::failed;
                co_return result;
            }

            // All topic-specific bucket contents are gone, we may erase
            // our controller tombstone.
            auto purge_result = co_await _topics_frontend.local().purged_topic(
              nt_revision, 5s);
            if (purge_result.ec != cluster::errc::success) {
                auto errc = cluster::make_error_code(purge_result.ec);
                // Just log: this will get retried next time the scrubber runs
                vlog(
                  archival_log.info,
                  "Failed to mark topic {} purged: {}, will retry on next "
                  "scrub",
                  nt_revision.nt,
                  errc.message());
            } else {
                vlog(archival_log.info, "Topic {} purge complete", nt_revision);
            }
        }
    }

    co_return result;
}

ss::future<cloud_storage::upload_result>
scrubber::write_remote_lifecycle_marker(
  const cluster::nt_revision& nt_revision,
  cloud_storage_clients::bucket_name& bucket,
  cloud_storage::lifecycle_status status,
  retry_chain_node& parent_rtc) {
    retry_chain_node marker_rtc(5s, 1s, &parent_rtc);
    cloud_storage::remote_nt_lifecycle_marker remote_marker{
      .cluster_id = config::shard_local_cfg().cluster_id().value_or(""),
      .topic = nt_revision,
      .status = status,
    };
    auto marker_key = remote_marker.get_key();

    co_return co_await _api.upload_object(
      bucket,
      marker_key,
      serde::to_iobuf(std::move(remote_marker)),
      marker_rtc,
      "remote_lifecycle_marker");
}

ss::future<> scrubber::stop() {
    vlog(archival_log.info, "Stopping ({})...", _gate.get_count());
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    return _gate.close();
}

void scrubber::interrupt() { _as.request_abort(); }

bool scrubber::interrupted() const { return _as.abort_requested(); }

void scrubber::set_enabled(bool e) { _enabled = e; }

void scrubber::acquire() { _holder = ss::gate::holder(_gate); }

void scrubber::release() { _holder.release(); }

ss::sstring scrubber::name() const { return "scrubber"; }

bool scrubber::collected_manifests::empty() const {
    return !current_serde.has_value() && !current_json.has_value()
           && spillover.empty();
}

scrubber::collected_manifests::flat_t scrubber::collected_manifests::flatten() {
    std::optional<ss::sstring> legacy_manifest_path;
    auto manifests_to_purge = std::move(spillover);

    if (current_serde.has_value()) {
        manifests_to_purge.push_back(std::move(current_serde.value()));
        legacy_manifest_path = std::move(current_json);
    } else if (current_json.has_value()) {
        manifests_to_purge.push_back(std::move(current_json.value()));
    }

    return {std::move(manifests_to_purge), std::move(legacy_manifest_path)};
}

} // namespace archival
