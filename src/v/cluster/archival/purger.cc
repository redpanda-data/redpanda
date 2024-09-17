/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/purger.h"

#include "base/vlog.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage_clients/types.h"
#include "cluster/archival/logger.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "hashing/xx.h"

namespace {
static constexpr std::string_view json_extension = ".json";

static constexpr auto partition_purge_timeout = 20s;
} // namespace

namespace archival {

using cloud_storage::download_result;
using cloud_storage::upload_result;

purger::purger(
  cloud_storage::remote& r,
  cluster::topic_table& tt,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::members_table>& mt)
  : _root_rtc(_as)
  , _api(r)
  , _topic_table(tt)
  , _topics_frontend(tf)
  , _members_table(mt) {}

ss::future<purger::purge_result> purger::purge_partition(
  const cluster::nt_lifecycle_marker& lifecycle_marker,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage::remote_path_provider& path_provider,
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
      bucket, path_provider, ntp, remote_revision, partition_purge_rtc);
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
          path_provider,
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

ss::future<std::optional<purger::collected_manifests>>
purger::collect_manifest_paths(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage::remote_path_provider& path_provider,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  retry_chain_node& parent_rtc) {
    retry_chain_node collection_rtc(&parent_rtc);
    retry_chain_logger ctxlog(archival_log, collection_rtc);

    auto base_path = path_provider.partition_manifest_prefix(
      ntp, remote_revision);

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

        // The spillover manifest path is of the form
        // "{prefix}/{manifest.bin().x.x.x.x.x.x}" Find the index of the last
        // '/' in the path, so we can check just the filename (starting from the
        // first character after '/').
        const size_t filename_idx = path.rfind('/');
        if (filename_idx == std::string_view::npos) {
            continue;
        }

        // File should start with "manifest.bin()", but it should have
        // additional spillover components as well.
        std::string_view file = path.substr(filename_idx + 1);
        if (
          file.starts_with(cloud_storage::partition_manifest::filename())
          && !file.ends_with(cloud_storage::partition_manifest::filename())) {
            collected.spillover.push_back(std::move(item.key));
        }
    }

    co_return collected;
}

ss::future<purger::purge_result> purger::purge_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage::remote_path_provider& path_provider,
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
      _api,
      bucket,
      path_provider,
      std::move(manifest),
      manifest_key,
      manifest_purge_rtc);

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

purger::global_position purger::get_global_position() {
    const auto& nodes = _members_table.local().nodes();
    auto self = config::node().node_id();

    uint32_t result = 0;
    uint32_t total = 0;

    // Iterate over node IDs earlier than ours, sum their core counts
    for (const auto& [id, node] : nodes) {
        const auto cores = node.broker.properties().cores;
        if (id < self) {
            result += cores;
        }
        total += cores;
    }

    result += ss::this_shard_id();

    return global_position{.self = result, .total = total};
}

ss::future<housekeeping_job::run_result> purger::run(run_quota_t quota) {
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

        cloud_storage::remote_path_provider path_provider(
          marker.config.properties.remote_label,
          marker.config.properties.remote_topic_namespace_override);
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
            retry_chain_node pre_purge_marker_rtc(5s, 1s, &_root_rtc);
            auto marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              path_provider,
              cloud_storage::lifecycle_status::purging,
              pre_purge_marker_rtc);
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
                  marker,
                  bucket,
                  path_provider,
                  ntp,
                  marker.initial_revision_id,
                  _root_rtc);

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
            const auto& tp_ns = topic_config.tp_ns;
            auto topic_manifest_path_serde = cloud_storage_clients::object_key{
              path_provider.topic_manifest_path(
                tp_ns, marker.initial_revision_id)};
            vlog(
              archival_log.debug,
              "Erasing topic manifest {}",
              topic_manifest_path_serde);

            retry_chain_node topic_manifest_rtc(5s, 1s, &_root_rtc);
            ss::future<upload_result> delete_result = _api.delete_object(
              bucket, topic_manifest_path_serde, topic_manifest_rtc);

            auto topic_manifest_path_json_opt
              = path_provider.topic_manifest_path_json(tp_ns);
            ss::future<upload_result> delete_result_json
              = ss::make_ready_future<upload_result>(upload_result::success);
            cloud_storage_clients::object_key topic_manifest_path_json{};
            if (topic_manifest_path_json_opt.has_value()) {
                topic_manifest_path_json = cloud_storage_clients::object_key{
                  *topic_manifest_path_json_opt};
                delete_result_json = _api.delete_object(
                  bucket, topic_manifest_path_json, topic_manifest_rtc);
            }
            auto [manifest_delete_result_serde, manifest_delete_result_json]
              = co_await ss::when_all_succeed(
                std::move(delete_result), std::move(delete_result_json));
            if (
              manifest_delete_result_serde != upload_result::success
              || manifest_delete_result_json != upload_result::success) {
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
            retry_chain_node post_purge_marker_rtc(5s, 1s, &_root_rtc);
            marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              path_provider,
              cloud_storage::lifecycle_status::purged,
              post_purge_marker_rtc);
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

ss::future<cloud_storage::upload_result> purger::write_remote_lifecycle_marker(
  const cluster::nt_revision& nt_revision,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage::remote_path_provider& path_provider,
  cloud_storage::lifecycle_status status,
  retry_chain_node& parent_rtc) {
    retry_chain_node marker_rtc(5s, 1s, &parent_rtc);
    cloud_storage::remote_nt_lifecycle_marker remote_marker{
      .cluster_id = config::shard_local_cfg().cluster_id().value_or(""),
      .topic = nt_revision,
      .status = status,
    };
    auto marker_key = remote_marker.get_key(path_provider);
    co_return co_await _api.upload_object({
      .transfer_details
      = {.bucket = bucket, .key = marker_key, .parent_rtc = marker_rtc},
      .type = cloud_storage::upload_type::remote_lifecycle_marker,
      .payload = serde::to_iobuf(std::move(remote_marker)),
    });
}

ss::future<> purger::stop() {
    vlog(archival_log.info, "Stopping purger ({})...", _gate.get_count());
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    return _gate.close();
}

void purger::interrupt() { _as.request_abort(); }

bool purger::interrupted() const { return _as.abort_requested(); }

void purger::set_enabled(bool e) { _enabled = e; }

void purger::acquire() { _holder = ss::gate::holder(_gate); }

void purger::release() { _holder.release(); }

retry_chain_node* purger::get_root_retry_chain_node() { return &_root_rtc; }

ss::sstring purger::name() const { return "purger"; }

bool purger::collected_manifests::empty() const {
    return !current_serde.has_value() && !current_json.has_value()
           && spillover.empty();
}

purger::collected_manifests::flat_t purger::collected_manifests::flatten() {
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
