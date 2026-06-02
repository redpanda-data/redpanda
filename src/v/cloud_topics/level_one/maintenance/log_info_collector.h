/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// Compute the delete horizon for CT compaction: cleaned ranges whose tombstones
// were cleaned at or below the returned timestamp are eligible to have their
// tombstones removed. Returns model::timestamp::min() (nothing removable) when
// delete.retention.ms is disabled, or while the partition is mid TS->CT
// migration -- its pre-migration data still lives in tiered-storage cloud, and
// removing a tombstone could resurrect a key whose superseded value is in that
// range. Once the migration completes (its TS data ages out) the bound reverts
// to now - delete.retention.ms. This mirrors the tiered-storage rule, where
// ntp_config::delete_retention_ms() returns nullopt while archival is enabled.
model::timestamp tombstone_removal_upper_bound(
  const cluster::topic_properties& props,
  std::optional<std::chrono::milliseconds> cluster_default_retention,
  model::timestamp now,
  bool partition_migrating);

// A wrapper to provide easy mocking and break dependency on a multitude of
// cluster objects within the `log_info_collector`.
class topic_cfg_provider {
public:
    virtual ~topic_cfg_provider() noexcept = default;

    virtual std::optional<
      std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const = 0;
};

// Default topic_cfg_provider, which is the `cluster::metadata_cache`.
class topic_cfg_provider_impl : public topic_cfg_provider {
public:
    topic_cfg_provider_impl(cluster::metadata_cache*);

    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const final;

private:
    cluster::metadata_cache* _metadata_cache;
};

// Provides the maximum offset which is compactible for a given ntp.
class max_compactible_offset_provider {
public:
    virtual ~max_compactible_offset_provider() noexcept = default;

    // Fills the provided map with max compactible offsets for the given NTPs.
    // NTPs that cannot be looked up (e.g. partition not found) will not have
    // an entry added to the map.
    virtual ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const = 0;

    // Sets the value for each key NTP to whether its partition is mid TS->CT
    // migration (a migration seal is recorded). NTPs that cannot be looked up
    // are left untouched.
    virtual ss::future<>
    fill_migrating_ntps(chunked_hash_map<model::ntp, bool>&) const = 0;
};

// Default max_compactible_offset_provider, which uses the `shard_table` and
// `partition_manager` to access a partition's `lowest_pinned_data_offset()`
// through its `stm_hookset()`. Batches cross-shard calls by grouping NTPs
// by their owning shard.
class max_compactible_offset_provider_impl
  : public max_compactible_offset_provider {
public:
    max_compactible_offset_provider_impl(
      ss::sharded<cluster::shard_table>*,
      ss::sharded<cluster::partition_manager>*);

    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final;

    ss::future<>
    fill_migrating_ntps(chunked_hash_map<model::ntp, bool>&) const final;

private:
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
};

// Responsible for issuing `get_compaction_info()` requests to the `metastore`
// when attempting to schedule a round of compactions.
class log_info_collector {
public:
    log_info_collector(
      metastore*,
      std::unique_ptr<topic_cfg_provider>,
      std::unique_ptr<max_compactible_offset_provider>);

    // Populates `compaction_info_and_ts` within `log_compaction_meta`s from
    // the provided `log_list_t` by collecting each log's compaction info from
    // the metastore. It is not guaranteed that every log present in
    // `log_list_t` will have its `compaction_info_and_ts` set e.g. due to
    // concurrent removal or metastore errors. If a log already has
    // `compaction_info_and_ts` set, it will not be collected again until an
    // interval has elapsed and the current `compaction_info_and_ts` is
    // determined stale. Additionally, logs that have an inflight compaction in
    // process do not need to be collected. Logs that have their information
    // collected and deemed eligible for compaction will also have their
    // `lw_shared_ptr` copied into the `log_compaction_queue` for future
    // compaction.
    ss::future<>
    collect_info_for_logs(log_set_t&, log_list_t&, log_compaction_queue&) const;

    // Issues a batched get_leveling_infos RPC for the provided logs and
    // populates each log's leveling_info_and_ts field. Skips logs whose
    // info cannot be obtained this round.
    ss::future<>
    collect_leveling_info(chunked_vector<log_compaction_meta_ptr> logs) const;

private:
    // Returns a container of `compaction_info_spec` to sample the metastore
    // with based on the input `log_list_t`.
    chunked_vector<metastore::compaction_info_spec> get_logs_to_collect(
      log_list_t&,
      size_t,
      model::timestamp,
      const chunked_hash_map<model::ntp, bool>& migrating_ntps) const;

    // Sets compaction info state within the input logs per the
    // `compaction_info_map` collected from the metastore and pushes logs
    // eligible for compaction to the provided `log_compaction_queue`.
    void populate_log_infos(
      metastore::compaction_info_map&,
      log_set_t&,
      log_list_t&,
      log_compaction_queue&,
      const chunked_hash_map<model::ntp, kafka::offset>&,
      model::timestamp) const;

    // Owned by `app`.
    metastore* _metastore;

    std::unique_ptr<topic_cfg_provider> _topic_metadata_provider;
    std::unique_ptr<max_compactible_offset_provider>
      _max_compactible_offset_provider;
};

log_info_collector make_default_log_info_collector(
  metastore*,
  cluster::metadata_cache*,
  ss::sharded<cluster::shard_table>*,
  ss::sharded<cluster::partition_manager>*);

} // namespace cloud_topics::l1
