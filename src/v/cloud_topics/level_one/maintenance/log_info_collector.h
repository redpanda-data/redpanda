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

private:
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
};

// Responsible for issuing info requests to the `metastore` when attempting to
// schedule rounds of compaction and leveling.
class log_info_collector {
public:
    log_info_collector(
      metastore*,
      std::unique_ptr<topic_cfg_provider>,
      std::unique_ptr<max_compactible_offset_provider>);

    /// Collects compaction info from the metastore for compacted logs and
    /// pushes eligible logs to the compaction queue.
    ss::future<> collect_compaction_info(
      log_set_t&, log_list_t&, log_compaction_queue&) const;

    /// Collects leveling info from the metastore for non-compacted logs and
    /// pushes eligible logs to the leveling queue.
    ss::future<>
    collect_leveling_info(log_set_t&, log_list_t&, log_leveling_queue&) const;

private:
    // Returns compaction specs for compacted logs in the input `log_list_t`.
    chunked_vector<metastore::compaction_info_spec>
    get_compaction_specs(log_list_t&, size_t, model::timestamp) const;

    // Returns leveling specs for non-compacted logs in the input `log_list_t`.
    chunked_vector<metastore::leveling_info_spec>
    get_leveling_specs(log_list_t&, size_t, model::timestamp) const;

    // Sets compaction info state within the input logs and pushes logs
    // eligible for compaction to the provided queue.
    void populate_compaction_infos(
      metastore::compaction_info_map&,
      log_set_t&,
      log_list_t&,
      log_compaction_queue&,
      const chunked_hash_map<model::ntp, kafka::offset>&,
      model::timestamp) const;

    // Sets leveling info state within the input logs and pushes logs
    // eligible for leveling to the provided queue.
    void populate_leveling_infos(
      metastore::leveling_info_map&,
      log_set_t&,
      log_list_t&,
      log_leveling_queue&,
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
