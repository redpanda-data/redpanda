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

#include "base/seastarx.h"
#include "cloud_storage/remote_label.h"
#include "cloud_topics/level_one/metastore/leader_router_probe.h"
#include "cloud_topics/level_one/metastore/rpc_service.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

template<typename T>
concept request_has_metastore_partition = requires(T t) {
    { t.metastore_partition } -> std::same_as<model::partition_id&>;
};

template<typename T>
concept request_has_topic_id_partition = requires(T t) {
    { t.tp } -> std::same_as<model::topic_id_partition&>;
};

namespace cloud_topics::l1 {
class domain_supervisor;

/*
 * Frontend is the gateway into a partition of a partitioned metastore on a
 * given shard.
 *
 * One leader_router instance per shard.
 */
class leader_router : public ss::peering_sharded_service<leader_router> {
public:
    using local_only = ss::bool_class<struct local_only>;

    leader_router(
      model::node_id self,
      ss::sharded<cluster::metadata_cache>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
      ss::sharded<::rpc::connection_cache>*,
      ss::sharded<domain_supervisor>*);

    ss::future<> stop();

    ss::future<rpc::add_objects_reply>
      add_objects(rpc::add_objects_request, local_only = local_only::no);

    ss::future<rpc::replace_objects_reply> replace_objects(
      rpc::replace_objects_request, local_only = local_only::no);

    ss::future<rpc::get_first_offset_ge_reply> get_first_offset_ge(
      rpc::get_first_offset_ge_request, local_only = local_only::no);

    ss::future<rpc::get_first_timestamp_ge_reply> get_first_timestamp_ge(
      rpc::get_first_timestamp_ge_request, local_only = local_only::no);

    ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(
        rpc::get_first_offset_for_bytes_request, local_only = local_only::no);

    ss::future<rpc::get_offsets_reply>
      get_offsets(rpc::get_offsets_request, local_only = local_only::no);

    ss::future<rpc::get_size_reply>
      get_size(rpc::get_size_request, local_only = local_only::no);

    ss::future<rpc::get_compaction_info_reply> get_compaction_info(
      rpc::get_compaction_info_request, local_only = local_only::no);

    ss::future<rpc::get_term_for_offset_reply> get_term_for_offset(
      rpc::get_term_for_offset_request, local_only = local_only::no);

    ss::future<rpc::get_end_offset_for_term_reply> get_end_offset_for_term(
      rpc::get_end_offset_for_term_request, local_only = local_only::no);

    ss::future<rpc::set_start_offset_reply> set_start_offset(
      rpc::set_start_offset_request, local_only = local_only::no);

    ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request, local_only = local_only::no);

    ss::future<rpc::get_compaction_infos_reply> get_compaction_infos(
      rpc::get_compaction_infos_request, local_only = local_only::no);

    ss::future<rpc::get_extent_metadata_reply> get_extent_metadata(
      rpc::get_extent_metadata_request, local_only = local_only::no);

    ss::future<rpc::flush_domain_reply>
      flush_domain(rpc::flush_domain_request, local_only = local_only::no);

    ss::future<rpc::restore_domain_reply>
      restore_domain(rpc::restore_domain_request, local_only = local_only::no);

    ss::future<rpc::preregister_objects_reply> preregister_objects(
      rpc::preregister_objects_request, local_only = local_only::no);

    std::optional<model::partition_id>
    metastore_partition(const model::topic_id_partition&) const;

    std::optional<int> num_metastore_partitions() const;

    // Creates the topic (optionally with the given number of partitions, if
    // provided). Does not validate that the actual number of partitions in
    // the topic matches if it already exists.
    ss::future<bool>
    ensure_topic_exists(std::optional<int> num_partitions = std::nullopt);

    // Indicates the location in object storage that this metastore's manifest
    // will be stored.
    std::optional<cloud_storage::remote_label> metastore_restore_label() const;

private:
    using proto_t = cloud_topics::l1::rpc::impl::l1_rpc_client_protocol;
    using client = cloud_topics::l1::rpc::impl::l1_rpc_client_protocol;

    static constexpr std::chrono::seconds rpc_timeout{5};

    // utilities for boiler plate RPC code.

    template<auto Func, typename req_t>
    requires requires(proto_t f, req_t req, ::rpc::client_opts opts) {
        (f.*Func)(std::move(req), std::move(opts));
    }
    ss::future<typename req_t::resp_t>
    remote_dispatch(req_t request, model::node_id leader_id);

    template<auto LocalFunc, auto RemoteFunc, typename req_t>
    requires requires(
      cloud_topics::l1::leader_router f, const model::ntp& ntp, req_t req) {
        (f.*LocalFunc)(std::move(req), ntp, ss::shard_id{0});
        request_has_metastore_partition<req_t>
          || request_has_topic_id_partition<req_t>;
    }
    ss::future<typename req_t::resp_t> process(req_t req, bool local_only);

    ss::future<rpc::add_objects_reply> add_objects_locally(
      rpc::add_objects_request, const model::ntp& metastore_ntp, ss::shard_id);

    ss::future<rpc::replace_objects_reply> replace_objects_locally(
      rpc::replace_objects_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_first_offset_ge_reply> get_first_offset_ge_locally(
      rpc::get_first_offset_ge_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_first_timestamp_ge_reply>
    get_first_timestamp_ge_locally(
      rpc::get_first_timestamp_ge_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_first_offset_for_bytes_reply>
    get_first_offset_for_bytes_locally(
      rpc::get_first_offset_for_bytes_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_offsets_reply> get_offsets_locally(
      rpc::get_offsets_request, const model::ntp& metastore_ntp, ss::shard_id);

    ss::future<rpc::get_size_reply> get_size_locally(
      rpc::get_size_request, const model::ntp& metastore_ntp, ss::shard_id);

    ss::future<rpc::get_compaction_info_reply> get_compaction_info_locally(
      rpc::get_compaction_info_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_term_for_offset_reply> get_term_for_offset_locally(
      rpc::get_term_for_offset_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_end_offset_for_term_reply>
    get_end_offset_for_term_locally(
      rpc::get_end_offset_for_term_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::set_start_offset_reply> set_start_offset_locally(
      rpc::set_start_offset_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::remove_topics_reply> remove_topics_locally(
      rpc::remove_topics_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_compaction_infos_reply> get_compaction_infos_locally(
      rpc::get_compaction_infos_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::get_extent_metadata_reply> get_extent_metadata_locally(
      rpc::get_extent_metadata_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::flush_domain_reply> flush_domain_locally(
      rpc::flush_domain_request, const model::ntp& metastore_ntp, ss::shard_id);

    ss::future<rpc::restore_domain_reply> restore_domain_locally(
      rpc::restore_domain_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::future<rpc::preregister_objects_reply> preregister_objects_locally(
      rpc::preregister_objects_request,
      const model::ntp& metastore_ntp,
      ss::shard_id);

    ss::gate _gate;
    model::node_id _self;
    ss::sharded<cluster::metadata_cache>* _metadata;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<::rpc::connection_cache>* _connection_cache;
    domain_supervisor* _domain_supervisor;
    leader_router_probe _probe;
};

} // namespace cloud_topics::l1
