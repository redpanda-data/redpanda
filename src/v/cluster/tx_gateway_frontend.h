/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/tm_stm.h"
#include "cluster/tx_protocol_types.h"
#include "features/feature_table.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"
#include "utils/available_promise.h"

namespace cluster {

class tx_gateway_frontend final
  : public ss::peering_sharded_service<tx_gateway_frontend> {
public:
    tx_gateway_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      cluster::controller*,
      ss::sharded<cluster::id_allocator_frontend>&,
      rm_group_proxy*,
      ss::sharded<cluster::rm_partition_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::tm_stm_cache_manager>&,
      ss::sharded<tx_topic_manager>&,
      config::binding<uint64_t> max_transactions_per_coordinator);

    std::optional<model::ntp> ntp_for_tx_id(const kafka::transactional_id&);

    ss::future<fetch_tx_reply> fetch_tx_locally(
      kafka::transactional_id, model::term_id, model::partition_id);
    ss::future<init_tm_tx_reply> init_tm_tx(
      kafka::transactional_id,
      std::chrono::milliseconds transaction_timeout_ms,
      model::timeout_clock::duration,
      model::producer_identity);
    ss::future<add_partitions_tx_reply> add_partition_to_tx(
      add_partitions_tx_request, model::timeout_clock::duration);
    ss::future<add_offsets_tx_reply>
      add_offsets_to_tx(add_offsets_tx_request, model::timeout_clock::duration);
    ss::future<end_tx_reply>
      end_txn(end_tx_request, model::timeout_clock::duration);

    using return_all_txs_res = result<fragmented_vector<tx_metadata>, tx::errc>;
    ss::future<return_all_txs_res>
    get_all_transactions_for_one_tx_partition(model::ntp tx_manager_ntp);
    ss::future<return_all_txs_res> get_all_transactions();
    ss::future<result<tx_metadata, tx::errc>>
      describe_tx(kafka::transactional_id);

    ss::future<try_abort_reply> route_globally(try_abort_request&&);
    ss::future<try_abort_reply> route_locally(try_abort_request&&);

    ss::future<tx::errc> delete_partition_from_tx(
      kafka::transactional_id, tx_metadata::tx_partition);

    ss::future<find_coordinator_reply>
      find_coordinator(kafka::transactional_id);

    ss::future<> stop();

private:
    ss::abort_source _as;
    ss::gate _gate;
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    cluster::controller* _controller;
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
    rm_group_proxy* _rm_group_proxy;
    ss::sharded<cluster::rm_partition_frontend>& _rm_partition_frontend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<cluster::tm_stm_cache_manager>& _tm_stm_cache_manager;
    ss::sharded<tx_topic_manager>& _tx_topic_manager;
    int16_t _metadata_dissemination_retries;
    std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;
    ss::timer<model::timeout_clock> _expire_timer;
    config::binding<std::chrono::milliseconds> _transactional_id_expiration;
    bool _transactions_enabled;
    config::binding<uint64_t> _max_transactions_per_coordinator;

    // Transaction GA includes: KIP_447, KIP-360, fix for compaction tx_group*
    // records, perf fix#1(Do not writing preparing state on disk in tm_stn),
    // perf fix#2(Do not writeing prepared marker in rm_stm)
    bool is_transaction_ga() {
        return _feature_table.local().is_active(
          features::feature::transaction_ga);
    }

    bool is_fetch_tx_supported() {
        return _feature_table.local().is_active(
          features::feature::tm_stm_cache);
    }

    void start_expire_timer();

    void rearm_expire_timer(bool force = false);

    ss::future<std::optional<model::node_id>>
    wait_for_leader(const model::ntp&);

    ss::future<checked<tx_metadata, tx::errc>> get_tx(
      model::term_id,
      ss::shared_ptr<tm_stm>,
      kafka::transactional_id,
      model::timeout_clock::duration);
    ss::future<checked<tx_metadata, tx::errc>> bump_etag(
      model::term_id,
      ss::shared_ptr<cluster::tm_stm>,
      cluster::tx_metadata,
      model::timeout_clock::duration);
    ss::future<checked<tx_metadata, tx::errc>> forget_tx(
      model::term_id,
      ss::shared_ptr<cluster::tm_stm>,
      cluster::tx_metadata,
      model::timeout_clock::duration);
    ss::future<checked<tx_metadata, tx::errc>> get_latest_tx(
      model::term_id,
      ss::shared_ptr<tm_stm>,
      model::producer_identity,
      kafka::transactional_id,
      model::timeout_clock::duration);
    ss::future<checked<tx_metadata, tx::errc>> get_ongoing_tx(
      model::term_id,
      ss::shared_ptr<tm_stm>,
      model::producer_identity,
      kafka::transactional_id,
      model::timeout_clock::duration);

    ss::future<checked<tx_metadata, tx::errc>>
      fetch_tx(kafka::transactional_id, model::term_id, model::partition_id);
    ss::future<> dispatch_fetch_tx(
      kafka::transactional_id,
      model::term_id,
      model::partition_id,
      model::timeout_clock::duration,
      ss::lw_shared_ptr<available_promise<checked<tx_metadata, tx::errc>>>);
    ss::future<fetch_tx_reply> dispatch_fetch_tx(
      model::node_id,
      kafka::transactional_id,
      model::term_id,
      model::partition_id,
      model::timeout_clock::duration,
      ss::lw_shared_ptr<available_promise<checked<tx_metadata, tx::errc>>>);
    ss::future<try_abort_reply> do_try_abort(
      model::term_id,
      ss::shared_ptr<tm_stm>,
      kafka::transactional_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);

    ss::future<cluster::init_tm_tx_reply> init_tm_tx_locally(
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::timeout_clock::duration,
      model::producer_identity,
      model::partition_id);
    ss::future<cluster::init_tm_tx_reply> limit_init_tm_tx(
      ss::shared_ptr<tm_stm>,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::timeout_clock::duration,
      model::producer_identity);
    ss::future<cluster::init_tm_tx_reply> do_init_tm_tx(
      ss::shared_ptr<tm_stm>,
      model::term_id,
      kafka::transactional_id,
      std::chrono::milliseconds,
      model::timeout_clock::duration,
      model::producer_identity);

    ss::future<end_tx_reply> do_end_txn(
      checked<ss::shared_ptr<tm_stm>, tx::errc>,
      end_tx_request,
      model::timeout_clock::duration);
    ss::future<checked<cluster::tx_metadata, tx::errc>> do_end_txn(
      end_tx_request,
      ss::shared_ptr<cluster::tm_stm>,
      model::timeout_clock::duration,
      ss::lw_shared_ptr<available_promise<tx::errc>>);
    ss::future<checked<cluster::tx_metadata, tx::errc>> do_abort_tm_tx(
      model::term_id,
      ss::shared_ptr<cluster::tm_stm>,
      cluster::tx_metadata,
      model::timeout_clock::duration);
    ss::future<checked<cluster::tx_metadata, tx::errc>> do_commit_tm_tx(
      model::term_id,
      ss::shared_ptr<cluster::tm_stm>,
      cluster::tx_metadata,
      model::timeout_clock::duration,
      ss::lw_shared_ptr<available_promise<tx::errc>>);
    ss::future<checked<cluster::tx_metadata, tx::errc>> recommit_tm_tx(
      ss::shared_ptr<tm_stm>,
      model::term_id,
      tx_metadata,
      model::timeout_clock::duration);
    ss::future<checked<cluster::tx_metadata, tx::errc>> reabort_tm_tx(
      ss::shared_ptr<tm_stm>,
      model::term_id,
      tx_metadata,
      model::timeout_clock::duration);

    template<typename Func>
    auto with_stm(model::partition_id tm, Func&& func);

    template<typename T>
    ss::future<typename T::reply> do_dispatch(model::node_id, T&&);

    template<typename T>
    ss::future<typename T::reply> do_route_globally(model::ntp, T&&);

    template<typename T>
    ss::future<typename T::reply> do_route_locally(model::ntp, T&&);

    ss::future<add_partitions_tx_reply> do_add_partition_to_tx(
      ss::shared_ptr<tm_stm>,
      add_partitions_tx_request,
      model::timeout_clock::duration);
    ss::future<add_offsets_tx_reply> do_add_offsets_to_tx(
      ss::shared_ptr<tm_stm>,
      add_offsets_tx_request,
      model::timeout_clock::duration);

    ss::future<tx::errc> do_delete_partition_from_tx(
      ss::shared_ptr<tm_stm>,
      kafka::transactional_id,
      tx_metadata::tx_partition);

    ss::future<tx_metadata> remove_deleted_partitions_from_tx(
      ss::shared_ptr<tm_stm>, model::term_id term, cluster::tx_metadata tx);

    ss::future<result<tx_metadata, tx::errc>>
      describe_tx(ss::shared_ptr<tm_stm>, kafka::transactional_id);

    ss::future<try_abort_reply>
    process_locally(ss::shared_ptr<tm_stm>, try_abort_request&&);

    void expire_old_txs();
    ss::future<> expire_old_txs(model::ntp);
    ss::future<> expire_old_txs(ss::shared_ptr<tm_stm>);
    ss::future<> expire_old_tx(ss::shared_ptr<tm_stm>, kafka::transactional_id);
    ss::future<tx::errc> do_expire_old_tx(
      ss::shared_ptr<tm_stm>,
      model::term_id term,
      tx_metadata,
      model::timeout_clock::duration,
      bool ignore_update_ts);

    friend tx_gateway;
};
} // namespace cluster
