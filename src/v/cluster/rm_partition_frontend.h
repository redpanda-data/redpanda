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

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "rpc/fwd.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>

namespace cluster {

class rm_partition_frontend {
public:
    rm_partition_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      cluster::controller*);

    ss::future<begin_tx_reply> begin_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::timeout_clock::duration,
      model::partition_id);
    ss::future<prepare_tx_reply> prepare_tx(
      model::ntp,
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<commit_tx_reply> commit_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<abort_tx_reply> abort_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<> stop() {
        _as.request_abort();
        return ss::make_ready_future<>();
    }

private:
    ss::abort_source _as;
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    cluster::controller* _controller;
    int16_t _metadata_dissemination_retries;
    std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;

    bool is_leader_of(const model::ntp&) const;

    ss::future<begin_tx_reply> dispatch_begin_tx(
      model::node_id,
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::timeout_clock::duration,
      model::partition_id);
    ss::future<begin_tx_reply> begin_tx_locally(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<begin_tx_reply> do_begin_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<prepare_tx_reply> dispatch_prepare_tx(
      model::node_id,
      model::ntp,
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<prepare_tx_reply> prepare_tx_locally(
      model::ntp,
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<prepare_tx_reply> do_prepare_tx(
      model::ntp,
      model::term_id,
      model::partition_id,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<commit_tx_reply> dispatch_commit_tx(
      model::node_id,
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<commit_tx_reply> commit_tx_locally(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<commit_tx_reply> do_commit_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<abort_tx_reply> dispatch_abort_tx(
      model::node_id,
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<abort_tx_reply> abort_tx_locally(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);
    ss::future<abort_tx_reply> do_abort_tx(
      model::ntp,
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration);

    friend tx_gateway;
};
} // namespace cluster
