/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "cluster/metadata_cache.h"
#include "cluster/tx_hash_ranges.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "seastar/core/sharded.hh"

#include <seastar/core/reactor.hh>

namespace cluster {

inline model::partition_id
get_tx_coordinator_partition(tx_id_hash tx_id_hash, int32_t partition_cnt) {
    auto default_partition_range_size = get_default_tx_hash_range_size(
      partition_cnt);

    auto partition = int32_t(tx_id_hash / default_partition_range_size);

    // Last partition in default distibuiton can have bigger range
    if (partition >= partition_cnt) {
        return model::partition_id(partition_cnt - 1);
    }
    return model::partition_id(partition);
}

inline model::ntp get_tx_coordinator_ntp(
  const kafka::transactional_id& tx_id, int32_t partition_count) {
    return {
      model::tx_manager_nt.ns,
      model::tx_manager_nt.tp,
      get_tx_coordinator_partition(get_tx_id_hash(tx_id), partition_count)};
}

/**
 * \brief Mapping from transaction id to coordinator ntp.
 *
 * A kafka transactional id is used to identify coordinator partition that
 * hosts this transaction.
 *
 * Currently this mapping is static.
 * It matches partition to specific section of hash value.
 * tx is considered to be on specific partition if its hash belongs to
 * section for this partiion.
 *
 * In future we will have separate stm that will hold mapping from tx_id to ntp
 * Mapper will check local tm partitions, if they contain specific tx_id
 * otherwise mapper will make request to mapper stm leader to identify
 * partition. This will allow us to make dynamic mapping
 */

class tx_coordinator_mapper {
public:
    explicit tx_coordinator_mapper(ss::sharded<cluster::metadata_cache>& md)
      : _md(md) {}

    ss::future<std::optional<model::ntp>>
    ntp_for(kafka::transactional_id tx_id) const {
        auto cfg = _md.local().get_topic_cfg(model::tx_manager_nt);
        if (!cfg) {
            // Transaction coordinator topic not exist in cache
            // should be catched by caller (find_coordinator)
            // It must wait for topic in cache or init topic
            co_return std::nullopt;
        }
        int32_t partitions_amount = cfg->partition_count;

        tx_id_hash tx_id_hash = get_tx_id_hash(tx_id);
        auto partition = get_tx_coordinator_partition(
          tx_id_hash, partitions_amount);
        co_return model::ntp(
          model::tx_manager_nt.ns, model::tx_manager_nt.tp, partition);
    }

    const model::ns& ns() const { return model::tx_manager_nt.ns; }
    const model::topic& topic() const { return model::tx_manager_nt.tp; }

private:
    ss::sharded<cluster::metadata_cache>& _md;
};

} // namespace cluster
