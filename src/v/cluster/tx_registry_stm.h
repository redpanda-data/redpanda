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
#include "cluster/persisted_stm.h"
#include "cluster/tx_hash_ranges.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace config {
struct configuration;
}

namespace cluster {

class tx_registry_stm final : public persisted_stm<> {
public:
    enum class batch_subtype : int32_t { tx_mapping = 1 };

    struct tx_mapping
      : serde::
          envelope<tx_mapping, serde::version<0>, serde::compat_version<0>> {
        repartitioning_id id;
        absl::flat_hash_map<model::partition_id, hosted_txs> mapping;

        tx_mapping() = default;

        tx_mapping(
          repartitioning_id id,
          absl::flat_hash_map<model::partition_id, hosted_txs> mapping)
          : id(id)
          , mapping(std::move(mapping)) {}

        friend bool operator==(const tx_mapping&, const tx_mapping&) = default;

        auto serde_fields() { return std::tie(id, mapping); }
    };

    explicit tx_registry_stm(ss::logger&, raft::consensus*);

    explicit tx_registry_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::gate& gate() { return _gate; }

    ss::future<ss::basic_rwlock<>::holder> read_lock() {
        return _lock.hold_read_lock();
    }

    ss::future<ss::basic_rwlock<>::holder> write_lock() {
        return _lock.hold_write_lock();
    }

    ss::future<checked<model::term_id, errc>> sync();

    ss::future<bool>
    try_init_mapping(model::term_id term, int32_t partitions_count);

    std::optional<model::partition_id>
      find_hosting_partition(kafka::transactional_id);

    bool seen_unknown_batch_subtype() const {
        return _seen_unknown_batch_subtype;
    }

    bool is_initialized() const { return _initialized; }

    const tx_mapping& get_mapping() const { return _mapping; }

    std::string_view get_name() const final { return "tx_registry_stm"; }
    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

private:
    ss::future<bool> do_write_mapping(model::term_id, tx_mapping);

    ss::future<checked<model::term_id, errc>>
      do_sync(model::timeout_clock::duration);

    ss::future<result<raft::replicate_result>>
    replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
        return _raft->replicate(
          term,
          model::make_memory_record_batch_reader(std::move(batch)),
          raft::replicate_options{raft::consistency_level::quorum_ack});
    }

    ss::future<> apply(const model::record_batch&) override;

    ss::future<> truncate_log_prefix();
    ss::future<> apply_local_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_local_snapshot() override;
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    ss::basic_rwlock<> _lock;

    // Unlike the data partitions tx_registry_stm doesn't rely on the eviction
    // stm and manages log truncations on its own. STM counts the number of
    // applied commands and when it (`_processed`) surpasses
    // `_log_capacity` tx_registry_stm trucates the prefix.
    int64_t _processed{0};
    config::binding<int16_t> _log_capacity;
    model::offset _next_snapshot{-1};
    bool _initialized{false};
    bool _seen_unknown_batch_subtype{false};
    tx_mapping _mapping;

    bool _is_truncating{false};
};

} // namespace cluster
