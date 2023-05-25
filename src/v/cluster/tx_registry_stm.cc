// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_registry_stm.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace cluster {

static model::record_batch
serialize_mapping(tx_registry_stm::tx_mapping mapping) {
    storage::record_batch_builder b(
      model::record_batch_type::tx_registry, model::offset(0));
    b.add_raw_kv(
      serde::to_iobuf(
        static_cast<int32_t>(tx_registry_stm::batch_subtype::tx_mapping)),
      serde::to_iobuf(mapping));
    return std::move(b).build();
}

tx_registry_stm::tx_registry_stm(ss::logger& logger, raft::consensus* c)
  : tx_registry_stm(logger, c, config::shard_local_cfg()) {}

tx_registry_stm::tx_registry_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& cfg)
  : persisted_stm("tx_registry.snapshot", logger, c)
  , _sync_timeout(cfg.tx_registry_sync_timeout_ms.bind())
  , _log_capacity(cfg.tx_registry_log_capacity.bind()) {}

ss::future<checked<model::term_id, errc>> tx_registry_stm::sync() {
    return ss::with_gate(_gate, [this] { return do_sync(_sync_timeout()); });
}

ss::future<checked<model::term_id, errc>>
tx_registry_stm::do_sync(model::timeout_clock::duration timeout) {
    if (!_c->is_leader()) {
        co_return errc::not_leader;
    }

    auto ready = co_await persisted_stm::sync(timeout);
    if (!ready) {
        co_return errc::generic_tx_error;
    }
    co_return _insync_term;
}

ss::future<> tx_registry_stm::apply(model::record_batch b) {
    const auto& hdr = b.header();

    if (hdr.type == model::record_batch_type::tx_registry) {
        vlog(
          txlog.trace, "processing tx_registry batch at {}", hdr.base_offset);

        vassert(
          b.record_count() == 1,
          "tx_registry batch must contain a single record");
        auto r = b.copy_records();
        auto& record = *r.begin();
        auto key = record.release_key();

        auto subtype = serde::from_iobuf<int32_t>(std::move(key));
        if (subtype == static_cast<int32_t>(batch_subtype::tx_mapping)) {
            vlog(
              txlog.trace,
              "processing tx_registry/tx_mapping cmd at {}",
              hdr.base_offset);
            auto value = record.release_value();
            _mapping = serde::from_iobuf<tx_mapping>(std::move(value));
            _initialized = true;
        } else {
            vlog(
              txlog.trace,
              "unknown tx_registry/{} cmd at {}",
              subtype,
              hdr.base_offset);
            _seen_unknown_batch_subtype = true;
        }
    }

    _insync_offset = b.last_offset();

    _processed++;
    if (_processed > _log_capacity()) {
        ssx::spawn_with_gate(_gate, [this] { return truncate_log_prefix(); });
    }

    return ss::now();
}

ss::future<bool> tx_registry_stm::try_init_mapping(
  model::term_id term, int32_t partitions_count) {
    if (_initialized) {
        co_return true;
    }

    auto units = co_await write_lock();
    if (_initialized) {
        co_return true;
    }

    tx_mapping mapping;
    mapping.id = repartitioning_id(0);

    for (int pid = 0; pid < partitions_count; pid++) {
        model::partition_id partition = model::partition_id(pid);
        auto initial_hash_range = default_hash_range(
          partition, partitions_count);
        hosted_txs initial_hosted_transactions{};
        auto res = hosted_transactions::add_range(
          initial_hosted_transactions, initial_hash_range);
        vassert(
          res == tx_hash_ranges_errc::success,
          "default txn hash space must be complete");
        mapping.mapping[partition] = initial_hosted_transactions;
    }

    auto batch = serialize_mapping(std::move(mapping));
    auto r = co_await replicate_quorum_ack(term, std::move(batch));
    if (!r) {
        vlog(
          txlog.info, "got error {} on initing default hash_ranges", r.error());
        if (_c->is_leader() && _c->term() == term) {
            co_await _c->step_down(
              "tx register try_init_mapping repliation error");
        }
        co_return false;
    }
    auto offset = r.value().last_offset;
    if (!co_await wait_no_throw(
          offset, model::timeout_clock::now() + _sync_timeout())) {
        vlog(
          txlog.info,
          "timeout on waiting until {} is applied on updating hash_ranges",
          offset);
        if (_c->is_leader() && _c->term() == term) {
            co_await _c->step_down("tx registry apply timeout");
        }
        co_return false;
    }
    if (_c->term() != term) {
        vlog(
          txlog.info,
          "lost leadership while waiting until {} is applied on updating hash "
          "ranges",
          offset);
        co_return false;
    }

    co_return true;
}

ss::future<> tx_registry_stm::truncate_log_prefix() {
    if (_is_truncating) {
        return ss::now();
    }
    if (_processed <= _log_capacity()) {
        return ss::now();
    }
    _is_truncating = true;
    return _c->write_snapshot(raft::write_snapshot_cfg(_next_snapshot, iobuf()))
      .then([this] {
          _next_snapshot = _insync_offset;
          _processed = 0;
      })
      .finally([this] { _is_truncating = false; });
}

ss::future<> tx_registry_stm::apply_snapshot(stm_snapshot_header, iobuf&&) {
    return ss::make_exception_future<>(
      std::logic_error("tx_registry_stm doesn't support snapshots"));
}

ss::future<stm_snapshot> tx_registry_stm::take_snapshot() {
    return ss::make_exception_future<stm_snapshot>(
      std::logic_error("tx_registry_stm doesn't support snapshots"));
}

ss::future<> tx_registry_stm::handle_raft_snapshot() {
    return write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _next_snapshot = _c->start_offset();
          _processed = 0;
          set_next(_next_snapshot);
          _insync_offset = model::prev_offset(_next_snapshot);
          return ss::now();
      });
}

} // namespace cluster
