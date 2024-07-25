/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm.h"

#include "cloud_topics/dl_stm_cmd.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/persisted_stm.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "ssx/semaphore.h"
#include "storage/offset_translator_state.h"
#include "utils/uuid.h"

namespace cloud_topics::detail {

sorted_run_t::sorted_run_t(const dl_overlay& o)
  : base(o.base_offset)
  , last(o.last_offset)
  , ts_base(o.base_ts)
  , ts_last(o.last_ts) {
    values.push_back(o);
}

bool sorted_run_t::maybe_append(const dl_overlay& o) {
    if (values.empty()) {
        values.push_back(o);
        base = o.base_offset;
        last = o.last_offset;
        ts_base = o.base_ts;
        ts_last = o.last_ts;
        return true;
    }
    // check invariant
    if (o.base_offset < kafka::next_offset(last)) {
        return false;
    }
    values.push_back(o);
    last = o.last_offset;
    ts_base = std::min(o.base_ts, ts_base);
    ts_last = std::max(o.last_ts, ts_last);
    return true;
}

void overlay_collection::append(const dl_overlay& o) noexcept {
    do_append(o);

    // If too many runs are accumulated we need to compact the storage
    // by k-way merging them.
    if (_runs.size() > max_runs) {
        compact();
    }
}

std::optional<dl_overlay> overlay_collection::lower_bound(
  kafka::offset o) const noexcept { // TODO: add overload for the timequery
    // This is guaranteed to be small because max_runs is small.
    std::vector<dl_overlay> results;
    results.reserve(max_runs);
    for (const auto& run : _runs) {
        auto it = std::lower_bound(
          run.values.begin(),
          run.values.end(),
          o,
          [](const dl_overlay& ov, kafka::offset key) {
              return ov.last_offset < key;
          });
        if (it != run.values.end()) {
            results.push_back(*it);
        }
    }
    if (results.empty()) {
        return std::nullopt;
    }
    // Select single value
    // NOTE: this is based on heuristic but it should
    // probably be avoided in the production code.

    // Prefer exact matches
    auto part = std::partition(
      results.begin(), results.end(), [o](const dl_overlay& ov) {
          return ov.base_offset <= o && o <= ov.last_offset;
      });
    if (part != results.begin()) {
        // This indicates that we found at least one run that contains
        // the key.
        results.resize(part - results.begin());
    }
    // Prefer exclusive ownership above anything else
    part = std::partition(
      results.begin(), results.end(), [](const dl_overlay& ov) {
          return ov.ownership == dl_stm_object_ownership::exclusive;
      });
    if (part != results.begin()) {
        // This indicates that we found at least one exclusively owned
        // sorted run. In this case no need to look at anything else.
        results.resize(part - results.begin());
    }

    // Chose the longest run (partial sort). Note that we can't
    // have more than max_runs results. This should work fast.
    std::nth_element(
      results.begin(),
      std::next(results.begin()),
      results.end(),
      [](const dl_overlay& lhs, const dl_overlay& rhs) {
          return (lhs.last_offset - lhs.base_offset)
                 > (rhs.last_offset - rhs.base_offset);
      });
    return results.front();
}

void overlay_collection::compact() noexcept {
    using priority_queue_t = std::
      priority_queue<dl_overlay, chunked_vector<dl_overlay>, std::greater<>>;

    std::deque<sorted_run_t> tmp;
    std::swap(tmp, _runs);

    // Initialize pq
    priority_queue_t pq;
    for (auto& run : tmp) {
        for (const auto& val : run.values) {
            pq.emplace(val);
        }
        run.values.clear();
    }

    // Main loop, pull data from pq and move it into the object
    while (!pq.empty()) {
        auto overlay = pq.top();
        pq.pop();
        do_append(overlay);
    }
    vassert(_runs.size() < max_runs, "Too many runs - {}", _runs.size());
}

void overlay_collection::do_append(const dl_overlay& o) noexcept {
    // Try to accommodate the overlay into existing
    // run.
    bool done = false;
    for (auto& run : _runs) {
        done = run.maybe_append(o); // TODO: clear terms before inserting
        if (done) {
            return;
        }
    }

    // If no run could accommodate the overlay create a new run.
    // This path is also taken if we're inserting the first overlay.
    _runs.emplace_back(o);
}

bool dl_stm_state::register_overlay_cmd(const dl_overlay& o) noexcept {
    // Validate command
    if (o.base_offset == kafka::offset{}) {
        return false;
    }
    if (o.base_offset > o.last_offset) {
        return false;
    }
    if (o.size_bytes() == 0) {
        return false;
    }
    // Admit command
    _overlays.append(o);

    // Propagate last reconciled offset
    propagate_last_reconciled_offset(o.base_offset, o.last_offset);

    // Update terms
    for (auto kv : o.terms) {
        _term_map.insert(kv);
    }
    return true;
}

std::optional<dl_overlay>
dl_stm_state::lower_bound(kafka::offset o) const noexcept {
    return _overlays.lower_bound(o);
}

std::optional<kafka::offset>
dl_stm_state::get_term_last_offset(model::term_id t) const noexcept {
    auto it = _term_map.upper_bound(t);
    if (it == _term_map.end()) {
        return std::nullopt;
    }
    return kafka::prev_offset(it->second);
}

void dl_stm_state::propagate_last_reconciled_offset(
  kafka::offset base, kafka::offset last) noexcept {
    // If newly added offset range connects with the last reconciled offset
    // we need to propagate it forward. We also need to take into account that
    // the last object may not be added to the end of the offset range but
    // fill the gap inside the offset range.
    // So basically we have two cases:
    // case 1:
    // [           LRO][new object] <- new LRO
    // case 2:
    // [          LRO][new object][         ] <- new LRO
    // To handle the second case we need to scan the overlays.
    if (last < _last_reconciled_offset) {
        // The overlay replaces some other overlay (leveling or
        // compaction).
        return;
    }
    if (_last_reconciled_offset == kafka::offset{}) {
        // First overlay
        _last_reconciled_offset = last;
        return;
    }
    if (kafka::prev_offset(base) > _last_reconciled_offset) {
        // We're creating the gap between last reconciled offset
        // and the newly added object. The new overlay is not
        // connected to any other in the collection.
        return;
    }
    // Invariant: here prev_offset(base) <= LRO.
    // Handle case 1;
    _last_reconciled_offset = last;
    // Handle case 2;
    auto next = _overlays.lower_bound(kafka::next_offset(last));
    while (next.has_value()) {
        _last_reconciled_offset = next->last_offset;
        next = _overlays.lower_bound(kafka::next_offset(next->last_offset));
    }
    // TODO: similar mechanism should be used to track objects with unique
    // ownership and compacted objects
}

} // namespace cloud_topics::detail

namespace cloud_topics {

dl_stm_command_builder::dl_stm_command_builder(
  dl_stm* parent, model::term_id term)
  : _builder(model::record_batch_type::dl_stm_cmd, model::offset{0})
  , _parent(parent)
  , _term(term) {}

void dl_stm_command_builder::add_overlay_batch(
  object_id id,
  first_byte_offset_t offset,
  byte_range_size_t size_bytes,
  dl_stm_object_ownership ownership,
  kafka::offset base_offset,
  kafka::offset last_offset,
  model::timestamp base_ts,
  model::timestamp last_ts,
  absl::btree_map<model::term_id, kafka::offset> terms) {
    dl_overlay payload{
      .base_offset = base_offset,
      .last_offset = last_offset,
      .base_ts = base_ts,
      .last_ts = last_ts,
      .terms = terms,
      .id = id,
      .ownership = ownership,
      .offset = offset,
      .size_bytes = size_bytes,
    };
    _builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::overlay),
      serde::to_iobuf(std::move(payload)));
}

ss::future<bool> dl_stm_command_builder::replicate() {
    auto rb = std::move(_builder).build();
    co_return co_await _parent->replicate(_term, std::move(rb));
}

dl_stm::dl_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft)
  , _state() {}

ss::future<> dl_stm::stop() {
    _as.request_abort();
    return _gate.close().then([this] { return raft::persisted_stm<>::stop(); });
}

dl_stm_command_builder dl_stm::make_command_builder(model::term_id term) {
    return dl_stm_command_builder(this, term);
}

model::offset dl_stm::max_collectible_offset() {
    // The LRO tracked by the in-memory state is a Kafka offset but
    // max collectible offset is a log offset. We need to translate
    // the offset first. It's safe to assume that the offset can be
    // translated because max_collectible offset can only be moved
    // forward by advancing LRO.
    auto ko = _state.get_last_reconciled();
    auto ot = _raft->log()->get_offset_translator_state();
    auto lo = ot->to_log_offset(kafka::offset_cast(ko));
    // TODO: take _last_persisted_offset into account
    return std::min(_state.get_insync_offset(), lo);
}

ss::future<bool>
dl_stm::replicate(model::term_id term, model::record_batch rb) {
    // TODO: synchronization
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush(); // needed for in-memory writes
    auto result = co_await _raft->replicate(
      term, model::make_memory_record_batch_reader(std::move(rb)), opts);
    if (result.has_error()) {
        vlog(
          _log.info,
          "Failed to replicate record batch, error: {}",
          result.error());
        co_return false;
    }
    co_return true;
}

ss::future<> dl_stm::do_apply(const model::record_batch& batch) {
    // Apply record batches in offset order.
    // Propagate offsets forward.
    vlog(_log.debug, "Applying record batch: {}", batch.header());
    if (batch.header().type != model::record_batch_type::dl_stm_cmd) {
        co_return;
    }
    batch.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<dl_stm_key>(r.release_key());
        switch (key) {
        case dl_stm_key::overlay: {
            vlog(_log.debug, "Decoding dl_overlay batch: {}", key);
            auto value = serde::from_iobuf<dl_overlay>(r.release_value());
            _state.register_overlay_cmd(value);
        } break;
        };
    });
    co_return;
}

std::optional<dl_overlay> dl_stm::lower_bound(kafka::offset o) const noexcept {
    return _state.lower_bound(o);
}

std::optional<kafka::offset>
dl_stm::get_term_last_offset(model::term_id t) const noexcept {
    return _state.get_term_last_offset(t);
}

kafka::offset dl_stm::get_last_reconciled() const noexcept {
    return _state.get_last_reconciled();
}

ss::future<> dl_stm::apply_raft_snapshot(const iobuf& snapshot_data) {
    co_return; // TODO: fixme
}

ss::future<> dl_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& snapshot_data) {
    // The 'data' contains snapshot from the local file.
    auto state = serde::from_iobuf<detail::dl_stm_state>(
      std::move(snapshot_data));
    //
    vlog(
      _log.info,
      "applying snapshot, snapshot offset: {}, insync offset: {}, LRO: {}",
      header.offset,
      state.get_insync_offset(),
      state.get_last_reconciled());

    // Install snapshot
    _state = std::move(state);
    co_return;
}

ss::future<raft::stm_snapshot>
dl_stm::take_local_snapshot(ssx::semaphore_units u) {
    // Take local file snapshot
    auto snapshot_data = serde::to_iobuf(_state);
    vlog(
      _log.debug,
      "creating snapshot at offset: {}, insync offset: {}, LRO: {}",
      last_applied_offset(),
      _state.get_insync_offset(),
      get_last_reconciled());
    co_return raft::stm_snapshot::create(
      0, last_applied_offset(), std::move(snapshot_data));
}

ss::future<iobuf> dl_stm::take_snapshot(model::offset) {
    // Take 'raft' snapshot that will be consumed by another
    // replica through the 'apply_raft_snapshot' method.
    // The implementation disrespects the offset and the apply
    // method can apply any snapshot as long as its insync
    // offset is available locally.
    co_return iobuf{}; // TODO
}

bool dl_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    return ntp_cfg.ntp().ns == model::kafka_namespace
           && is_shadow_topic(ntp_cfg);
}

bool dl_stm_factory::is_shadow_topic(const storage::ntp_config& ntp_cfg) const {
    return ntp_cfg.ntp().tp.topic().ends_with("_st");
}

void dl_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<cloud_topics::dl_stm>(
      cloud_topics::cd_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace cloud_topics
