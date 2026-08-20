/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/consumer_group_stm.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "raft/errc.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace kafka {

namespace {

/// What the state machine's groups write through: nothing. The groups are
/// applied state on every replica; the serving path provides real writers
/// where writes are legal.
class null_offset_writer final : public offset_writer {
public:
    model::term_id term() const final { return model::term_id{}; }

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch, model::term_id) final {
        return ss::make_ready_future<result<raft::replicate_result>>(
          raft::errc::not_leader);
    }

    ss::future<result<raft::replicate_result>>
    replicate(chunked_vector<model::record_batch>, model::term_id) final {
        return ss::make_ready_future<result<raft::replicate_result>>(
          raft::errc::not_leader);
    }

    raft::replicate_stages replicate_in_stages(
      chunked_vector<model::record_batch>, model::term_id) final {
        return raft::replicate_stages(raft::errc::not_leader);
    }

    ss::future<> maybe_step_down(model::term_id, std::string_view) final {
        return ss::now();
    }
};

class null_tx_coordinator_client final : public tx_coordinator_client {
public:
    ss::future<cluster::try_abort_reply> try_abort(
      model::partition_id, model::producer_identity, model::tx_seq) final {
        return ss::make_ready_future<cluster::try_abort_reply>(
          cluster::try_abort_reply(cluster::tx::errc::not_coordinator));
    }
};

template<typename T>
member_partitions to_member_partitions(const chunked_vector<T>& topics) {
    member_partitions out;
    for (const auto& t : topics) {
        auto [it, inserted] = out.try_emplace(t.topic_id);
        for (auto p : t.partitions) {
            it->second.emplace(p);
        }
    }
    return out;
}

consumer_group_subscription
to_subscription(consumer_group_member_metadata_value v) {
    // subscribed_topic_regex waits for regex subscriptions, and
    // classic_metadata for classic member migration; neither is modeled yet.
    return {
      .client_id = std::move(v.client_id),
      .client_host = std::move(v.client_host),
      .instance_id = std::move(v.instance_id),
      .rack_id = std::move(v.rack_id),
      .topics = std::move(v.subscribed_topic_names),
      .rebalance_timeout = v.rebalance_timeout,
      .assignor = std::move(v.server_assignor),
    };
}

consumer_group_member_assignment
to_member_assignment(const consumer_group_current_member_assignment_value& v) {
    // the per-partition assignment_epochs are not modeled yet; they fence
    // stale offset commits, which is the offset paths' concern
    return {
      .epoch = kafka::member_epoch(v.member_epoch),
      .previous_epoch = kafka::member_epoch(v.previous_member_epoch),
      .state = v.state,
      .assigned = to_member_partitions(v.assigned_partitions),
      .pending_revocation = to_member_partitions(
        v.partitions_pending_revocation),
    };
}

} // namespace

consumer_group_stm::consumer_group_stm(
  ss::logger& logger,
  raft::consensus* raft,
  ss::sharded<features::feature_table>& feature_table)
  : raft::persisted_stm<>("consumer_group_stm.snapshot", logger, raft)
  , group_data_parser<consumer_group_stm>()
  , _catchup_lock(ss::make_lw_shared<ss::rwlock>())
  , _feature_table(feature_table) {}

ss::lw_shared_ptr<consumer_group>
consumer_group_stm::get_group(const kafka::group_id& id) const {
    if (auto it = _groups.find(id); it != _groups.end()) {
        return it->second;
    }
    return nullptr;
}

ss::lw_shared_ptr<offset_store>
consumer_group_stm::get_offsets(const kafka::group_id& id) const {
    if (auto it = _offsets.find(id); it != _offsets.end()) {
        return it->second;
    }
    return nullptr;
}

ss::lw_shared_ptr<offset_store>
consumer_group_stm::get_or_create_offsets(const kafka::group_id& id) {
    auto it = _offsets.find(id);
    if (it == _offsets.end()) {
        it = _offsets
               .emplace(
                 id,
                 ss::make_lw_shared<offset_store>(
                   id,
                   config::shard_local_cfg(),
                   _catchup_lock,
                   std::make_unique<null_offset_writer>(),
                   model::term_id{},
                   std::make_unique<null_tx_coordinator_client>(),
                   _feature_table,
                   // The store outlives any group of its id, so no group's
                   // death can stop it applying.
                   [] { return false; },
                   offset_store::role::applied))
               .first;
    }
    return it->second;
}

consumer_group&
consumer_group_stm::get_or_create_group(const kafka::group_id& id) {
    auto it = _groups.find(id);
    if (it == _groups.end()) {
        vlog(cg_klog.debug, "[group: {}] creating the group", id);
        it = _groups
               .emplace(
                 id,
                 ss::make_lw_shared<consumer_group>(
                   id, get_or_create_offsets(id)))
               .first;
    }
    return *it->second;
}

ss::future<bool>
consumer_group_stm::sync(model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    co_return co_await raft::persisted_stm<>::sync(timeout);
}

ss::future<> consumer_group_stm::do_apply(const model::record_batch& b) {
    co_await parse(b);
}

ss::future<>
consumer_group_stm::handle_raft_data(const model::record_batch& batch) {
    const auto base_offset = batch.base_offset();
    co_await model::for_each_record(
      batch, [this, base_offset](this auto, model::record& r) -> ss::future<> {
          const auto log_offset = model::offset(
            base_offset() + r.offset_delta());
          // A record this machine cannot decode is one it does not own: the
          // key versions it knows are its own and the classic ones, and both
          // decoders throw on anything else. Skipping the record keeps the
          // rest of the log applying, where a throw would stall apply, and
          // with it the compaction bound, for every group on the partition.
          auto applied = co_await ss::coroutine::as_future(
            apply_record(std::move(r), log_offset));
          if (applied.failed()) {
              auto error = applied.get_exception();
              vlog(
                cg_klog.error,
                "skipping the record at offset {}: {}",
                log_offset,
                error);
          }
      });
}

ss::future<>
consumer_group_stm::apply_record(model::record r, model::offset log_offset) {
    auto record_type = group_metadata_serializer::get_metadata_type(
      r.key().copy());
    switch (record_type) {
    case group_metadata:
    case noop:
        // the classic coordinator's
        co_return;
    case offset_commit:
        apply_offset_metadata(
          group_metadata_serializer::decode_offset_metadata(std::move(r)),
          log_offset);
        co_return;
    case consumer_group_metadata:
        co_await apply_group_metadata(
          group_metadata_serializer::decode_consumer_group_metadata(
            std::move(r)));
        co_return;
    case consumer_group_member_metadata:
        apply_member_metadata(
          group_metadata_serializer::decode_consumer_group_member_metadata(
            std::move(r)));
        co_return;
    case consumer_group_target_assignment_metadata:
        apply_target_assignment_metadata(
          group_metadata_serializer::
            decode_consumer_group_target_assignment_metadata(std::move(r)));
        co_return;
    case consumer_group_target_assignment_member:
        apply_target_assignment_member(
          group_metadata_serializer::
            decode_consumer_group_target_assignment_member(std::move(r)));
        co_return;
    case consumer_group_current_member_assignment:
        apply_current_member_assignment(
          group_metadata_serializer::
            decode_consumer_group_current_member_assignment(std::move(r)));
        co_return;
    }
}

ss::future<>
consumer_group_stm::apply_group_metadata(consumer_group_metadata_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "consumer group metadata")) {
        co_return;
    }
    if (md.value) {
        vlog(
          cg_klog.trace,
          "[group: {}] applying group epoch {}",
          md.key.group_id,
          md.value->epoch);
        auto& group = get_or_create_group(md.key.group_id);
        group.set_epoch(kafka::group_epoch(md.value->epoch));
        group.set_metadata_hash(md.value->metadata_hash);
        co_return;
    }
    auto it = _groups.find(md.key.group_id);
    if (it == _groups.end()) {
        vlog(
          cg_klog.trace,
          "[group: {}] tombstone for a group not owned here",
          md.key.group_id);
        co_return;
    }
    if (!it->second->deletable()) {
        // The deletion or conversion that wrote this tombstone checked that
        // the group was empty with no open transaction before replicating it,
        // and state contradicting the check landed in between. The record is
        // applied anyway: keeping a group the log has deleted would leave
        // every replica permanently disagreeing with the log, and a later
        // record for the same id reusing the stale group.
        vlog(
          cg_klog.error,
          "[group: {}] dropping a group that is no longer deletable: members: "
          "{}, open transaction: {}",
          md.key.group_id,
          it->second->members().size(),
          it->second->offsets().has_transactions_in_progress());
    }
    vlog(cg_klog.debug, "[group: {}] dropping the group", md.key.group_id);
    it->second->mark_removed();
    // The group's transactions go with it: nothing will apply their commit or
    // abort once the group is gone, so a transaction left open would hold the
    // compaction bound for the partition forever. The committed offsets stay:
    // they are keyed by group id, not by the group, and the deletion that
    // wrote this tombstone writes their tombstones too.
    it->second->offsets().reset_tx_state(model::term_id{});
    _groups.erase(it);
    co_return;
}

void consumer_group_stm::apply_member_metadata(
  consumer_group_member_metadata_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "consumer member metadata")) {
        return;
    }
    vlog(
      cg_klog.trace,
      "[group: {}] member metadata {} for {}",
      md.key.group_id,
      md.value ? "update" : "tombstone",
      md.key.member_id);
    if (md.value) {
        get_or_create_group(md.key.group_id)
          .upsert_member_subscription(
            std::move(md.key.member_id), to_subscription(std::move(*md.value)));
        return;
    }
    if (auto it = _groups.find(md.key.group_id); it != _groups.end()) {
        it->second->erase_member(md.key.member_id);
        // A member's target is written and tombstoned by its own record, but
        // a target outliving its member would be assigned to nobody.
        it->second->erase_member_target(md.key.member_id);
    }
}

void consumer_group_stm::apply_target_assignment_metadata(
  consumer_group_target_assignment_metadata_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "target assignment")) {
        return;
    }
    vlog(
      cg_klog.trace,
      "[group: {}] target assignment metadata {}",
      md.key.group_id,
      md.value ? "update" : "tombstone");
    if (md.value) {
        // assignment_timestamp is not modeled
        get_or_create_group(md.key.group_id)
          .set_assignment_epoch(
            kafka::assignment_epoch(md.value->assignment_epoch));
        return;
    }
    if (auto it = _groups.find(md.key.group_id); it != _groups.end()) {
        it->second->set_assignment_epoch(kafka::assignment_epoch{0});
    }
}

void consumer_group_stm::apply_target_assignment_member(
  consumer_group_target_assignment_member_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "member target assignment")) {
        return;
    }
    vlog(
      cg_klog.trace,
      "[group: {}] target assignment {} for {}",
      md.key.group_id,
      md.value ? "update" : "tombstone",
      md.key.member_id);
    if (md.value) {
        get_or_create_group(md.key.group_id)
          .set_member_target(
            std::move(md.key.member_id),
            to_member_partitions(md.value->topic_partitions));
        return;
    }
    if (auto it = _groups.find(md.key.group_id); it != _groups.end()) {
        it->second->erase_member_target(md.key.member_id);
    }
}

void consumer_group_stm::apply_current_member_assignment(
  consumer_group_current_member_assignment_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "member assignment")) {
        return;
    }
    vlog(
      cg_klog.trace,
      "[group: {}] member assignment {} for {}",
      md.key.group_id,
      md.value ? "update" : "tombstone",
      md.key.member_id);
    if (md.value) {
        get_or_create_group(md.key.group_id)
          .upsert_member_assignment(
            std::move(md.key.member_id), to_member_assignment(*md.value));
        return;
    }
    if (auto it = _groups.find(md.key.group_id); it != _groups.end()) {
        it->second->clear_member_assignment(md.key.member_id);
    }
}

void consumer_group_stm::apply_offset_metadata(
  offset_metadata_kv md, model::offset log_offset) {
    // Not filtered by ownership: an offset record names no protocol, and a
    // group's own records can replay after its offsets, which compaction
    // makes routine because a group's metadata record is rewritten on every
    // epoch bump while its offset records stay where they were written.
    // Filtering on the group would discard the offsets of the very group that
    // is about to be created.
    if (
      auto blocked = _group_blocks.find(md.key.group_id);
      blocked != _group_blocks.end() && blocked->second.is_blocked) {
        vlog(
          cg_klog.trace,
          "[group: {}] skipping offsets, group is blocked",
          md.key.group_id);
        return;
    }
    auto offsets = get_or_create_offsets(md.key.group_id);
    model::topic_partition tp(md.key.topic, md.key.partition);
    if (md.value) {
        const auto expiry = md.value->expiry();
        offsets->try_upsert_offset(
          tp,
          offset_store::offset_metadata{
            .log_offset = log_offset,
            .offset = md.value->offset,
            .metadata = std::move(md.value->metadata),
            .committed_leader_epoch = md.value->leader_epoch,
            .commit_timestamp = md.value->commit_timestamp,
            .expiry_timestamp = expiry,
            // the record carries no such field: it defaults to true on the
            // decoded value, and reading it here would exempt every applied
            // offset from retention
            .non_reclaimable = false,
          });
        return;
    }
    offsets->erase_offset(tp);
    // The last of an id's offsets going away leaves nothing to hold, unless a
    // group of that id is holding the store.
    if (offsets->empty() && !_groups.contains(md.key.group_id)) {
        offsets->pre_shutdown();
        _offsets.erase(md.key.group_id);
    }
}

ss::future<> consumer_group_stm::handle_tx_offsets(
  model::record_batch_header header, kafka::group_tx::offsets_metadata data) {
    auto it = _groups.find(data.group_id);
    if (it == _groups.end()) {
        return ss::now();
    }
    it->second->offsets().stage_tx_offsets(
      model::producer_identity{header.producer_id, header.producer_epoch},
      data,
      header.last_offset());
    return ss::now();
}

ss::future<> consumer_group_stm::handle_fence_v0(
  model::record_batch_header, kafka::group_tx::fence_metadata_v0) {
    // fences below the current version predate consumer groups, so none can
    // belong to one
    return ss::now();
}

ss::future<> consumer_group_stm::handle_fence_v1(
  model::record_batch_header, kafka::group_tx::fence_metadata_v1) {
    return ss::now();
}

ss::future<> consumer_group_stm::handle_fence(
  model::record_batch_header header, kafka::group_tx::fence_metadata fence) {
    auto it = _groups.find(fence.group_id);
    if (it == _groups.end()) {
        return ss::now();
    }
    it->second->offsets().apply_tx_fence(
      model::producer_identity{header.producer_id, header.producer_epoch},
      fence.tx_seq,
      fence.transaction_timeout_ms,
      fence.tm_partition,
      header.base_offset);
    return ss::now();
}

ss::future<> consumer_group_stm::handle_abort(
  model::record_batch_header header, kafka::group_tx::abort_metadata data) {
    auto it = _groups.find(data.group_id);
    if (it == _groups.end()) {
        return ss::now();
    }
    it->second->offsets().apply_tx_abort(
      model::producer_identity{header.producer_id, header.producer_epoch});
    return ss::now();
}

ss::future<> consumer_group_stm::handle_commit(
  model::record_batch_header header, kafka::group_tx::commit_metadata data) {
    auto it = _groups.find(data.group_id);
    if (it == _groups.end()) {
        return ss::now();
    }
    it->second->offsets().apply_tx_commit(
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.max_timestamp);
    return ss::now();
}

ss::future<> consumer_group_stm::handle_version_fence(
  features::feature_table::version_fence) {
    return ss::now();
}

void consumer_group_stm::handle_group_block(kafka::group_block gb) {
    base_t::do_handle_group_block(gb);
    // The group itself is kept. Blocking freezes a group so that a migration
    // can move it, and its records stop being applied while the block stands;
    // dropping the state instead would leave the group unrecoverable, since
    // unblocking replays nothing and the log is only read once.
    auto blocked = _group_blocks.find(gb.group_id);
    if (blocked != _group_blocks.end() && blocked->second.is_blocked) {
        vlog(
          cg_klog.warn,
          "[group: {}] blocked, skipping its records until it is unblocked",
          gb.group_id);
    }
}

model::offset consumer_group_stm::max_removable_local_log_offset() {
    // An open transaction holds the bound until its commit or abort is
    // applied. Expiring one that its producer abandoned is a write, so it
    // belongs to whoever serves the group, not here.
    auto result = last_applied_offset();
    for (const auto& [_, group] : _groups) {
        if (
          auto earliest = group->offsets().earliest_tx_begin_offset();
          earliest.has_value()) {
            result = std::min(result, model::prev_offset(*earliest));
        }
    }
    return result;
}

ss::future<raft::local_snapshot_applied>
consumer_group_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    // Recovery is full log replay: a snapshot would resurrect keys whose
    // tombstones were removed after it was taken, until the storage layer
    // clamps tombstone removal to this machine's snapshot offset.
    co_return raft::local_snapshot_applied::no;
}

ss::future<raft::stm_snapshot>
consumer_group_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    // The storage layer requests a background snapshot from every registered
    // machine on a segment roll, and the request cannot be declined. Write an
    // empty placeholder; apply_local_snapshot rejects it.
    const auto offset = last_applied_offset();
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      local_snapshot_version, offset, iobuf());
}

ss::future<> consumer_group_stm::apply_raft_snapshot(const iobuf&) {
    // __consumer_offsets is compacted, never deleted, so the log is not
    // prefix truncated and there is no snapshot to install state from. If
    // one arrives, the groups below it are gone and cannot be replayed back.
    vlog(
      cg_klog.error,
      "[{}] installed a raft snapshot over a log this state machine recovers "
      "by replaying: the group state below offset {} is lost",
      _raft->ntp(),
      _raft->start_offset());
    return ss::now();
}

ss::future<iobuf> consumer_group_stm::take_raft_snapshot(model::offset) {
    return ss::make_ready_future<iobuf>(iobuf());
}

ss::future<> consumer_group_stm::start() {
    co_await raft::persisted_stm<>::start();
    if (_raft->start_offset() > model::offset(0)) {
        vlog(
          cg_klog.error,
          "[{}] recovering by replay from offset {} of a prefix truncated "
          "log: any group state below it is lost",
          _raft->ntp(),
          _raft->start_offset());
    }
}

ss::future<> consumer_group_stm::stop() {
    co_await raft::persisted_stm<>::stop();
    for (const auto& [_, offsets] : _offsets) {
        offsets->pre_shutdown();
        co_await offsets->stop();
    }
}

consumer_group_stm_factory::consumer_group_stm_factory(
  ss::sharded<features::feature_table>& feature_table)
  : _feature_table(feature_table) {}

bool consumer_group_stm_factory::is_applicable_for(
  const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return ntp.ns == model::kafka_consumer_offsets_nt.ns
           && ntp.tp.topic == model::kafka_consumer_offsets_nt.tp;
}

void consumer_group_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<kafka::consumer_group_stm>(
      cg_klog, raft, _feature_table);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace kafka
