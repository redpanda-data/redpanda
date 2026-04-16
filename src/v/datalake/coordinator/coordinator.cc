/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/coordinator.h"

#include "base/vlog.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
#include "datalake/partition_spec_parser.h"
#include "datalake/record_translator.h"
#include "datalake/table_id_provider.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"
#include "storage/record_batch_builder.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <exception>
#include <optional>

namespace datalake::coordinator {

namespace {
coordinator::errc convert_stm_errc(coordinator_stm::errc e) {
    switch (e) {
    case coordinator_stm::errc::not_leader:
        return coordinator::errc::not_leader;
    case coordinator_stm::errc::shutting_down:
        return coordinator::errc::shutting_down;
    case coordinator_stm::errc::apply_error:
        return coordinator::errc::stm_apply_error;
    case coordinator_stm::errc::raft_error:
        return coordinator::errc::timedout;
    }
}
} // namespace

coordinator::ensure_table_result_t coordinator::notify_waiters_and_erase(
  const coordinator::table_key& key,
  coordinator::ensure_table_map_t& in_flight_map,
  ensure_table_result_t res) {
    auto it = in_flight_map.find(key);
    if (it == in_flight_map.end()) {
        // Unexpected, but benign.
        return res;
    }
    for (auto& prom : it->second) {
        prom.set_value(res);
    }
    in_flight_map.erase(it);
    return res;
}

std::optional<ss::future<coordinator::ensure_table_result_t>>
coordinator::maybe_add_waiter(
  const table_key& key, coordinator::ensure_table_map_t& in_flight_map) {
    auto it = in_flight_map.find(key);
    if (it != in_flight_map.end()) {
        // There's an in-flight request already. Attach to it and wait for it
        // to complete.
        auto& proms = it->second;
        ss::promise<ensure_table_result_t> res_prom;
        auto res_fut = res_prom.get_future();
        proms.emplace_back(std::move(res_prom));
        return std::move(res_fut);
    }
    in_flight_map.insert({key, {}});
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& o, coordinator::errc e) {
    switch (e) {
    case coordinator::errc::not_leader:
        return o << "coordinator::errc::not_leader";
    case coordinator::errc::shutting_down:
        return o << "coordinator::errc::shutting_down";
    case coordinator::errc::stm_apply_error:
        return o << "coordinator::errc::stm_apply_error";
    case coordinator::errc::revision_mismatch:
        return o << "coordinator::errc::revision_mismatch";
    case coordinator::errc::incompatible_schema:
        return o << "coordinator::errc::incompatible_schema";
    case coordinator::errc::timedout:
        return o << "coordinator::errc::timedout";
    case coordinator::errc::failed:
        return o << "coordinator::errc::failed";
    }
}

void coordinator::start() {
    ssx::spawn_with_gate(gate_, [this] { return run_until_abort(); });
}

ss::future<> coordinator::stop_and_wait() {
    vlog(datalake_log.debug, "Coordinator stopping...");
    as_.request_abort();
    leader_cond_.broken();
    if (term_as_.has_value()) {
        term_as_->get().request_abort();
    }
    co_await gate_.close();
    vlog(datalake_log.debug, "Coordinator stopped...");
}

ss::future<> coordinator::run_until_abort() {
    while (!as_.abort_requested()) {
        auto& raft = *stm_->raft();
        if (!raft.is_leader()) {
            bool shutdown = false;
            auto leader = co_await ss::coroutine::as_future(
              leader_cond_.wait());
            if (leader.failed()) {
                auto eptr = leader.get_exception();
                if (ssx::is_shutdown_exception(eptr)) {
                    shutdown = true;
                }
            }
            if (shutdown || as_.abort_requested()) {
                // Conservative continue.
                continue;
            }
        }
        auto sync_res = co_await stm_->sync(10s);
        if (sync_res.has_error()) {
            auto msg = "Error while syncing coordinator";
            switch (sync_res.error()) {
            case coordinator_stm::errc::shutting_down:
            case coordinator_stm::errc::not_leader:
                vlog(datalake_log.debug, "{}", msg);
                break;
            case coordinator_stm::errc::raft_error:
            case coordinator_stm::errc::apply_error:
                vlog(datalake_log.warn, "{}", msg);
                break;
            }
            continue;
        }
        auto synced_term = sync_res.value();
        auto term_fut = co_await ss::coroutine::as_future(
          run_until_term_change(synced_term));
        if (term_fut.failed()) {
            auto e = term_fut.get_exception();
            auto lvl = ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                                     : ss::log_level::error;
            vlogl(
              datalake_log,
              lvl,
              "Coordinator exception in term {}: {}",
              synced_term,
              e);
            continue;
        }
        auto term_res = term_fut.get();
        if (term_res.has_error()) {
            vlog(
              datalake_log.debug,
              "Coordinator error while running in term {}: {}",
              synced_term,
              term_res.error());
            continue;
        }
    }
    co_return;
}

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::run_until_term_change(model::term_id term) {
    auto& raft = *stm_->raft();
    vassert(!term_as_.has_value(), "Multiple calls to run_until_term_change");
    ss::abort_source term_as;
    term_as_ = term_as;
    auto reset_term_as = ss::defer([this] { term_as_.reset(); });
    vlog(datalake_log.debug, "Running coordinator loop in term {}", term);
    while (raft.is_leader() && term == raft.term()) {
        // Make a copy of the topics to reconcile, in case the map changes
        // during this call.
        // TODO: probably worth building a more robust scheduler.
        chunked_vector<model::topic> topics;
        for (const auto& [t, _] : stm_->state().topic_to_state) {
            topics.emplace_back(t);
        }
        for (const auto& t : topics) {
            // TODO: per topic means embarrassingly parallel.

            if (!disable_snapshot_expiry_()) {
                // Before we add to the table, clean up any expired snapshots.
                // TODO: consider decoupling this from the commit interval.
                auto removal_res
                  = co_await snapshot_remover_.remove_expired_snapshots(
                    t, stm_->state(), model::timestamp::now());
                if (removal_res.has_error()) {
                    switch (removal_res.error()) {
                    case snapshot_remover::errc::shutting_down:
                        co_return errc::shutting_down;
                    case snapshot_remover::errc::failed:
                        vlog(
                          datalake_log.debug,
                          "Error removing snapshots from catalog for topic {}",
                          t);
                    }
                    // Inentional fallthrough -- snapshot removal shouldn't
                    // block progressing appends.
                }
            }

            auto commit_res
              = co_await file_committer_.commit_topic_files_to_catalog(
                t, stm_->state());
            if (commit_res.has_error()) {
                switch (commit_res.error()) {
                case file_committer::errc::shutting_down:
                    co_return errc::shutting_down;
                case file_committer::errc::failed:
                    vlog(
                      datalake_log.debug,
                      "Error while committing files to catalog for topic {}",
                      t);
                    continue;
                }
            }
            // TODO: apply table retention periodically too.

            auto updates = std::move(commit_res.value());
            if (!updates.empty()) {
                storage::record_batch_builder builder(
                  model::record_batch_type::datalake_coordinator,
                  model::offset{0});
                for (auto& update : updates) {
                    builder.add_raw_kv(
                      serde::to_iobuf(mark_files_committed_update::key),
                      serde::to_iobuf(std::move(update)));
                }
                auto repl_res = co_await stm_->replicate_and_wait(
                  term, std::move(builder).build(), as_);
                if (repl_res.has_error()) {
                    auto e = convert_stm_errc(repl_res.error());
                    vlog(datalake_log.warn, "Replication failed {}", e);
                    co_return e;
                }
            }

            // check if the topic has been deleted and we need to clean up
            // topic state.
            while (true) {
                auto update_res = co_await update_lifecycle_state(t, term);
                if (update_res.has_error()) {
                    co_return update_res.error();
                }
                if (update_res.value() == ss::stop_iteration::yes) {
                    break;
                }
            }
        }
        auto sleep_res = co_await ss::coroutine::as_future(
          ssx::sleep_abortable(commit_interval_(), as_, term_as));
        if (sleep_res.failed()) {
            auto eptr = sleep_res.get_exception();
            auto log_lvl = ssx::is_shutdown_exception(eptr)
                             ? ss::log_level::debug
                             : ss::log_level::warn;
            vlogl(
              datalake_log,
              log_lvl,
              "Coordinator hit exception while sleeping in term {}: {}",
              term,
              eptr);
            co_return errc::shutting_down;
        }
        // Onto the next iteration!
    }
    co_return std::nullopt;
}

checked<ss::gate::holder, coordinator::errc> coordinator::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return errc::shutting_down;
    }
    return gate_.hold();
}

struct coordinator::table_schema_provider {
    virtual iceberg::table_identifier
    get_table_id(const model::topic&) const = 0;

    virtual ss::future<checked<iceberg::struct_type, coordinator::errc>>
      get_record_type(record_schema_components) const = 0;

    virtual ss::sstring
    get_partition_spec(const cluster::topic_metadata&) const = 0;

    virtual ~table_schema_provider() = default;
};

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::do_ensure_table_exists(
  model::topic topic,
  model::revision_id topic_revision,
  record_schema_components comps,
  std::string_view method_name,
  const table_schema_provider& schema_provider) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }

    vlog(
      datalake_log.debug,
      "{} requested, topic: {} rev: {}",
      method_name,
      topic,
      topic_revision);

    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    auto topic_md = topic_table_.get_topic_metadata_ref(
      model::topic_namespace_view{model::kafka_namespace, topic});
    if (!topic_md || topic_md->get().get_revision() != topic_revision) {
        vlog(
          datalake_log.debug,
          "Rejecting {} for {} rev {}, topic table revision {}",
          method_name,
          topic,
          topic_revision,
          topic_md ? std::optional{topic_md->get().get_revision()}
                   : std::nullopt);
        co_return errc::revision_mismatch;
    }

    auto partition_spec_str = schema_provider.get_partition_spec(
      topic_md->get());
    auto partition_spec = parse_partition_spec(partition_spec_str);
    if (!partition_spec.has_value()) {
        vlog(
          datalake_log.warn,
          "{} failed, couldn't parse partition spec {} for {} rev {}: {}",
          method_name,
          partition_spec_str,
          topic,
          topic_revision,
          partition_spec.error());
        co_return errc::failed;
    }

    topic_lifecycle_update update{
      .topic = topic,
      .revision = topic_revision,
      .new_state = topic_state::lifecycle_state_t::live,
    };
    auto check_res = update.can_apply(stm_->state());
    if (check_res.has_error()) {
        vlog(
          datalake_log.debug,
          "Rejecting {} for {} rev {}: {}",
          method_name,
          topic,
          topic_revision,
          check_res.error());
        co_return errc::revision_mismatch;
    }

    if (check_res.value()) {
        // update is non-trivial
        storage::record_batch_builder builder(
          model::record_batch_type::datalake_coordinator, model::offset{0});
        builder.add_raw_kv(
          serde::to_iobuf(topic_lifecycle_update::key),
          serde::to_iobuf(std::move(update)));
        auto repl_res = co_await stm_->replicate_and_wait(
          sync_res.value(), std::move(builder).build(), as_);
        if (repl_res.has_error()) {
            co_return convert_stm_errc(repl_res.error());
        }
    }

    // TODO: verify stm state after replication

    auto table_id = schema_provider.get_table_id(topic);
    auto record_type = co_await schema_provider.get_record_type(
      std::move(comps));
    if (!record_type.has_value()) {
        vlog(
          datalake_log.warn,
          "{} failed, couldn't resolve record type for {} rev {}",
          method_name,
          topic,
          topic_revision);
        co_return errc::failed;
    }

    auto ensure_res = co_await schema_mgr_.ensure_table_schema(
      table_id, record_type.value(), partition_spec.value());
    if (ensure_res.has_error()) {
        switch (ensure_res.error()) {
        case schema_manager::errc::not_supported:
            co_return errc::incompatible_schema;
        case schema_manager::errc::failed:
            co_return errc::failed;
        case schema_manager::errc::shutting_down:
            co_return errc::shutting_down;
        }
    }

    co_return std::nullopt;
}

struct coordinator::main_table_schema_provider
  : public coordinator::table_schema_provider {
    explicit main_table_schema_provider(coordinator& parent)
      : parent(parent) {}

    iceberg::table_identifier
    get_table_id(const model::topic& topic) const final {
        return table_id_provider::table_id(topic);
    }

    ss::future<checked<iceberg::struct_type, coordinator::errc>>
    get_record_type(record_schema_components comps) const final {
        std::optional<shared_resolved_type_t> val_type;
        if (comps.val_identifier) {
            auto type_res = co_await parent.type_resolver_.resolve_identifier(
              comps.val_identifier.value());
            if (type_res.has_error()) {
                co_return errc::failed;
            }
            val_type = std::move(type_res.value());
        }

        auto record_type = default_translator{}.build_type(std::move(val_type));
        co_return std::move(record_type.type);
    }

    ss::sstring
    get_partition_spec(const cluster::topic_metadata& topic_md) const final {
        return parent.get_effective_default_partition_spec(
          topic_md.get_configuration().properties.iceberg_partition_spec);
    }

    const coordinator& parent;
};

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::sync_ensure_table_exists(
  model::topic topic,
  model::revision_id topic_revision,
  record_schema_components comps) {
    // If there's already an inflight request for this table, attach it to the
    // in-flight request.
    table_key key{
      .topic = topic, .topic_revision = topic_revision, .record_comps = comps};
    auto waiter_fut = maybe_add_waiter(key, in_flight_main_);
    if (waiter_fut.has_value()) {
        co_return co_await std::move(*waiter_fut);
    }
    auto res_fut = co_await ss::coroutine::as_future(do_ensure_table_exists(
      topic,
      topic_revision,
      std::move(comps),
      "sync_ensure_table_exists",
      main_table_schema_provider{*this}));

    if (res_fut.failed()) {
        // NOTE: we don't expect any exceptions given we're using result types,
        // but be conservative to guarantee fulfilling the promises.
        auto ex = res_fut.get_exception();
        vlog(datalake_log.error, "Exception ensuring table: {}", ex);
        co_return notify_waiters_and_erase(key, in_flight_main_, errc::failed);
    }
    co_return notify_waiters_and_erase(key, in_flight_main_, res_fut.get());
}

struct coordinator::dlq_table_schema_provider
  : public coordinator::table_schema_provider {
    explicit dlq_table_schema_provider(coordinator& parent)
      : parent(parent) {}

    iceberg::table_identifier
    get_table_id(const model::topic& topic) const final {
        return table_id_provider::dlq_table_id(topic);
    }

    ss::future<checked<iceberg::struct_type, coordinator::errc>>
    get_record_type(record_schema_components) const final {
        co_return key_value_translator{}.build_type(std::nullopt).type;
    }

    ss::sstring get_partition_spec(const cluster::topic_metadata&) const final {
        return parent.get_effective_default_partition_spec(std::nullopt);
    }

    const coordinator& parent;
};

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::sync_ensure_dlq_table_exists(
  model::topic topic, model::revision_id topic_revision) {
    table_key key{
      .topic = topic, .topic_revision = topic_revision, .record_comps = {}};
    auto waiter_fut = maybe_add_waiter(key, in_flight_dlq_);
    if (waiter_fut.has_value()) {
        co_return co_await std::move(*waiter_fut);
    }
    auto res_fut = co_await ss::coroutine::as_future(do_ensure_table_exists(
      topic,
      topic_revision,
      record_schema_components{},
      "sync_ensure_dlq_table_exists",
      dlq_table_schema_provider{*this}));

    if (res_fut.failed()) {
        // NOTE: we don't expect any exceptions given we're using result types,
        // but be conservative to guarantee fulfilling the promises.
        auto ex = res_fut.get_exception();
        vlog(datalake_log.error, "Exception ensuring DLQ table: {}", ex);
        co_return notify_waiters_and_erase(key, in_flight_dlq_, errc::failed);
    }
    co_return notify_waiters_and_erase(key, in_flight_dlq_, res_fut.get());
}

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::sync_add_files(
  model::topic_partition tp,
  model::revision_id topic_revision,
  chunked_vector<translated_offset_range> entries) {
    if (entries.empty()) {
        vlog(datalake_log.debug, "Empty entry requested {}", tp);
        co_return std::nullopt;
    }
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    vlog(
      datalake_log.debug,
      "Sync add files requested {} (topic rev: {}): [{}, {}], kafka_bytes: {} "
      "files: {}",
      tp,
      topic_revision,
      entries.begin()->start_offset,
      entries.back().last_offset,
      entries.back().kafka_bytes_processed,
      entries.size());
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    auto topic_it = stm_->state().topic_to_state.find(tp.topic);
    if (
      topic_it == stm_->state().topic_to_state.end()
      || topic_it->second.revision != topic_revision) {
        vlog(
          datalake_log.debug,
          "Rejecting request to add files for {}: unexpected topic revision",
          tp);
        co_return errc::revision_mismatch;
    }

    auto added_last_offset = entries.back().last_offset;
    auto update_res = add_files_update::build(
      stm_->state(), tp, topic_revision, std::move(entries));
    if (update_res.has_error()) {
        // NOTE: rejection here is just an optimization -- the operation would
        // fail to be applied to the STM anyway.
        vlog(
          datalake_log.debug,
          "Rejecting request to add files for {}: {}",
          tp,
          update_res.error());
        co_return errc::stm_apply_error;
    }
    storage::record_batch_builder builder(
      model::record_batch_type::datalake_coordinator, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(add_files_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (repl_res.has_error()) {
        co_return convert_stm_errc(repl_res.error());
    }
    // query the STM once again after the scheduling point
    topic_it = stm_->state().topic_to_state.find(tp.topic);
    // Check that the resulting state matches that expected by the caller.
    // NOTE: a mismatch here just means there was a race to update the STM, and
    // this should be handled by callers.
    // TODO: would be nice to encapsulate this in some update validator.
    if (
      topic_it == stm_->state().topic_to_state.end()
      || topic_it->second.revision != topic_revision) {
        vlog(
          datalake_log.debug,
          "Unexpected topic revision for {} after STM update",
          tp);
        co_return errc::stm_apply_error;
    }

    auto prt_opt = stm_->state().partition_state(tp);
    if (
      !prt_opt.has_value() || prt_opt->get().pending_entries.empty()
      || prt_opt->get().pending_entries.back().data.last_offset
           != added_last_offset) {
        vlog(
          datalake_log.debug,
          "Resulting last offset for {} does not match expected {}",
          tp,
          added_last_offset);
        co_return errc::stm_apply_error;
    }
    co_return std::nullopt;
}

ss::future<checked<coordinator::last_offsets, coordinator::errc>>
coordinator::sync_get_last_added_offsets(
  model::topic_partition tp, model::revision_id requested_topic_rev) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }
    auto topic_it = stm_->state().topic_to_state.find(tp.topic);
    if (topic_it == stm_->state().topic_to_state.end()) {
        co_return last_offsets{std::nullopt, std::nullopt};
    }
    const auto& topic = topic_it->second;
    if (requested_topic_rev < topic.revision) {
        vlog(
          datalake_log.debug,
          "asked offsets for tp {} but rev {} is obsolete, current rev: {}",
          tp,
          requested_topic_rev,
          topic.revision);
        co_return errc::revision_mismatch;
    } else if (requested_topic_rev > topic.revision) {
        if (topic.lifecycle_state == topic_state::lifecycle_state_t::purged) {
            // Coordinator is ready to accept files for the new topic revision,
            // but there is no stm record yet. Reply with "no offset".
            co_return last_offsets{std::nullopt, std::nullopt};
        }

        vlog(
          datalake_log.debug,
          "asked offsets for tp {} rev: {}, but rev: {} still not purged",
          tp,
          requested_topic_rev,
          topic.revision);
        co_return errc::revision_mismatch;
    }

    if (topic.lifecycle_state != topic_state::lifecycle_state_t::live) {
        vlog(
          datalake_log.debug,
          "asked offsets for tp {} rev: {}, but it is already closed",
          tp,
          requested_topic_rev);
        co_return errc::revision_mismatch;
    }

    auto partition_it = topic.pid_to_pending_files.find(tp.partition);
    if (partition_it == topic.pid_to_pending_files.end()) {
        co_return last_offsets{std::nullopt, std::nullopt};
    }
    const auto& prt_state = partition_it->second;
    if (prt_state.pending_entries.empty()) {
        co_return last_offsets{
          prt_state.last_committed, prt_state.last_committed};
    }
    co_return last_offsets{
      prt_state.pending_entries.back().data.last_offset,
      prt_state.last_committed};
}

void coordinator::notify_leadership(std::optional<model::node_id> leader_id) {
    auto node_id = stm_->raft()->self().id();
    bool is_leader = leader_id && *leader_id == stm_->raft()->self().id();
    vlog(
      datalake_log.debug,
      "Coordinator leadership notification: is_leader: {}, leader_id: {}, self "
      "node id: {}",
      is_leader,
      leader_id ? *leader_id : model::node_id{},
      node_id);
    if (is_leader) {
        leader_cond_.signal();
    } else if (term_as_.has_value()) {
        term_as_->get().request_abort();
    }
}

ss::future<checked<ss::stop_iteration, coordinator::errc>>
coordinator::update_lifecycle_state(
  const model::topic& t, model::term_id term) {
    auto topic_it = stm_->state().topic_to_state.find(t);
    if (topic_it == stm_->state().topic_to_state.end()) {
        co_return ss::stop_iteration::yes;
    }
    const auto& topic = topic_it->second;
    auto revision = topic.revision;

    if (revision >= topic_table_.last_applied_revision()) {
        // topic table not yet up-to-date
        co_return ss::stop_iteration::yes;
    }

    topic_state::lifecycle_state_t new_state;
    switch (topic.lifecycle_state) {
    case topic_state::lifecycle_state_t::live: {
        auto topic_md = topic_table_.get_topic_metadata_ref(
          model::topic_namespace_view{model::kafka_namespace, t});
        if (topic_md && revision >= topic_md->get().get_revision()) {
            // topic still exists
            co_return ss::stop_iteration::yes;
        }

        new_state = topic_state::lifecycle_state_t::closed;
        break;
    }
    case topic_state::lifecycle_state_t::closed: {
        if (topic.has_pending_entries()) {
            // can't purge yet, have to deal with pending entries first
            co_return ss::stop_iteration::yes;
        }

        // Now that we don't have pending files, we can check if the
        // corresponding iceberg tombstone is present, and if it is, drop the
        // table.

        auto tombstone_it = topic_table_.get_iceberg_tombstones().find(
          model::topic_namespace_view{model::kafka_namespace, t});
        if (tombstone_it != topic_table_.get_iceberg_tombstones().end()) {
            auto tombstone_rev = tombstone_it->second.last_deleted_revision;
            // The Glue REST catalog doesn't support purging data; explicitly
            // pass that down to the drop operations.
            auto should_purge = using_glue_catalog()
                                  ? file_committer::purge_data::no
                                  : file_committer::purge_data::yes;
            if (tombstone_rev >= topic.revision) {
                // Drop the main table if it exists.
                {
                    auto table_id = table_id_provider::table_id(t);
                    auto drop_res = co_await file_committer_.drop_table(
                      table_id, should_purge);
                    if (drop_res.has_error()) {
                        switch (drop_res.error()) {
                        case file_committer::errc::shutting_down:
                            co_return errc::shutting_down;
                        case file_committer::errc::failed:
                            vlog(
                              datalake_log.warn,
                              "failed to drop table for topic {}",
                              t);
                            co_return ss::stop_iteration::yes;
                        }
                    }
                }

                // Drop the DLQ table if it exists.
                {
                    auto dlq_table_id = table_id_provider::dlq_table_id(t);
                    auto drop_res = co_await file_committer_.drop_table(
                      dlq_table_id, should_purge);
                    if (drop_res.has_error()) {
                        switch (drop_res.error()) {
                        case file_committer::errc::shutting_down:
                            co_return errc::shutting_down;
                        case file_committer::errc::failed:
                            vlog(
                              datalake_log.warn,
                              "failed to drop dlq table for topic {}",
                              t);
                            co_return ss::stop_iteration::yes;
                        }
                    }
                }
            }
            auto ts_res = co_await remove_tombstone_(t, tombstone_rev);
            if (ts_res.has_error()) {
                if (ts_res.error() == errc::shutting_down) {
                    co_return ts_res.error();
                }
                co_return ss::stop_iteration::yes;
            }
        }

        new_state = topic_state::lifecycle_state_t::purged;
        break;
    }
    case topic_state::lifecycle_state_t::purged:
        co_return ss::stop_iteration::yes;
    }

    topic_lifecycle_update update{
      .topic = t,
      .revision = revision,
      .new_state = new_state,
    };
    auto check_res = update.can_apply(stm_->state());
    if (check_res.has_error()) {
        vlog(
          datalake_log.debug,
          "Rejecting lifecycle transition request {}: {}",
          update,
          check_res.error());
        co_return errc::stm_apply_error;
    }
    storage::record_batch_builder builder(
      model::record_batch_type::datalake_coordinator, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(topic_lifecycle_update::key),
      serde::to_iobuf(std::move(update)));

    auto repl_res = co_await stm_->replicate_and_wait(
      term, std::move(builder).build(), as_);
    if (repl_res.has_error()) {
        auto e = convert_stm_errc(repl_res.error());
        vlog(datalake_log.warn, "Replication failed {}", e);
        co_return e;
    }

    co_return ss::stop_iteration::no;
}

ss::future<checked<datalake_usage_stats, coordinator::errc>>
coordinator::sync_get_usage_stats() {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    datalake_usage_stats result;
    for (const auto& [topic, state] : stm_->state().topic_to_state) {
        result.topic_usages.emplace_back(
          topic, state.revision, state.total_kafka_bytes_processed);
    }
    co_return result;
}

ss::future<
  checked<chunked_hash_map<model::topic, topic_state>, coordinator::errc>>
coordinator::sync_get_topic_state(chunked_vector<model::topic> topics_filter) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    chunked_hash_map<model::topic, topic_state> result;
    if (topics_filter.empty()) {
        for (const auto& [t, state] : stm_->state().topic_to_state) {
            result.emplace(t, state.copy());
        }
        co_return result;
    }
    for (const auto& topic : topics_filter) {
        auto topic_it = stm_->state().topic_to_state.find(topic);
        if (topic_it != stm_->state().topic_to_state.end()) {
            result.insert({topic, topic_it->second.copy()});
        }
    }
    co_return result;
}

ss::future<checked<void, coordinator::errc>>
coordinator::sync_reset_topic_state(
  model::topic topic,
  model::revision_id topic_revision,
  bool reset_all_partitions,
  chunked_hash_map<model::partition_id, partition_state_override>
    partition_overrides) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }

    vlog(datalake_log.debug, "Resetting coordinator state for topic {}", topic);
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    reset_topic_state_update update{
      .topic = topic,
      .topic_revision = topic_revision,
      .reset_all_partitions = reset_all_partitions,
      .partition_overrides = std::move(partition_overrides),
    };
    auto check_res = update.can_apply(stm_->state());
    if (check_res.has_error()) {
        vlog(
          datalake_log.debug,
          "Rejecting reset topic state request for {}: {}",
          topic,
          check_res.error());
        co_return errc::stm_apply_error;
    }
    storage::record_batch_builder builder(
      model::record_batch_type::datalake_coordinator, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(reset_topic_state_update::key),
      serde::to_iobuf(std::move(update)));

    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (repl_res.has_error()) {
        auto e = convert_stm_errc(repl_res.error());
        vlog(datalake_log.warn, "Replication failed {}", e);
        co_return e;
    }

    co_return outcome::success();
}

ss::sstring coordinator::get_effective_default_partition_spec(
  const std::optional<ss::sstring>& partition_spec) const {
    const auto& cfg = config::shard_local_cfg();
    auto current_spec = partition_spec.value_or(default_partition_spec_());
    if (
      using_glue_catalog()
      && current_spec == cfg.iceberg_default_partition_spec.default_value()) {
        // Glue can't partition on nested fields like redpanda.timestamp.
        static constexpr auto rate_limit = std::chrono::seconds(5);
        static thread_local ss::logger::rate_limit rate(rate_limit);
        vloglr(
          datalake_log,
          ss::log_level::warn,
          rate,
          "Overriding default partition spec to '()' for AWS Glue "
          "compatibility");
        return "()";
    }

    return current_spec;
}

bool coordinator::using_glue_catalog() const {
    const auto& cfg = config::shard_local_cfg();
    return cfg.iceberg_catalog_type() == config::datalake_catalog_type::rest
           && cfg.iceberg_rest_catalog_authentication_mode()
                == config::datalake_catalog_auth_mode::aws_sigv4
           && cfg.iceberg_rest_catalog_aws_service_name() == "glue";
}

} // namespace datalake::coordinator
