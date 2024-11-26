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
#include "container/fragmented_vector.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
#include "datalake/table_creator.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"
#include "storage/record_batch_builder.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <exception>

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
    as_.request_abort();
    leader_cond_.broken();
    if (term_as_.has_value()) {
        term_as_->get().request_abort();
    }
    return gate_.close();
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
            vlog(
              datalake_log.error,
              "Coordinator hit exception while running in term {}: {}",
              synced_term,
              term_fut.get_exception());
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

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::sync_ensure_table_exists(
  model::topic topic,
  model::revision_id topic_revision,
  record_schema_components comps) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }

    vlog(
      datalake_log.debug,
      "Sync ensure table exists requested, topic: {} rev: {}",
      topic,
      topic_revision);

    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }

    // TODO: add mutex to protect against the thundering herd problem

    topic_lifecycle_update update{
      .topic = topic,
      .revision = topic_revision,
      .new_state = topic_state::lifecycle_state_t::live,
    };
    auto check_res = update.can_apply(stm_->state());
    if (check_res.has_error()) {
        vlog(
          datalake_log.debug,
          "Rejecting ensure_table_exist for {} rev {}: {}",
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

    auto ensure_res = co_await table_creator_.ensure_table(
      topic, topic_revision, std::move(comps));
    if (ensure_res.has_error()) {
        switch (ensure_res.error()) {
        case table_creator::errc::incompatible_schema:
            co_return errc::incompatible_schema;
        case table_creator::errc::failed:
            co_return errc::failed;
        case table_creator::errc::shutting_down:
            co_return errc::shutting_down;
        }
    }

    co_return std::nullopt;
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
      "Sync add files requested {} (topic rev: {}): [{}, {}], {} files",
      tp,
      topic_revision,
      entries.begin()->start_offset,
      entries.back().last_offset,
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
            if (tombstone_rev >= topic.revision) {
                auto drop_res = co_await file_committer_.drop_table(t);
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

} // namespace datalake::coordinator
