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
#include "container/fragmented_vector.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
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
    case coordinator::errc::timedout:
        return o << "coordinator::errc::timedout";
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
            if (updates.empty()) {
                // Nothing to replicate.
                continue;
            }
            storage::record_batch_builder builder(
              model::record_batch_type::datalake_coordinator, model::offset{0});
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
coordinator::sync_add_files(
  model::topic_partition tp, chunked_vector<translated_offset_range> entries) {
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
      "Sync add files requested {}: [{}, {}], {} files",
      tp,
      entries.begin()->start_offset,
      entries.back().last_offset,
      entries.size());
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }
    auto added_last_offset = entries.back().last_offset;
    auto update_res = add_files_update::build(
      stm_->state(), tp, std::move(entries));
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

    // Check that the resulting state matches that expected by the caller.
    // NOTE: a mismatch here just means there was a race to update the STM, and
    // this should be handled by callers.
    // TODO: would be nice to encapsulate this in some update validator.
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

ss::future<checked<std::optional<kafka::offset>, coordinator::errc>>
coordinator::sync_get_last_added_offset(model::topic_partition tp) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }
    auto prt_state_opt = stm_->state().partition_state(tp);
    if (!prt_state_opt.has_value()) {
        co_return std::nullopt;
    }
    const auto& prt_state = prt_state_opt->get();
    if (prt_state.pending_entries.empty()) {
        co_return prt_state.last_committed;
    }
    co_return prt_state.pending_entries.back().data.last_offset;
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
} // namespace datalake::coordinator
