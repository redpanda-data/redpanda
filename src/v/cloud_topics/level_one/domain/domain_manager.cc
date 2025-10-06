/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/domain/domain_manager.h"

#include "cloud_topics/level_one/metastore/garbage_collector.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/logger.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/sleep.hh>

namespace cloud_topics::l1 {
namespace {
rpc::errc convert_stm_errc(simple_stm::errc e) {
    switch (e) {
    case simple_stm::errc::shutting_down:
    case simple_stm::errc::not_leader:
        return rpc::errc::not_leader;
    case simple_stm::errc::apply_error:
        return rpc::errc::concurrent_requests;
    case simple_stm::errc::raft_error:
        return rpc::errc::timed_out;
    }
}
rpc::errc convert_metastore_errc(metastore::errc e) {
    switch (e) {
    case metastore::errc::invalid_request:
        return rpc::errc::concurrent_requests;
    case metastore::errc::out_of_range:
        return rpc::errc::out_of_range;
    case metastore::errc::missing_ntp:
        return rpc::errc::missing_ntp;
    case metastore::errc::transport_error:
        return rpc::errc::timed_out;
    }
}
} // namespace

domain_manager::domain_manager(ss::shared_ptr<simple_stm> stm, io* io)
  : stm_(std::move(stm))
  , object_io_(io) {}

void domain_manager::start() {
    ssx::spawn_with_gate(gate_, [this] { return gc_loop(); });
}

ss::future<> domain_manager::stop_and_wait() {
    vlog(cd_log.debug, "Domain manager stopping...");
    as_.request_abort();
    co_await gate_.close();
    vlog(cd_log.debug, "Domain manager stopped...");
}

std::optional<ss::gate::holder> domain_manager::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return std::nullopt;
    }
    return gate_.hold();
}

ss::future<rpc::add_objects_reply>
domain_manager::add_objects(rpc::add_objects_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
    }
    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto update_res = add_objects_update::build(
      stm_state,
      std::move(req.new_objects),
      std::move(req.new_terms),
      &corrections);
    if (!update_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to add objects: {}",
          update_res.error());
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    storage::record_batch_builder builder(
      model::record_batch_type::l1_stm, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(add_objects_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (!repl_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = convert_stm_errc(repl_res.error()),
        };
    }
    // Check if any of the objects were successfully added. Presumably the
    // presence of any objects is signal enough that the update was
    // successfully applied, given these updates are atomic.
    bool any_added = false;
    for (const auto& oid : added_oids) {
        if (stm_->state().objects.contains(oid)) {
            any_added = true;
            break;
        }
    }
    if (!any_added) {
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    co_return rpc::add_objects_reply{
      .ec = rpc::errc::ok,
      .corrected_next_offsets = std::move(corrections),
    };
}

ss::future<rpc::replace_objects_reply>
domain_manager::replace_objects(rpc::replace_objects_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
    }
    auto& stm_state = stm_->state();
    auto update_res = replace_objects_update::build(
      stm_state, std::move(req.new_objects), std::move(req.compaction_updates));
    if (!update_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to replace objects: {}",
          update_res.error());
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    storage::record_batch_builder builder(
      model::record_batch_type::l1_stm, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(replace_objects_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (!repl_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = convert_stm_errc(repl_res.error()),
        };
    }
    // Check if any of the objects were successfully added. Presumably the
    // presence of any objects is signal enough that the update was
    // successfully applied, given these updates are atomic.
    bool any_added = false;
    for (const auto& oid : added_oids) {
        if (stm_->state().objects.contains(oid)) {
            any_added = true;
            break;
        }
    }
    if (!any_added) {
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    co_return rpc::replace_objects_reply{
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::get_first_offset_ge_reply>
domain_manager::get_first_offset_ge(rpc::get_first_offset_ge_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_first_ge(stm_state, req.tp, req.o);
    if (!get_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    auto& obj = get_res.value();
    co_return rpc::get_first_offset_ge_reply{
        .ec = rpc::errc::ok,
        .object = rpc::object_metadata{
            .oid = obj.oid,
            .footer_pos = obj.footer_pos,
            .object_size = obj.object_size,
            .first_offset = obj.first_offset,
            .last_offset = obj.last_offset,
        },
    };
}

ss::future<rpc::get_first_timestamp_ge_reply>
domain_manager::get_first_timestamp_ge(
  rpc::get_first_timestamp_ge_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_first_ge(stm_state, req.tp, req.ts);
    if (!get_res.has_value()) {
        co_return rpc::get_first_timestamp_ge_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    auto& obj = get_res.value();
    co_return rpc::get_first_timestamp_ge_reply{
        .ec = rpc::errc::ok,
        .object = rpc::object_metadata{
            .oid = obj.oid,
            .footer_pos = obj.footer_pos,
            .object_size = obj.object_size,
            .first_offset = obj.first_offset,
            .last_offset = obj.last_offset,
        },
    };
}

ss::future<rpc::get_first_offset_for_bytes_reply>
domain_manager::get_first_offset_for_bytes(
  rpc::get_first_offset_for_bytes_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_first_offset_for_bytes(
      stm_state, req.tp, req.size);
    if (!get_res.has_value()) {
        co_return rpc::get_first_offset_for_bytes_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    auto offset = get_res.value();
    co_return rpc::get_first_offset_for_bytes_reply{
      .offset = offset,
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::get_offsets_reply>
domain_manager::get_offsets(rpc::get_offsets_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_offsets_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_offsets_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_offsets(stm_state, req.tp);
    if (!get_res.has_value()) {
        co_return rpc::get_offsets_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_offsets_reply{
      .ec = rpc::errc::ok,
      .start_offset = get_res->start_offset,
      .next_offset = get_res->next_offset,
    };
}

ss::future<rpc::get_compaction_offsets_reply>
domain_manager::get_compaction_offsets(
  rpc::get_compaction_offsets_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_compaction_offsets_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_compaction_offsets_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_compaction_offsets(
      stm_state, req.tp, req.tombstone_removal_upper_bound_ts);
    if (!get_res.has_value()) {
        co_return rpc::get_compaction_offsets_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_compaction_offsets_reply{
      .ec = rpc::errc::ok,
      .dirty_ranges = std::move(get_res->dirty_ranges),
      .removable_tombstone_ranges = std::move(
        get_res->removable_tombstone_ranges),
    };
}

ss::future<rpc::get_term_for_offset_reply>
domain_manager::get_term_for_offset(rpc::get_term_for_offset_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_term_for_offset(
      stm_state, req.tp, req.offset);
    if (!get_res.has_value()) {
        co_return rpc::get_term_for_offset_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_term_for_offset_reply{
      .ec = rpc::errc::ok,
      .term = get_res.value(),
    };
}

ss::future<rpc::get_end_offset_for_term_reply>
domain_manager::get_end_offset_for_term(
  rpc::get_end_offset_for_term_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_end_offset_for_term(
      stm_state, req.tp, req.term);
    if (!get_res.has_value()) {
        co_return rpc::get_end_offset_for_term_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_end_offset_for_term_reply{
      .ec = rpc::errc::ok,
      .end_offset = get_res.value(),
    };
}

ss::future<rpc::set_start_offset_reply>
domain_manager::set_start_offset(rpc::set_start_offset_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::set_start_offset_reply{.ec = rpc::errc::not_leader};
    }

    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = convert_stm_errc(sync_res.error())};
    }
    auto update_res = set_start_offset_update::build(
      stm_->state(), req.tp, req.start_offset);
    if (!update_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to set start offset: {}",
          update_res.error());
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    storage::record_batch_builder builder(
      model::record_batch_type::l1_stm, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(set_start_offset_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (!repl_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = convert_stm_errc(repl_res.error()),
        };
    }
    auto prt_ref = stm_->state().partition_state(req.tp);
    if (
      !prt_ref.has_value() || prt_ref->get().start_offset != req.start_offset) {
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }

    co_return rpc::set_start_offset_reply{.ec = rpc::errc::ok};
}

ss::future<> domain_manager::gc_loop() {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return;
    }
    // TODO: make configurable.
    auto gc_interval = 5min;
    garbage_collector gc(stm_.get(), object_io_);
    while (!as_.abort_requested()) {
        vlog(cd_log.debug, "Running garbage collection now...");
        auto gc_res = co_await gc.remove_unreferenced_objects(&as_);
        if (!gc_res.has_value()) {
            vlog(cd_log.warn, "Garbage collection failed: {}", gc_res.error());
        }
        vlog(
          cd_log.debug, "Re-running garbage collection in {}...", gc_interval);
        auto sleep_res = co_await ss::coroutine::as_future(
          ssx::sleep_abortable(gc_interval, as_));
        if (sleep_res.failed()) {
            auto eptr = sleep_res.get_exception();
            auto log_lvl = ssx::is_shutdown_exception(eptr)
                             ? ss::log_level::debug
                             : ss::log_level::warn;
            vlogl(
              cd_log,
              log_lvl,
              "Garbage collection loop hit exception while sleeping: {}",
              eptr);
        }
    }
    vlog(cd_log.debug, "Garbage collection loop stopped...");
}

} // namespace cloud_topics::l1
