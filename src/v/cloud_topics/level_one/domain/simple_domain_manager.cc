/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/domain/simple_domain_manager.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/garbage_collector.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "model/batch_builder.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/sleep.hh>

#include <exception>

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

chunked_vector<rpc::extent_metadata>
meta_to_rpc_extent_metadata(metastore::extent_metadata_vec v) {
    chunked_vector<rpc::extent_metadata> res;
    res.reserve(v.size());
    for (auto& e : v) {
        std::optional<rpc::extent_object_info> obj_info;
        if (e.object_info.has_value()) {
            obj_info = rpc::extent_object_info{
              .oid = e.object_info->oid,
              .footer_pos = e.object_info->footer_pos,
              .object_size = e.object_info->object_size,
            };
        }
        res.push_back(
          rpc::extent_metadata{
            .base_offset = e.base_offset,
            .last_offset = e.last_offset,
            .max_timestamp = e.max_timestamp,
            .object_info = std::move(obj_info)});
    }
    return res;
}

} // namespace

simple_domain_manager::simple_domain_manager(
  ss::shared_ptr<simple_stm> stm, io* io)
  : gc_interval_(
      config::shard_local_cfg()
        .cloud_topics_long_term_garbage_collection_interval)
  , stm_(std::move(stm))
  , object_io_(io) {
    gc_interval_.watch([this]() { sem_.signal(); });
}

void simple_domain_manager::start() {
    ssx::spawn_with_gate(gate_, [this] { return gc_loop(); });
}

ss::future<> simple_domain_manager::stop_and_wait() {
    vlog(cd_log.debug, "Domain manager stopping...");
    as_.request_abort();
    sem_.broken();
    co_await gate_.close();
    vlog(cd_log.debug, "Domain manager stopped...");
}

std::optional<ss::gate::holder> simple_domain_manager::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return std::nullopt;
    }
    return gate_.hold();
}

ss::future<rpc::add_objects_reply>
simple_domain_manager::add_objects(rpc::add_objects_request req) {
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
simple_domain_manager::replace_objects(rpc::replace_objects_request req) {
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
simple_domain_manager::get_first_offset_ge(
  rpc::get_first_offset_ge_request req) {
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
simple_domain_manager::get_first_timestamp_ge(
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
    auto get_res = simple_metastore::get_first_ge(
      stm_state, req.tp, req.o, req.ts);
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
simple_domain_manager::get_first_offset_for_bytes(
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
simple_domain_manager::get_offsets(rpc::get_offsets_request req) {
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

ss::future<rpc::get_size_reply>
simple_domain_manager::get_size(rpc::get_size_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_size_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_size_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();
    auto get_res = simple_metastore::get_size(stm_state, req.tp);
    if (!get_res.has_value()) {
        co_return rpc::get_size_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_size_reply{
      .ec = rpc::errc::ok,
      .size = get_res->size,
      .num_extents = get_res->num_extents,
    };
}

rpc::get_compaction_info_reply simple_domain_manager::do_get_compaction_info(
  const state& stm_state, rpc::get_compaction_info_request req) {
    auto get_res = simple_metastore::get_compaction_info(
      stm_state, req.tp, req.tombstone_removal_upper_bound_ts);
    if (!get_res.has_value()) {
        return rpc::get_compaction_info_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    return rpc::get_compaction_info_reply{
      .ec = rpc::errc::ok,
      .dirty_ranges = std::move(get_res->offsets_response.dirty_ranges),
      .removable_tombstone_ranges = std::move(
        get_res->offsets_response.removable_tombstone_ranges),
      .dirty_ratio = get_res->dirty_ratio,
      .earliest_dirty_ts = get_res->earliest_dirty_ts,
      .compaction_epoch
      = partition_state::compaction_epoch_t{get_res->compaction_epoch()},
      .start_offset = get_res->start_offset};
}

ss::future<rpc::get_compaction_info_reply>
simple_domain_manager::get_compaction_info(
  rpc::get_compaction_info_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();

    co_return do_get_compaction_info(stm_state, std::move(req));
}

ss::future<rpc::get_term_for_offset_reply>
simple_domain_manager::get_term_for_offset(
  rpc::get_term_for_offset_request req) {
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
simple_domain_manager::get_end_offset_for_term(
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
simple_domain_manager::set_start_offset(rpc::set_start_offset_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::set_start_offset_reply{.ec = rpc::errc::not_leader};
    }

    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::set_start_offset_reply{
          .ec = convert_stm_errc(sync_res.error())};
    }
    bool is_no_op = false;
    auto update_res = set_start_offset_update::build(
      stm_->state(), req.tp, req.start_offset, &is_no_op);
    if (!update_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to set start offset: {}",
          update_res.error());
        co_return rpc::set_start_offset_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    if (is_no_op) {
        vlog(
          cd_log.debug,
          "Request to set {} start offset to {} is no-op",
          req.tp,
          req.start_offset);
        co_return rpc::set_start_offset_reply{.ec = rpc::errc::ok};
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

ss::future<rpc::remove_topics_reply>
simple_domain_manager::remove_topics(rpc::remove_topics_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = rpc::errc::not_leader, .not_removed = {}};
    }

    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = convert_stm_errc(sync_res.error()), .not_removed = {}};
    }
    chunked_vector<model::topic_id> topics_to_remove;
    for (const auto& t : req.topics) {
        if (stm_->state().topic_to_state.contains(t)) {
            topics_to_remove.emplace_back(t);
        }
    }
    // NOTE: even if topics_to_remove is empty, replicate it so we're
    // guaranteed to have been a caught up leader when we build not_removed
    // below. It's possible the sync() call above finished but our state was
    // still stale, e.g. because of a cross-shard movement.
    auto update_res = remove_topics_update::build(
      stm_->state(), std::move(topics_to_remove));
    if (!update_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to remove topics: {}",
          update_res.error());
        co_return rpc::remove_topics_reply{
          .ec = rpc::errc::concurrent_requests,
          .not_removed = {},
        };
    }
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::l1_stm);
    builder.add_record(
      {.key = serde::to_iobuf(remove_topics_update::key),
       .value = serde::to_iobuf(std::move(update_res.value()))});
    auto batch = co_await builder.build();
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(batch), as_);
    if (!repl_res.has_value()) {
        co_return rpc::remove_topics_reply{
          .ec = convert_stm_errc(repl_res.error()),
          .not_removed = {},
        };
    }
    chunked_vector<model::topic_id> not_removed;
    for (const auto& t : req.topics) {
        if (stm_->state().topic_to_state.contains(t)) {
            not_removed.emplace_back(t);
        }
    }
    co_return rpc::remove_topics_reply{
      .ec = rpc::errc::ok,
      .not_removed = std::move(not_removed),
    };
}

ss::future<rpc::get_compaction_infos_reply>
simple_domain_manager::get_compaction_infos(
  rpc::get_compaction_infos_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_compaction_infos_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_compaction_infos_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();

    chunked_hash_map<model::topic_id_partition, rpc::get_compaction_info_reply>
      compaction_infos;
    for (auto& log_req : req.logs) {
        auto log_info = do_get_compaction_info(stm_state, log_req);
        compaction_infos.insert_or_assign(log_req.tp, std::move(log_info));
    }
    co_return rpc::get_compaction_infos_reply{
      .responses = std::move(compaction_infos)};
}

ss::future<rpc::get_extent_metadata_reply>
simple_domain_manager::get_extent_metadata(
  rpc::get_extent_metadata_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }
    auto& stm_state = stm_->state();

    auto get_res = [&]() {
        switch (req.o) {
        case rpc::get_extent_metadata_request::order::forwards:
            return simple_metastore::get_extent_metadata_forwards(
              stm_state,
              req.tp,
              req.min_offset,
              req.max_offset,
              req.max_num_extents,
              metastore::include_object_metadata(req.include_object_metadata));
        case rpc::get_extent_metadata_request::order::backwards:
            return simple_metastore::get_extent_metadata_backwards(
              stm_state,
              req.tp,
              req.min_offset,
              req.max_offset,
              req.max_num_extents);
        }
    }();

    if (!get_res.has_value()) {
        co_return rpc::get_extent_metadata_reply{
          .ec = convert_metastore_errc(get_res.error()),
        };
    }
    co_return rpc::get_extent_metadata_reply{
      .ec = rpc::errc::ok,
      .extents = meta_to_rpc_extent_metadata(std::move(get_res->extents)),
      .end_of_stream = get_res->end_of_stream};
}

ss::future<rpc::preregister_objects_reply>
simple_domain_manager::preregister_objects(
  rpc::preregister_objects_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }

    preregister_objects_update update;
    update.registered_at = model::timestamp::now();
    update.object_ids.reserve(req.count);
    for (uint32_t i = 0; i < req.count; ++i) {
        update.object_ids.push_back(create_object_id());
    }

    chunked_vector<object_id> reply_ids;
    reply_ids.reserve(update.object_ids.size());
    for (const auto& oid : update.object_ids) {
        reply_ids.push_back(oid);
    }

    storage::record_batch_builder builder(
      model::record_batch_type::l1_stm, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(preregister_objects_update::key),
      serde::to_iobuf(std::move(update)));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (!repl_res.has_value()) {
        co_return rpc::preregister_objects_reply{
          .ec = convert_stm_errc(repl_res.error()),
        };
    }

    co_return rpc::preregister_objects_reply{
      .ec = rpc::errc::ok,
      .object_ids = std::move(reply_ids),
    };
}

ss::future<rpc::flush_domain_reply>
simple_domain_manager::flush_domain(rpc::flush_domain_request) {
    // Not supported.
    co_return rpc::flush_domain_reply{.ec = rpc::errc::concurrent_requests};
}

ss::future<rpc::restore_domain_reply>
simple_domain_manager::restore_domain(rpc::restore_domain_request) {
    // Not supported.
    co_return rpc::restore_domain_reply{.ec = rpc::errc::concurrent_requests};
}

ss::future<> simple_domain_manager::gc_loop() {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return;
    }

    // TODO: make configurable.
    garbage_collector gc(stm_.get(), object_io_);
    auto ntp = stm_->raft()->log()->config().ntp();
    while (!as_.abort_requested()) {
        vlog(cd_log.debug, "{} - Running garbage collection now...", ntp);
        auto gc_res = co_await gc.remove_unreferenced_objects(&as_);
        if (!gc_res.has_value()) {
            vlog(cd_log.warn, "Garbage collection failed: {}", gc_res.error());
        }
        auto sleep_interval = gc_interval_();
        vlog(
          cd_log.debug,
          "{} - Re-running garbage collection in {}...",
          ntp,
          sleep_interval);
        try {
            co_await sem_.wait(
              sleep_interval, std::max(sem_.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        } catch (...) {
            auto eptr = std::current_exception();
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

    vlog(cd_log.debug, "{} - Garbage collection loop stopped...", ntp);
}

ss::future<std::expected<database_stats, rpc::errc>>
simple_domain_manager::get_database_stats() {
    // Not implemented.
    co_return std::unexpected(rpc::errc::concurrent_requests);
}

ss::future<std::expected<void, rpc::errc>>
simple_domain_manager::write_debug_rows(chunked_vector<write_batch_row>) {
    co_return std::unexpected(rpc::errc::not_leader);
}

ss::future<std::expected<domain_manager::read_debug_rows_result, rpc::errc>>
simple_domain_manager::read_debug_rows(
  std::optional<ss::sstring>, std::optional<ss::sstring>, uint32_t) {
    co_return std::unexpected(rpc::errc::not_leader);
}

ss::future<
  std::expected<partition_validation_result, partition_validator::error>>
simple_domain_manager::validate_partition(validate_partition_options) {
    co_return std::unexpected(
      partition_validator::error(
        partition_validator::errc::io_error,
        "validate_partition not supported on simple_domain_manager"));
}

} // namespace cloud_topics::l1
