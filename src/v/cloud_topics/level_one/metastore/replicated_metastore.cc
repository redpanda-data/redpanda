/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/replicated_metastore.h"

#include "cloud_topics/level_one/metastore/leader_router.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"

#include <algorithm>

namespace cloud_topics::l1 {

namespace {

// Convert metastore error codes to RPC error codes
metastore::errc rpc_to_meta_errc(rpc::errc ec) {
    switch (ec) {
    case rpc::errc::ok:
        dassert(false, "Unexpected OK code");
        return static_cast<metastore::errc>(0); // Should not be called with ok
    case rpc::errc::not_leader:
        return metastore::errc::transport_error;
    case rpc::errc::incorrect_partition:
    case rpc::errc::concurrent_requests:
        return metastore::errc::invalid_request;
    case rpc::errc::timed_out:
        return metastore::errc::transport_error;
    case rpc::errc::out_of_range:
        return metastore::errc::out_of_range;
    case rpc::errc::missing_ntp:
        return metastore::errc::missing_ntp;
    }
}

new_object meta_to_rpc_obj(const metastore::object_metadata& obj) {
    new_object rpc_obj;
    rpc_obj.oid = obj.oid;
    rpc_obj.footer_pos = obj.footer_pos;
    rpc_obj.object_size = obj.object_size;

    for (const auto& ntp_meta : obj.ntp_metas) {
        auto& topic_map = rpc_obj.extent_metas[ntp_meta.tidp.topic_id];
        new_object::metadata meta;
        meta.base_offset = ntp_meta.base_offset;
        meta.last_offset = ntp_meta.last_offset;
        meta.max_timestamp = ntp_meta.max_timestamp;
        meta.filepos = ntp_meta.pos;
        meta.len = ntp_meta.size;
        topic_map[ntp_meta.tidp.partition] = std::move(meta);
    }

    return rpc_obj;
}

compaction_state_update
meta_to_rpc_compact_update(const metastore::compaction_update& update) {
    compaction_state_update rpc_update;

    if (update.new_cleaned_range) {
        compaction_state_update::cleaned_range range;
        range.base_offset = update.new_cleaned_range->base_offset;
        range.last_offset = update.new_cleaned_range->last_offset;
        range.has_tombstones = update.new_cleaned_range->has_tombstones;
        rpc_update.new_cleaned_range = std::move(range);
    }

    rpc_update.removed_tombstones_ranges = update.removed_tombstones_ranges;
    rpc_update.cleaned_at = update.cleaned_at;

    return rpc_update;
}

// Implementation of the `object_metadata_builder` interface that splits
// objects up by the appropriate metastore topic partition.
class replicated_object_builder : public metastore::object_metadata_builder {
public:
    replicated_object_builder(leader_router& fe)
      : object_metadata_builder()
      , fe_(fe) {}
    ~replicated_object_builder() override {}
    replicated_object_builder(const replicated_object_builder&) = delete;
    replicated_object_builder(replicated_object_builder&&) = delete;
    replicated_object_builder& operator=(const replicated_object_builder&)
      = delete;
    replicated_object_builder& operator=(replicated_object_builder&&) = delete;

    std::expected<object_id, error>
    get_or_create_object_for(const model::topic_id_partition&) override;
    std::expected<void, error> remove_pending_object(object_id) override;
    std::expected<void, error>
      add(object_id, metastore::object_metadata::ntp_metadata) override;
    std::expected<void, error>
    finish(object_id, size_t footer_pos, size_t object_size) override;

private:
    friend class cloud_topics::l1::replicated_metastore;

    struct partitioned_objects {
        chunked_hash_map<
          object_id,
          metastore::object_metadata::ntp_metas_list_t>
          pending_objects_;
        chunked_vector<metastore::object_metadata> finished_objects_;
    };
    leader_router& fe_;
    chunked_hash_map<model::partition_id, partitioned_objects> partitions_;
};

std::expected<object_id, replicated_object_builder::error>
replicated_object_builder::get_or_create_object_for(
  const model::topic_id_partition& tidp) {
    auto metastore_pid = fe_.metastore_partition(tidp);
    if (!metastore_pid) {
        return std::unexpected(
          error{"could not determine metastore partition for "
                "get_or_create_object_for()"});
    }
    auto& partition_objects = partitions_[*metastore_pid];

    if (partition_objects.pending_objects_.empty()) {
        auto oid = create_object_id();
        partition_objects.pending_objects_[oid] = {};
        return oid;
    }
    return partition_objects.pending_objects_.begin()->first;
}

std::expected<void, replicated_object_builder::error>
replicated_object_builder::remove_pending_object(object_id oid) {
    auto p_it = std::ranges::find_if(partitions_, [oid](auto& p) {
        return p.second.pending_objects_.contains(oid);
    });
    if (p_it == partitions_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    auto& [_, objects] = *p_it;
    auto it = objects.pending_objects_.find(oid);
    dassert(
      it != objects.pending_objects_.end(),
      "Pending objects expected to contain {}",
      oid);
    objects.pending_objects_.erase(it);
    if (objects.pending_objects_.empty()) {
        partitions_.erase(p_it);
    }
    return {};
}

std::expected<void, replicated_object_builder::error>
replicated_object_builder::add(
  object_id oid, metastore::object_metadata::ntp_metadata ntp_meta) {
    auto metastore_pid = fe_.metastore_partition(ntp_meta.tidp);
    if (!metastore_pid) {
        return std::unexpected(
          error{"could not determine metastore partition for add()"});
    }
    auto& partition_objects = partitions_[*metastore_pid];
    auto it = partition_objects.pending_objects_.find(oid);
    if (it == partition_objects.pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }

    it->second.push_back(std::move(ntp_meta));
    return {};
}

std::expected<void, replicated_object_builder::error>
replicated_object_builder::finish(
  object_id oid, size_t footer_pos, size_t object_size) {
    auto p_it = std::find_if(
      partitions_.begin(), partitions_.end(), [oid](auto& p) {
          return p.second.pending_objects_.contains(oid);
      });
    if (p_it == partitions_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    auto& [_, objects] = *p_it;
    auto it = objects.pending_objects_.find(oid);
    dassert(
      it != objects.pending_objects_.end(),
      "Pending objects expected to contain {}",
      oid);
    objects.finished_objects_.emplace_back(
      metastore::object_metadata{
        .oid = oid,
        .footer_pos = footer_pos,
        .object_size = object_size,
        .ntp_metas = std::move(it->second),
      });
    objects.pending_objects_.erase(it);

    return {};
}

} // anonymous namespace

replicated_metastore::replicated_metastore(leader_router& fe)
  : fe_(fe) {}

ss::future<std::expected<
  std::unique_ptr<metastore::object_metadata_builder>,
  metastore::errc>>
replicated_metastore::object_builder() {
    auto ensure_fut = co_await ss::coroutine::as_future(
      fe_.ensure_topic_exists());
    if (ensure_fut.failed()) {
        auto ex = ensure_fut.get_exception();
        vlog(cd_log.warn, "Error while ensuring metastore topic: {}", ex);
        co_return std::unexpected(errc::transport_error);
    }
    auto success = ensure_fut.get();
    if (!success) {
        vlog(cd_log.warn, "Ensuring metastore topic did not succeed");
        co_return std::unexpected(errc::transport_error);
    }
    co_return std::make_unique<replicated_object_builder>(fe_);
}

ss::future<std::expected<metastore::offsets_response, metastore::errc>>
replicated_metastore::get_offsets(const model::topic_id_partition& tidp) {
    rpc::get_offsets_request req;
    req.tp = tidp;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_offsets(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();
    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    metastore::offsets_response resp;
    resp.start_offset = reply.start_offset;
    resp.next_offset = reply.next_offset;
    co_return resp;
}

ss::future<std::expected<metastore::add_response, metastore::errc>>
replicated_metastore::add_objects(
  const metastore::object_metadata_builder& builder,
  const metastore::term_offset_map_t& terms) {
    auto& replicated_builder = static_cast<const replicated_object_builder&>(
      builder);

    for (const auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        if (!partition_objects.pending_objects_.empty()) {
            vlog(
              cd_log.error,
              "Error while sending request: unfinished objects remain");
            co_return std::unexpected(metastore::errc::invalid_request);
        }
    }
    chunked_hash_map<model::partition_id, term_state_update_t>
      partitioned_terms;
    for (const auto& [tp, tp_terms] : terms) {
        auto metastore_partition = fe_.metastore_partition(tp);
        if (!metastore_partition) {
            vlog(cd_log.error, "Unable to get metastore partition for {}", tp);
            co_return std::unexpected(errc::transport_error);
        }
        auto& prt_terms = partitioned_terms[*metastore_partition][tp];
        for (const auto& t : tp_terms) {
            prt_terms.emplace_back(
              term_start{.term_id = t.term, .start_offset = t.first_offset});
        }
    }
    add_response resp;
    for (auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        auto terms_it = partitioned_terms.find(partition_id);
        if (terms_it == partitioned_terms.end()) {
            // TODO: consider making this less strict, down to the STM layer?
            vlog(
              cd_log.error,
              "No term metadata routed to partition {}",
              partition_id);
            co_return std::unexpected(errc::invalid_request);
        }
        rpc::add_objects_request req;
        req.metastore_partition = partition_id;
        req.new_terms = std::move(terms_it->second);
        chunked_vector<new_object> new_objects;
        for (auto& obj : partition_objects.finished_objects_) {
            new_objects.emplace_back(meta_to_rpc_obj(obj));
        }
        req.new_objects = std::move(new_objects);
        auto reply_fut = co_await ss::coroutine::as_future(
          fe_.add_objects(std::move(req)));
        if (reply_fut.failed()) {
            auto ex = reply_fut.get_exception();
            vlog(cd_log.warn, "Error while sending request: {}", ex);
            co_return std::unexpected(metastore::errc::transport_error);
        }
        auto reply = reply_fut.get();
        if (reply.ec != rpc::errc::ok) {
            vlog(cd_log.debug, "Error code received for request {}", reply.ec);
            co_return std::unexpected(rpc_to_meta_errc(reply.ec));
        }
        for (const auto& [tp, o] : reply.corrected_next_offsets) {
            resp.corrected_next_offsets[tp] = o;
        }
    }
    co_return resp;
}

ss::future<std::expected<void, metastore::errc>>
replicated_metastore::replace_objects(
  const metastore::object_metadata_builder& builder) {
    auto& replicated_builder = static_cast<const replicated_object_builder&>(
      builder);

    for (const auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        if (!partition_objects.pending_objects_.empty()) {
            vlog(
              cd_log.error,
              "Error while sending request: unfinished objects remain");
            co_return std::unexpected(metastore::errc::invalid_request);
        }
    }
    for (auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        rpc::replace_objects_request req;
        req.metastore_partition = partition_id;
        chunked_vector<new_object> new_objects;
        for (auto& obj : partition_objects.finished_objects_) {
            new_objects.emplace_back(meta_to_rpc_obj(obj));
        }
        req.new_objects = std::move(new_objects);

        // Empty compaction updates for basic replace
        req.compaction_updates = {};
        auto reply_fut = co_await ss::coroutine::as_future(
          fe_.replace_objects(std::move(req)));
        if (reply_fut.failed()) {
            auto ex = reply_fut.get_exception();
            vlog(cd_log.warn, "Error while sending request: {}", ex);
            co_return std::unexpected(metastore::errc::transport_error);
        }
        auto reply = reply_fut.get();
        if (reply.ec != rpc::errc::ok) {
            vlog(
              cd_log.debug,
              "Error code received for request {}",
              int(reply.ec));
            co_return std::unexpected(rpc_to_meta_errc(reply.ec));
        }
    }
    co_return std::expected<void, errc>{};
}

ss::future<std::expected<void, metastore::errc>>
replicated_metastore::set_start_offset(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    rpc::set_start_offset_request req;
    req.tp = tidp;
    req.start_offset = offset;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.set_start_offset(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();
    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    co_return std::expected<void, metastore::errc>{};
}

ss::future<std::expected<metastore::topic_removal_response, metastore::errc>>
replicated_metastore::remove_topics(
  const chunked_vector<model::topic_id>& topics) {
    auto num_metastore_partitions = fe_.num_metastore_partitions();
    if (!num_metastore_partitions.has_value()) {
        vlog(cd_log.warn, "Unable to get num metastore partitions");
        co_return std::unexpected(errc::transport_error);
    }
    static constexpr auto max_rpc_concurrency = 10;
    chunked_hash_set<model::topic_id> not_removed;
    auto fut = co_await ss::coroutine::as_future(
      ss::max_concurrent_for_each(
        std::views::iota(0, *num_metastore_partitions),
        max_rpc_concurrency,
        [this, &topics, &not_removed](int pid) {
            return fe_
              .remove_topics(
                rpc::remove_topics_request{
                  .metastore_partition = model::partition_id{pid},
                  .topics = topics.copy(),
                })
              .then(
                [&not_removed, &topics](const rpc::remove_topics_reply& repl) {
                    if (topics.size() == not_removed.size()) {
                        // Minor optimization: exit early if the set needing
                        // retry is the complete set of topics to remove.
                        return;
                    }
                    if (repl.ec != rpc::errc::ok) {
                        not_removed.insert(topics.begin(), topics.end());
                    }
                    not_removed.insert(
                      repl.not_removed.begin(), repl.not_removed.end());
                });
        }));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(cd_log.warn, "Error while sending topic removal requests: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    co_return topic_removal_response{
      .not_removed = std::move(not_removed),
    };
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
replicated_metastore::get_first_ge(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    rpc::get_first_offset_ge_request req;
    req.tp = tidp;
    req.o = offset;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_first_offset_ge(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();

    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    metastore::object_response resp;
    resp.oid = reply.object.oid;
    resp.footer_pos = reply.object.footer_pos;
    resp.object_size = reply.object.object_size;
    resp.first_offset = reply.object.first_offset;
    resp.last_offset = reply.object.last_offset;
    co_return resp;
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
replicated_metastore::get_first_ge(
  const model::topic_id_partition& tidp, kafka::offset o, model::timestamp ts) {
    rpc::get_first_timestamp_ge_request req;
    req.tp = tidp;
    req.o = o;
    req.ts = ts;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_first_timestamp_ge(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();

    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    metastore::object_response resp;
    resp.oid = reply.object.oid;
    resp.footer_pos = reply.object.footer_pos;
    resp.object_size = reply.object.object_size;
    resp.first_offset = reply.object.first_offset;
    resp.last_offset = reply.object.last_offset;
    co_return resp;
}

ss::future<std::expected<kafka::offset, metastore::errc>>
replicated_metastore::get_first_offset_for_bytes(
  const model::topic_id_partition& tp, uint64_t size) {
    rpc::get_first_offset_for_bytes_request req;
    req.tp = tp;
    req.size = size;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_first_offset_for_bytes(req));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();
    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }
    co_return reply.offset;
}

ss::future<std::expected<model::term_id, metastore::errc>>
replicated_metastore::get_term_for_offset(
  const model::topic_id_partition& tidp, kafka::offset offset) {
    rpc::get_term_for_offset_request req;
    req.tp = tidp;
    req.offset = offset;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_term_for_offset(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();
    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    co_return reply.term;
}

ss::future<std::expected<kafka::offset, metastore::errc>>
replicated_metastore::get_end_offset_for_term(
  const model::topic_id_partition& tidp, model::term_id term) {
    rpc::get_end_offset_for_term_request req;
    req.tp = tidp;
    req.term = term;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_end_offset_for_term(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();
    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    co_return reply.end_offset;
}

ss::future<std::expected<void, metastore::errc>>
replicated_metastore::compact_objects(
  const metastore::object_metadata_builder& builder,
  const metastore::compaction_map_t& compaction_updates) {
    auto& replicated_builder = static_cast<const replicated_object_builder&>(
      builder);

    for (const auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        if (!partition_objects.pending_objects_.empty()) {
            vlog(
              cd_log.error,
              "Error while sending request: unfinished objects remain");
            co_return std::unexpected(metastore::errc::invalid_request);
        }
    }
    chunked_hash_map<
      model::partition_id,
      chunked_hash_map<model::topic_id_partition, compaction_state_update>>
      compaction_updates_by_partition;
    for (auto& [tp, update] : compaction_updates) {
        auto metastore_partition = fe_.metastore_partition(tp);
        if (!metastore_partition) {
            vlog(cd_log.warn, "Unable to get metastore partition for {}", tp);
            co_return std::unexpected(errc::transport_error);
        }
        if (!replicated_builder.partitions_.contains(*metastore_partition)) {
            vlog(
              cd_log.error,
              "Expected objects for partition {}",
              *metastore_partition);
            co_return std::unexpected(errc::invalid_request);
        }
        compaction_updates_by_partition[*metastore_partition].emplace(
          tp, meta_to_rpc_compact_update(update));
    }
    for (auto& [partition_id, partition_objects] :
         replicated_builder.partitions_) {
        rpc::replace_objects_request req;
        req.metastore_partition = partition_id;
        chunked_vector<new_object> new_objects;
        for (auto& obj : partition_objects.finished_objects_) {
            new_objects.emplace_back(meta_to_rpc_obj(obj));
        }
        req.new_objects = std::move(new_objects);

        req.compaction_updates = std::move(
          compaction_updates_by_partition.at(partition_id));
        auto reply_fut = co_await ss::coroutine::as_future(
          fe_.replace_objects(std::move(req)));
        if (reply_fut.failed()) {
            auto ex = reply_fut.get_exception();
            vlog(cd_log.warn, "Error while sending request: {}", ex);
            co_return std::unexpected(metastore::errc::transport_error);
        }
        auto reply = reply_fut.get();
        if (reply.ec != rpc::errc::ok) {
            vlog(
              cd_log.debug,
              "Error code received for request {}",
              int(reply.ec));
            co_return std::unexpected(rpc_to_meta_errc(reply.ec));
        }
    }
    co_return std::expected<void, errc>{};
}

ss::future<
  std::expected<metastore::compaction_offsets_response, metastore::errc>>
replicated_metastore::get_compaction_offsets(
  const model::topic_id_partition& tidp, model::timestamp ts) {
    auto reply_res = co_await get_compaction_info(
      compaction_info_spec{
        .tidp = tidp, .tombstone_removal_upper_bound_ts = ts});

    if (!reply_res.has_value()) {
        co_return std::unexpected(reply_res.error());
    }

    auto reply = std::move(reply_res).value();

    metastore::compaction_offsets_response resp;
    resp.dirty_ranges = std::move(reply.offsets_response.dirty_ranges);
    resp.removable_tombstone_ranges = std::move(
      reply.offsets_response.removable_tombstone_ranges);
    co_return resp;
}

ss::future<std::expected<metastore::compaction_info_response, metastore::errc>>
replicated_metastore::get_compaction_info(const compaction_info_spec& log) {
    rpc::get_compaction_info_request req;
    req.tp = log.tidp;
    req.tombstone_removal_upper_bound_ts = log.tombstone_removal_upper_bound_ts;

    auto reply_fut = co_await ss::coroutine::as_future(
      fe_.get_compaction_info(std::move(req)));
    if (reply_fut.failed()) {
        auto ex = reply_fut.get_exception();
        vlog(cd_log.warn, "Error while sending request: {}", ex);
        co_return std::unexpected(metastore::errc::transport_error);
    }
    auto reply = reply_fut.get();

    if (reply.ec != rpc::errc::ok) {
        co_return std::unexpected(rpc_to_meta_errc(reply.ec));
    }

    metastore::compaction_info_response resp;
    resp.dirty_ratio = reply.dirty_ratio;
    resp.earliest_dirty_ts = reply.earliest_dirty_ts;
    resp.offsets_response = {
      .dirty_ranges = reply.dirty_ranges,
      .removable_tombstone_ranges = reply.removable_tombstone_ranges};

    co_return resp;
}

} // namespace cloud_topics::l1
