/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/metastore.h"

#include "absl/container/btree_map.h"
#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"
#include "cloud_topics/level_one/metastore/lsm/debug_serde.h"
#include "cloud_topics/level_one/metastore/lsm/debug_writer.h"
#include "cloud_topics/level_one/metastore/partition_validator.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {

[[noreturn]] void check_errc(cloud_topics::l1::metastore::errc ec) {
    switch (ec) {
    case cloud_topics::l1::metastore::errc::missing_ntp:
        throw serde::pb::rpc::not_found_exception("missing ntp");
    case cloud_topics::l1::metastore::errc::invalid_request:
        throw serde::pb::rpc::invalid_argument_exception();
    case cloud_topics::l1::metastore::errc::out_of_range:
        throw serde::pb::rpc::out_of_range_exception();
    case cloud_topics::l1::metastore::errc::transport_error:
        throw serde::pb::rpc::unavailable_exception("transport error");
    }
    throw serde::pb::rpc::unknown_exception();
}

} // namespace

seastar::future<proto::admin::metastore::get_offsets_response>
metastore_service_impl::get_offsets(
  serde::pb::rpc::context, proto::admin::metastore::get_offsets_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_offsets_response response;
    auto result = co_await _metastore->local().get_offsets(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    proto::admin::metastore::offsets offsets;
    offsets.set_start_offset(result.value().start_offset());
    offsets.set_next_offset(result.value().next_offset());
    response.set_offsets(std::move(offsets));
    co_return response;
}

seastar::future<proto::admin::metastore::get_size_response>
metastore_service_impl::get_size(
  serde::pb::rpc::context, proto::admin::metastore::get_size_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_size_response response;
    auto result = co_await _metastore->local().get_size(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    response.set_size_bytes(result.value().size);
    co_return response;
}

seastar::future<proto::admin::metastore::get_database_stats_response>
metastore_service_impl::get_database_stats(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::get_database_stats_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    // If we're not leader, reroute.
    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .get_database_stats(std::move(ctx), std::move(req));
    }

    // We're the leader; process locally.
    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }
    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<
          cloud_topics::l1::database_stats,
          cloud_topics::l1::rpc::errc>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->get_database_stats();
      });
    if (!result_exp.has_value()) {
        switch (result_exp.error()) {
        case cloud_topics::l1::rpc::errc::not_leader:
            throw serde::pb::rpc::unavailable_exception("not leader");
        case cloud_topics::l1::rpc::errc::missing_ntp:
            throw serde::pb::rpc::not_found_exception("missing ntp");
        case cloud_topics::l1::rpc::errc::timed_out:
            throw serde::pb::rpc::deadline_exceeded_exception();
        default:
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("error: {}", result_exp.error()));
        }
    }

    auto& result = result_exp.value();
    proto::admin::metastore::get_database_stats_response response;
    response.set_active_memtable_bytes(result.active_memtable_bytes);
    response.set_immutable_memtable_bytes(result.immutable_memtable_bytes);
    response.set_total_size_bytes(result.total_size_bytes);
    for (const auto& level : result.levels) {
        proto::admin::metastore::lsm_level level_proto;
        level_proto.set_level_number(level.level_number);

        for (const auto& file : level.files) {
            proto::admin::metastore::lsm_file file_proto;
            file_proto.set_epoch(file.epoch);
            file_proto.set_id(file.id);
            file_proto.set_size_bytes(file.size_bytes);
            file_proto.set_smallest_key_info(file.smallest_key_info);
            file_proto.set_largest_key_info(file.largest_key_info);
            level_proto.get_files().push_back(std::move(file_proto));
        }

        response.get_levels().push_back(std::move(level_proto));
    }

    co_return response;
}

seastar::future<proto::admin::metastore::write_rows_response>
metastore_service_impl::write_rows(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::write_rows_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .write_rows(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    auto rows_res = cloud_topics::l1::debug_writer::build_rows(req);
    if (!rows_res.has_value()) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("failed to build rows: {}", rows_res.error()));
    }
    auto num_rows = rows_res->size();

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp, rows = std::move(*rows_res)](
        this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<void, cloud_topics::l1::rpc::errc>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->write_debug_rows(std::move(rows));
      });
    if (!result_exp.has_value()) {
        switch (result_exp.error()) {
        case cloud_topics::l1::rpc::errc::not_leader:
            throw serde::pb::rpc::unavailable_exception("not leader");
        case cloud_topics::l1::rpc::errc::missing_ntp:
            throw serde::pb::rpc::not_found_exception("missing ntp");
        case cloud_topics::l1::rpc::errc::timed_out:
            throw serde::pb::rpc::deadline_exceeded_exception();
        default:
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("error: {}", result_exp.error()));
        }
    }

    proto::admin::metastore::write_rows_response response;
    response.set_rows_written(static_cast<uint32_t>(num_rows));
    co_return response;
}

seastar::future<proto::admin::metastore::read_rows_response>
metastore_service_impl::read_rows(
  serde::pb::rpc::context ctx, proto::admin::metastore::read_rows_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .read_rows(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    // Resolve seek key.
    std::optional<ss::sstring> seek_key;
    if (req.has_raw_seek_key()) {
        seek_key = req.get_raw_seek_key();
    } else if (req.has_seek_key()) {
        auto enc = cloud_topics::l1::debug_encode_key(req.get_seek_key());
        if (!enc.has_value()) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("invalid seek_key: {}", enc.error()));
        }
        seek_key = std::move(*enc);
    }

    // Resolve last key.
    std::optional<ss::sstring> last_key;
    if (req.has_raw_last_key()) {
        last_key = req.get_raw_last_key();
    } else if (req.has_last_key()) {
        auto enc = cloud_topics::l1::debug_encode_key(req.get_last_key());
        if (!enc.has_value()) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("invalid last_key: {}", enc.error()));
        }
        last_key = std::move(*enc);
    }

    uint32_t max_rows = req.get_max_rows();
    if (max_rows == 0) {
        max_rows = 100;
    }

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp,
       seek = std::move(seek_key),
       last = std::move(last_key),
       max_rows](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<
          cloud_topics::l1::domain_manager::read_debug_rows_result,
          cloud_topics::l1::rpc::errc>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->read_debug_rows(
            std::move(seek), std::move(last), max_rows);
      });
    if (!result_exp.has_value()) {
        switch (result_exp.error()) {
        case cloud_topics::l1::rpc::errc::not_leader:
            throw serde::pb::rpc::unavailable_exception("not leader");
        case cloud_topics::l1::rpc::errc::missing_ntp:
            throw serde::pb::rpc::not_found_exception("missing ntp");
        case cloud_topics::l1::rpc::errc::timed_out:
            throw serde::pb::rpc::deadline_exceeded_exception();
        default:
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("error: {}", result_exp.error()));
        }
    }

    auto& result = result_exp.value();
    auto resp_res = cloud_topics::l1::debug_reader::build_response(
      result.rows, std::move(result.next_key));
    if (!resp_res.has_value()) {
        throw serde::pb::rpc::internal_exception(
          fmt::format("decode error: {}", resp_res.error()));
    }
    co_return std::move(*resp_res);
}

seastar::future<proto::admin::metastore::validate_partition_response>
metastore_service_impl::validate_partition(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::validate_partition_request req) {
    model::topic_id topic_id;

    try {
        topic_id = model::topic_id{uuid_t::from_string(req.get_topic_id())};
    } catch (...) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("invalid topic id: {}", req.get_topic_id()));
    }
    auto partition_id = model::partition_id{req.get_partition_id()};
    auto tidp = model::topic_id_partition{topic_id, partition_id};

    // Resolve the metastore partition from the topic-partition.
    auto mp = _leader_router->local().metastore_partition(tidp);
    if (!mp) {
        throw serde::pb::rpc::unavailable_exception(
          "cannot resolve metastore partition");
    }
    model::ntp metastore_ntp{
      model::kafka_internal_namespace, model::l1_metastore_topic, *mp};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .validate_partition(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    cloud_topics::l1::validate_partition_options opts{
      .tidp = tidp,
      .check_object_metadata = req.get_check_object_metadata(),
      .check_object_storage = req.get_check_object_storage(),
      .resume_at_offset = req.has_resume_at_offset()
                            ? std::optional{kafka::offset{
                                req.get_resume_at_offset()}}
                            : std::nullopt,
      .max_extents = req.get_max_extents(),
    };

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp, opts](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<
          cloud_topics::l1::partition_validation_result,
          cloud_topics::l1::partition_validator::error>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              throw serde::pb::rpc::unavailable_exception("not leader");
          }
          co_return co_await dm->validate_partition(opts);
      });

    if (!result_exp.has_value()) {
        throw serde::pb::rpc::unavailable_exception(
          fmt::format("validation error: {}", result_exp.error()));
    }

    auto& result = result_exp.value();
    proto::admin::metastore::validate_partition_response response;
    for (auto& anomaly : result.anomalies) {
        proto::admin::metastore::metastore_anomaly a;
        a.set_anomaly_type(
          static_cast<proto::admin::metastore::anomaly_type>(anomaly.type));
        a.set_description(std::move(anomaly.description));
        response.get_anomalies().push_back(std::move(a));
    }
    if (result.resume_at_offset.has_value()) {
        response.set_resume_at_offset((*result.resume_at_offset)());
    }
    response.set_extents_validated(result.extents_validated);
    co_return response;
}

seastar::future<proto::admin::metastore::list_cloud_topics_response>
metastore_service_impl::list_cloud_topics(
  serde::pb::rpc::context,
  proto::admin::metastore::list_cloud_topics_request req) {
    auto after = ss::sstring(req.get_after_topic_name());
    uint32_t max_topics = req.get_max_topics();
    if (max_topics == 0) {
        max_topics = 100;
    }

    const auto& topics_metadata = _topic_table->local().all_topics_metadata();
    absl::
      btree_map<ss::sstring, const cluster::topic_table::topic_metadata_item*>
        cloud_topics;
    for (const auto& [tn, item] : topics_metadata) {
        const auto& cfg = item.get_configuration();
        if (
          !cfg.is_cloud_topic() || cfg.is_read_replica()
          || !cfg.tp_id.has_value()) {
            continue;
        }
        if (!after.empty() && tn.tp() <= after) {
            continue;
        }
        cloud_topics.emplace(tn.tp(), &item);
    }

    proto::admin::metastore::list_cloud_topics_response response;
    uint32_t count = 0;
    for (const auto& [name, item] : cloud_topics) {
        if (count >= max_topics) {
            response.set_has_more(true);
            break;
        }
        const auto& cfg = item->get_configuration();
        proto::admin::metastore::cloud_topic_info info;
        info.set_topic_name(ss::sstring(name));
        info.set_topic_id(ss::sstring(cfg.tp_id.value()()));
        info.set_partition_count(cfg.partition_count);
        response.get_topics().push_back(std::move(info));
        ++count;
    }
    co_return response;
}

} // namespace admin
