/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/partition_manager.h"
#include "cluster/tx_gateway_frontend.h"
#include "container/lw_shared_container.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/server.h"
#include "redpanda/admin/api-doc/transaction.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/json/json_elements.hh>

void admin_server::register_transaction_routes() {
    register_route<user>(
      ss::httpd::transaction_json::get_all_transactions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_all_transactions_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::transaction_json::delete_partition,
      [this](std::unique_ptr<ss::http::request> req) {
          return delete_partition_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::transaction_json::find_coordinator,
      [this](std::unique_ptr<ss::http::request> req) {
          return find_tx_coordinator_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::transaction_json::unsafe_abort_group_transaction,
      [this](std::unique_ptr<ss::http::request> req) {
          return unsafe_abort_group_transaction(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_all_transactions_handler(
  std::unique_ptr<ss::http::request> req) {
    if (!config::shard_local_cfg().enable_transactions) {
        throw ss::httpd::bad_request_exception("Transaction are disabled");
    }

    auto coordinator_partition_str = req->get_query_param(
      "coordinator_partition_id");
    int64_t coordinator_partition;
    try {
        coordinator_partition = std::stoi(coordinator_partition_str);
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Partition must be an integer: {}", coordinator_partition_str));
    }

    if (coordinator_partition < 0) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Invalid coordinator partition {}", coordinator_partition));
    }

    model::ntp tx_ntp(
      model::tx_manager_nt.ns,
      model::tx_manager_nt.tp,
      model::partition_id(coordinator_partition));

    if (need_redirect_to_leader(tx_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, tx_ntp);
    }

    if (!_tx_gateway_frontend.local_is_initialized()) {
        throw ss::httpd::bad_request_exception("Can not get tx_frontend");
    }

    auto res = co_await _tx_gateway_frontend.local()
                 .get_all_transactions_for_one_tx_partition(tx_ntp);
    if (!res.has_value()) {
        co_await throw_on_error(*req, res.error(), tx_ntp);
    }

    using tx_info = ss::httpd::transaction_json::transaction_summary;
    fragmented_vector<tx_info> ans;
    ans.reserve(res.value().size());

    for (auto& tx : res.value()) {
        if (tx.status == cluster::tx_status::tombstone) {
            continue;
        }

        tx_info new_tx;

        new_tx.transactional_id = tx.id;

        ss::httpd::transaction_json::producer_identity pid;
        pid.id = tx.pid.id;
        pid.epoch = tx.pid.epoch;
        new_tx.pid = pid;

        new_tx.tx_seq = tx.tx_seq;
        new_tx.etag = tx.etag;

        // The motivation behind mapping expired to preparing_abort is to make
        // user not to think about the subtle differences between both
        // statuses
        if (tx.status == cluster::tx_status::preparing_internal_abort) {
            tx.status = cluster::tx_status::preparing_abort;
        }
        new_tx.status = ss::sstring(tx.get_status());

        new_tx.timeout_ms = tx.get_timeout().count();
        new_tx.staleness_ms = tx.get_staleness().count();

        for (auto& partition : tx.partitions) {
            ss::httpd::transaction_json::partition partition_info;
            partition_info.ns = partition.ntp.ns;
            partition_info.topic = partition.ntp.tp.topic;
            partition_info.partition_id = partition.ntp.tp.partition;
            partition_info.etag = partition.etag;

            new_tx.partitions.push(partition_info);
        }

        for (auto& group : tx.groups) {
            ss::httpd::transaction_json::group group_info;
            group_info.group_id = group.group_id;
            group_info.etag = group.etag;

            new_tx.groups.push(group_info);
        }

        ans.push_back(std::move(new_tx));
        co_await ss::coroutine::maybe_yield();
    }

    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      lw_shared_container(std::move(ans)),
      [](auto& tx_info) { return tx_info; }));
}

ss::future<ss::json::json_return_type>
admin_server::find_tx_coordinator_handler(
  std::unique_ptr<ss::http::request> req) {
    auto transaction_id = req->get_path_param("transactional_id");
    kafka::transactional_id tid(transaction_id);
    auto r = co_await _tx_gateway_frontend.local().find_coordinator(tid);

    ss::httpd::transaction_json::find_coordinator_reply reply;
    if (r.coordinator) {
        reply.coordinator = r.coordinator.value()();
    }
    if (r.ntp) {
        ss::httpd::transaction_json::ntp ntp;
        ntp.ns = r.ntp->ns();
        ntp.topic = r.ntp->tp.topic();
        ntp.partition = r.ntp->tp.partition();
        reply.ntp = ntp;
    }
    reply.ec = static_cast<int>(r.ec);

    co_return ss::json::json_return_type(std::move(reply));
}

ss::future<ss::json::json_return_type>
admin_server::delete_partition_handler(std::unique_ptr<ss::http::request> req) {
    if (!_tx_gateway_frontend.local_is_initialized()) {
        throw ss::httpd::bad_request_exception("Transaction are disabled");
    }
    auto transaction_id = req->get_path_param("transactional_id");
    kafka::transactional_id tid(transaction_id);

    auto r = co_await _tx_gateway_frontend.local().find_coordinator(tid);
    if (!r.ntp) {
        throw ss::httpd::bad_request_exception("Coordinator not available");
    }
    if (need_redirect_to_leader(*r.ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, *r.ntp);
    }

    auto tx_ntp = _tx_gateway_frontend.local().ntp_for_tx_id(tid);
    if (!tx_ntp) {
        throw ss::httpd::bad_request_exception("Coordinator not available");
    }

    auto ntp = parse_ntp_from_query_param(req);

    auto etag_str = req->get_query_param("etag");
    int64_t etag;
    try {
        etag = std::stoi(etag_str);
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Etag must be an integer: {}", etag_str));
    }

    if (etag < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid etag {}", etag));
    }

    cluster::tx_metadata::tx_partition partition_for_delete{
      .ntp = ntp, .etag = model::term_id(etag)};

    vlog(
      adminlog.info,
      "Delete partition(ntp: {}, etag: {}) from transaction({})",
      ntp,
      etag,
      tid);

    auto res = co_await _tx_gateway_frontend.local().delete_partition_from_tx(
      tid, partition_for_delete);
    co_await throw_on_error(*req, res, ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::unsafe_abort_group_transaction(
  std::unique_ptr<ss::http::request> request) {
    if (!_tx_gateway_frontend.local_is_initialized()) {
        throw ss::httpd::bad_request_exception("Transaction are disabled");
    }

    auto group_id = request->get_path_param("group_id");
    auto pid_str = request->get_query_param("producer_id");
    auto epoch_str = request->get_query_param("producer_epoch");
    auto sequence_str = request->get_query_param("sequence");

    if (group_id.empty()) {
        throw ss::httpd::bad_param_exception("group_id cannot be empty");
    }

    if (pid_str.empty() || epoch_str.empty() || sequence_str.empty()) {
        throw ss::httpd::bad_param_exception(
          "invalid producer_id/epoch, should be >= 0");
    }

    std::optional<model::producer_id> pid;
    try {
        auto parsed_pid = boost::lexical_cast<model::producer_id::type>(
          pid_str);
        pid = model::producer_id{parsed_pid};
    } catch (const boost::bad_lexical_cast& e) {
        throw ss::httpd::bad_param_exception(
          fmt::format("invalid producer_id, should be >= 0: {}", e));
    }

    std::optional<model::producer_epoch> epoch;
    try {
        auto parsed_epoch = boost::lexical_cast<model::producer_epoch::type>(
          epoch_str);
        epoch = model::producer_epoch{parsed_epoch};
    } catch (const boost::bad_lexical_cast& e) {
        throw ss::httpd::bad_param_exception(
          fmt::format("invalid producer_epoch, should be >= 0: {}", e));
    }

    std::optional<model::tx_seq> seq;
    try {
        auto parsed_seq = boost::lexical_cast<model::tx_seq::type>(
          sequence_str);
        seq = model::tx_seq{parsed_seq};
    } catch (const boost::bad_lexical_cast& e) {
        throw ss::httpd::bad_param_exception(
          fmt::format("invalid transaction sequence, should be >= 0: {}", e));
    }

    auto& mapper = _kafka_server.local().coordinator_mapper();
    auto kafka_gid = kafka::group_id{group_id};
    auto group_ntp = mapper.ntp_for(kafka::group_id{group_id});
    if (!group_ntp) {
        throw ss::httpd::server_error_exception(
          "consumer_offsets topic now found");
    }

    auto result
      = co_await _tx_gateway_frontend.local().unsafe_abort_group_transaction(
        std::move(kafka_gid),
        model::producer_identity{pid.value(), epoch.value()},
        seq.value(),
        5s);

    co_await throw_on_error(*request, result, group_ntp.value());
    co_return ss::json::json_return_type(ss::json::json_void());
}
