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

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_stm.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "container/fragmented_vector.h"
#include "container/lw_shared_container.h"
#include "model/record.h"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "redpanda/admin/api-doc/partition.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"

#include <seastar/json/json_elements.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>

using admin::apply_validator;

ss::future<ss::json::json_return_type>
admin_server::get_transactions_handler(std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    auto shard = _shard_table.local().shard_for(ntp);
    // Strange situation, but need to check it
    if (!shard) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not find shard for partition {}", ntp.tp));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [ntp = std::move(ntp), req = std::move(req), this](
        cluster::partition_manager& pm) mutable {
          return get_transactions_inner_handler(
            pm, std::move(ntp), std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_transactions_inner_handler(
  cluster::partition_manager& pm,
  model::ntp ntp,
  std::unique_ptr<ss::http::request> req) {
    auto partition = pm.get(ntp);
    if (!partition) {
        throw ss::httpd::server_error_exception(
          fmt_with_ctx(fmt::format, "Can not find partition {}", partition));
    }

    auto rm_stm_ptr = partition->rm_stm();

    if (!rm_stm_ptr) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not get rm_stm for partition {}", partition));
    }

    auto transactions = co_await rm_stm_ptr->get_transactions();

    if (transactions.has_error()) {
        co_await throw_on_error(*req, transactions.error(), ntp);
    }
    ss::httpd::partition_json::transactions ans;

    auto log = partition->log();

    for (auto& [id, tx_info] : transactions.value()) {
        ss::httpd::partition_json::producer_identity pid;
        pid.id = id.get_id();
        pid.epoch = id.get_epoch();

        ss::httpd::partition_json::transaction new_tx;
        new_tx.producer_id = pid;
        new_tx.status = ss::sstring(tx_info.get_status());

        new_tx.lso_bound = log->from_log_offset(tx_info.lso_bound);

        auto staleness = tx_info.get_staleness();
        // -1 is returned for expired transaction, because how
        // long transaction do not do progress is useless for
        // expired tx.
        new_tx.staleness_ms
          = staleness.has_value()
              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  staleness.value())
                  .count()
              : -1;
        auto timeout = tx_info.get_timeout();
        // -1 is returned for expired transaction, because
        // timeout is useless for expired tx.
        new_tx.timeout_ms
          = timeout.has_value()
              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  timeout.value())
                  .count()
              : -1;

        if (tx_info.is_expired()) {
            ans.expired_transactions.push(new_tx);
        } else {
            ans.active_transactions.push(new_tx);
        }
    }

    co_return ss::json::json_return_type(ans);
}

ss::future<ss::json::json_return_type>
admin_server::mark_transaction_expired_handler(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);

    model::producer_identity pid;
    auto param = req->get_query_param("id");
    try {
        pid.id = boost::lexical_cast<model::producer_id>(param);
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt_with_ctx(fmt::format, "Invalid producer id: {}", param));
    }
    param = req->get_query_param("epoch");
    try {
        pid.epoch = boost::lexical_cast<model::producer_epoch>(param);
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt_with_ctx(fmt::format, "Invalid producer epoch: {}", param));
    }

    vlog(
      adminlog.info,
      "Mark transaction expired for partition: {}, pid:{}",
      ntp,
      pid);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    auto shard = _shard_table.local().shard_for(ntp);
    // Strange situation, but need to check it
    if (!shard) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not find shard for partition {}", ntp.tp));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [_ntp = std::move(ntp), pid, _req = std::move(req), this](
        cluster::partition_manager& pm) mutable
      -> ss::future<ss::json::json_return_type> {
          auto ntp = std::move(_ntp);
          auto req = std::move(_req);
          auto partition = pm.get(ntp);
          if (!partition) {
              return ss::make_exception_future<ss::json::json_return_type>(
                ss::httpd::server_error_exception(fmt_with_ctx(
                  fmt::format, "Can not find partition {}", partition)));
          }

          auto rm_stm_ptr = partition->rm_stm();

          if (!rm_stm_ptr) {
              return ss::make_exception_future<ss::json::json_return_type>(
                ss::httpd::server_error_exception(fmt_with_ctx(
                  fmt::format,
                  "Can not get rm_stm for partition {}",
                  partition)));
          }

          return rm_stm_ptr->mark_expired(pid).then(
            [this, ntp, req = std::move(req)](auto res) {
                return throw_on_error(*req, std::error_code{res}, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_reconfigurations_handler(std::unique_ptr<ss::http::request>) {
    using reconfiguration = ss::httpd::partition_json::reconfiguration;

    auto& in_progress
      = _controller->get_topics_state().local().updates_in_progress();

    std::vector<model::ntp> ntps;
    ntps.reserve(in_progress.size());

    for (auto& [ntp, status] : in_progress) {
        ntps.push_back(ntp);
    }

    const auto deadline = model::timeout_clock::now() + 5s;

    auto [reconfiguration_states, reconciliations]
      = co_await ss::when_all_succeed(
        _controller->get_api().local().get_partitions_reconfiguration_state(
          ntps, deadline),
        _controller->get_api().local().get_global_reconciliation_state(
          ntps, deadline));

    if (reconfiguration_states.has_error()) {
        vlog(
          adminlog.info,
          "unable to get reconfiguration status: {}({})",
          reconfiguration_states.error().message(),
          reconfiguration_states.error());
        throw ss::httpd::base_exception(
          fmt::format(
            "unable to get reconfiguration status: {}({})",
            reconfiguration_states.error().message(),
            reconfiguration_states.error()),
          ss::http::reply::status_type::service_unavailable);
    }
    // we are forced to use shared pointer as underlying chunked_fifo is not
    // copyable
    auto reconciliations_ptr
      = ss::make_lw_shared<cluster::global_reconciliation_state>(
        std::move(reconciliations));
    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      std::move(reconfiguration_states.value()),
      [reconciliations = std::move(reconciliations_ptr)](auto& s) {
          reconfiguration r;
          r.ns = s.ntp.ns;
          r.topic = s.ntp.tp.topic;
          r.partition = s.ntp.tp.partition;

          for (const model::broker_shard& bs : s.current_assignment) {
              ss::httpd::partition_json::assignment assignment;
              assignment.core = bs.shard;
              assignment.node_id = bs.node_id;
              r.current_replicas.push(assignment);
          }

          for (const model::broker_shard& bs : s.previous_assignment) {
              ss::httpd::partition_json::assignment assignment;
              assignment.core = bs.shard;
              assignment.node_id = bs.node_id;
              r.previous_replicas.push(assignment);
          }

          size_t left_to_move = 0;
          size_t already_moved = 0;
          for (auto replica_status : s.already_transferred_bytes) {
              left_to_move += (s.current_partition_size - replica_status.bytes);
              already_moved += replica_status.bytes;
          }
          r.bytes_left_to_move = left_to_move;
          r.bytes_moved = already_moved;
          r.partition_size = s.current_partition_size;
          // if no information from partitions is present yet, we may indicate
          // that everything have to be moved
          if (already_moved == 0 && left_to_move == 0) {
              r.bytes_left_to_move = s.current_partition_size;
          }
          r.reconfiguration_policy = ssx::sformat("{}", s.policy);
          auto it = reconciliations->ntp_backend_operations.find(s.ntp);
          if (it != reconciliations->ntp_backend_operations.end()) {
              for (auto& node_ops : it->second) {
                  seastar::httpd::partition_json::
                    partition_reconciliation_status per_node_status;
                  per_node_status.node_id = node_ops.node_id;

                  for (auto& op : node_ops.backend_operations) {
                      auto& current_op = node_ops.backend_operations.front();
                      seastar::httpd::partition_json::
                        partition_reconciliation_operation r_op;
                      r_op.core = op.source_shard;
                      r_op.retry_number = current_op.current_retry;
                      r_op.revision = current_op.revision_of_operation;
                      r_op.status = fmt::format(
                        "{} ({})",
                        cluster::error_category().message(
                          (int)current_op.last_operation_result),
                        current_op.last_operation_result);
                      r_op.type = fmt::format("{}", current_op.type);
                      per_node_status.operations.push(r_op);
                  }
                  r.reconciliation_statuses.push(per_node_status);
              }
          }
          return r;
      }));
}

ss::future<ss::json::json_return_type>
admin_server::cancel_partition_reconfig_handler(
  std::unique_ptr<ss::http::request> req) {
    const auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't cancel controller reconfiguration"));
    }
    vlog(
      adminlog.debug,
      "Requesting cancelling of {} partition reconfiguration",
      ntp);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .cancel_moving_partition_replicas(
                   ntp,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::unclean_abort_partition_reconfig_handler(
  std::unique_ptr<ss::http::request> req) {
    const auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          "Can't unclean abort controller reconfiguration");
    }
    vlog(
      adminlog.warn,
      "Requesting unclean abort of {} partition reconfiguration",
      ntp);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .abort_moving_partition_replicas(
                   ntp,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

namespace {

json::validator make_set_replicas_validator() {
    const std::string schema = R"(
{
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "node_id": {
                "type": "number"
            },
            "core": {
                "type": "number"
            }
        },
        "required": [
            "node_id",
            "core"
        ],
        "additionalProperties": false
    }
}
)";
    return json::validator(schema);
}

ss::future<std::vector<model::broker_shard>> validate_set_replicas(
  const json::Document& doc, const cluster::topics_frontend& topic_fe) {
    static thread_local json::validator set_replicas_validator(
      make_set_replicas_validator());

    apply_validator(set_replicas_validator, doc);

    std::vector<model::broker_shard> replicas;
    if (!doc.IsArray()) {
        throw ss::httpd::bad_request_exception("Expected array");
    }
    for (auto& r : doc.GetArray()) {
        const auto& node_id_json = r["node_id"];
        const auto& core_json = r["core"];
        if (!node_id_json.IsInt() || !core_json.IsInt()) {
            throw ss::httpd::bad_request_exception(
              "`node_id` and `core` must be integers");
        }
        const auto node_id = model::node_id(r["node_id"].GetInt());
        uint32_t shard = 0;
        if (topic_fe.node_local_core_assignment_enabled()) {
            bool is_valid = co_await topic_fe.validate_shard(node_id, 0);
            if (!is_valid) {
                throw ss::httpd::bad_request_exception(fmt::format(
                  "Replica set refers to non-existent node {}", node_id));
            }
#ifndef NDEBUG
            // set invalid shard in debug mode so that we can spot places
            // where we are still using it.
            shard = 32132132;
#endif
        } else {
            shard = static_cast<uint32_t>(r["core"].GetInt());
            // Validate node ID and shard - subsequent code assumes
            // they exist and may assert if not.
            bool is_valid = co_await topic_fe.validate_shard(node_id, shard);
            if (!is_valid) {
                throw ss::httpd::bad_request_exception(fmt::format(
                  "Replica set refers to non-existent node/shard "
                  "(node {} shard {})",
                  node_id,
                  shard));
            }
        }

        auto contains_already = std::find_if(
                                  replicas.begin(),
                                  replicas.end(),
                                  [node_id](const model::broker_shard& bs) {
                                      return bs.node_id == node_id;
                                  })
                                != replicas.end();
        if (contains_already) {
            throw ss::httpd::bad_request_exception(fmt::format(
              "All the replicas must be placed on separate nodes. "
              "Requested replica set contains node: {} more than "
              "once",
              node_id));
        }
        replicas.push_back(
          model::broker_shard{.node_id = node_id, .shard = shard});
    }
    co_return replicas;
}

} // namespace

ss::future<ss::json::json_return_type>
admin_server::force_set_partition_replicas_handler(
  std::unique_ptr<ss::http::request> req) {
    if (unlikely(!_controller->get_feature_table().local().is_active(
          features::feature::force_partition_reconfiguration))) {
        throw ss::httpd::bad_request_exception(
          "Feature not active yet, upgrade in progress?");
    }

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto ntp = parse_ntp_from_request(req->param);
    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't reconfigure a controller"));
    }

    auto doc = co_await parse_json_body(req.get());
    auto replicas = co_await validate_set_replicas(
      doc, _controller->get_topics_frontend().local());

    const auto& topics = _controller->get_topics_state().local();
    const auto& in_progress = topics.updates_in_progress();
    const auto in_progress_it = in_progress.find(ntp);

    if (in_progress_it != in_progress.end()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("A partition operation is in progress. Check "
                      "reconfigurations and "
                      "cancel in flight update before issuing force "
                      "replica set update."));
    }
    const auto current_assignment = topics.get_partition_assignment(ntp);
    if (current_assignment) {
        const auto& current_replicas = current_assignment->replicas;
        if (current_replicas == replicas) {
            vlog(
              adminlog.info,
              "Request to change ntp {} replica set to {}, no change",
              ntp,
              replicas);
            co_return ss::json::json_void();
        }
        auto relax_restrictions
          = _controller->get_feature_table().local().is_active(
            features::feature::enhanced_force_reconfiguration);
        if (
          !relax_restrictions
          && !cluster::is_proper_subset(replicas, current_replicas)) {
            throw ss::httpd::bad_request_exception(fmt::format(
              "Target assignment {} is not a proper subset of current {}, "
              "choose a proper subset of existing replicas.",
              replicas,
              current_replicas));
        }
    }

    vlog(
      adminlog.info,
      "Request to force update ntp {} replica set to {}",
      ntp,
      replicas);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .force_update_partition_replicas(
                   ntp,
                   replicas,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    vlog(
      adminlog.debug,
      "Request to change ntp {} replica set to {}: err={}",
      ntp,
      replicas,
      err);

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::set_partition_replicas_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't reconfigure a controller"));
    }

    auto doc = co_await parse_json_body(req.get());
    auto replicas = co_await validate_set_replicas(
      doc, _controller->get_topics_frontend().local());
    auto current_assignment
      = _controller->get_topics_state().local().get_partition_assignment(ntp);

    // For a no-op change, just return success here, to avoid doing
    // all the raft writes and consensus restarts for a config change
    // that will do nothing.
    if (current_assignment && current_assignment->replicas == replicas) {
        vlog(
          adminlog.info,
          "Request to change ntp {} replica set to {}, no change",
          ntp,
          replicas);
        co_return ss::json::json_void();
    }

    vlog(
      adminlog.info,
      "Request to change ntp {} replica set to {}",
      ntp,
      replicas);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .move_partition_replicas(
                   ntp,
                   replicas,
                   cluster::reconfiguration_policy::full_local_retention,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    vlog(
      adminlog.debug,
      "Request to change ntp {} replica set to {}: err={}",
      ntp,
      replicas,
      err);

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

namespace {

json::validator make_set_replica_core_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "core": {
            "type": "number"
        }
    },
    "required": [
        "core"
    ],
    "additionalProperties": false
}
)";
    return json::validator(schema);
}

uint32_t parse_replica_core_from_json(const json::Document& doc) {
    static thread_local json::validator json_validator(
      make_set_replica_core_validator());
    apply_validator(json_validator, doc);

    if (!doc.IsObject()) {
        throw ss::httpd::bad_request_exception("Expected array");
    }
    const auto& core_field = doc["core"];
    if (!core_field.IsInt()) {
        throw ss::httpd::bad_request_exception("`core` must be integer");
    }
    return uint32_t(core_field.GetInt());
}

} // namespace

ss::future<ss::json::json_return_type>
admin_server::set_partition_replica_core_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ntp = parse_ntp_from_request(req->param);

    auto node_str = req->param.get_decoded_param("node");
    model::node_id node_id;
    try {
        node_id = model::node_id(std::stoi(node_str));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("node parameter must be an integer: `{}'", node_str));
    }

    auto doc = co_await parse_json_body(req.get());
    uint32_t core = parse_replica_core_from_json(doc);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't modify controller partition core"));
    }

    auto ec = co_await _controller->get_topics_frontend()
                .local()
                .set_partition_replica_shard(
                  ntp, node_id, core, model::timeout_clock::now() + 5s);
    co_await throw_on_error(*req, ec, ntp);
    co_return ss::json::json_void();
}

void admin_server::register_partition_routes() {
    /*
     * Get a list of partition summaries.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partitions,
      [this](std::unique_ptr<ss::http::request>) {
          using summary = ss::httpd::partition_json::partition_summary;
          auto get_summaries =
            [](auto& partition_manager, bool materialized, auto get_leader) {
                return partition_manager.map_reduce0(
                  [materialized, get_leader](auto& pm) {
                      fragmented_vector<summary> partitions;
                      for (const auto& it : pm.partitions()) {
                          summary p;
                          p.ns = it.first.ns;
                          p.topic = it.first.tp.topic;
                          p.partition_id = it.first.tp.partition;
                          p.core = ss::this_shard_id();
                          p.materialized = materialized;
                          p.leader = get_leader(it.second);
                          partitions.push_back(std::move(p));
                      }
                      return partitions;
                  },
                  fragmented_vector<summary>{},
                  [](
                    fragmented_vector<summary> acc,
                    fragmented_vector<summary> update) {
                      std::move(
                        std::make_move_iterator(update.begin()),
                        std::make_move_iterator(update.end()),
                        std::back_inserter(acc));
                      return acc;
                  });
            };
          return get_summaries(
                   _partition_manager,
                   false,
                   [](const auto& p) {
                       return p->get_leader_id().value_or(model::node_id(-1))();
                   })
            .then([](auto partitions) mutable {
                return ss::json::json_return_type(
                  ss::json::stream_range_as_array(
                    lw_shared_container{std::move(partitions)},
                    [](const summary& i) -> const summary& { return i; }));
            });
      });

    register_route<user>(
      ss::httpd::partition_json::get_partitions_local_summary,
      [this](std::unique_ptr<ss::http::request>) {
          // This type mirrors partitions_local_summary, but satisfies
          // the seastar map_reduce requirement of being nothrow move
          // constructible.
          struct summary_t {
              uint64_t count{0};
              uint64_t leaderless{0};
              uint64_t under_replicated{0};
          };

          return _partition_manager
            .map_reduce0(
              [](auto& pm) {
                  summary_t s;
                  for (const auto& it : pm.partitions()) {
                      s.count += 1;
                      if (it.second->get_leader_id() == std::nullopt) {
                          s.leaderless += 1;
                      }
                      if (it.second->get_under_replicated() == std::nullopt) {
                          s.under_replicated += 1;
                      }
                  }
                  return s;
              },
              summary_t{},
              [](summary_t acc, summary_t update) {
                  acc.count += update.count;
                  acc.leaderless += update.leaderless;
                  acc.under_replicated += update.under_replicated;
                  return acc;
              })
            .then([](summary_t summary) {
                ss::httpd::partition_json::partitions_local_summary result;
                result.count = summary.count;
                result.leaderless = summary.leaderless;
                result.under_replicated = summary.under_replicated;
                return ss::json::json_return_type(std::move(result));
            });
      });
    register_route<user>(
      ss::httpd::partition_json::get_topic_partitions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_topic_partitions_handler(std::move(req));
      });

    /*
     * Get detailed information about a partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partition,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_partition_handler(std::move(req));
      });

    /*
     * Get detailed information about transactions for partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_transactions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_transactions_handler(std::move(req));
      });

    /*
     * Abort transaction for partition
     */
    register_route<superuser>(
      ss::httpd::partition_json::mark_transaction_expired,
      [this](std::unique_ptr<ss::http::request> req) {
          return mark_transaction_expired_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::partition_json::cancel_partition_reconfiguration,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_partition_reconfig_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::partition_json::unclean_abort_partition_reconfiguration,
      [this](std::unique_ptr<ss::http::request> req) {
          return unclean_abort_partition_reconfig_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::set_partition_replicas,
      [this](std::unique_ptr<ss::http::request> req) {
          return set_partition_replicas_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::force_update_partition_replicas,
      [this](std::unique_ptr<ss::http::request> req) {
          return force_set_partition_replicas_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::trigger_partitions_rebalance,
      [this](std::unique_ptr<ss::http::request> req) {
          return trigger_on_demand_rebalance_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::trigger_partitions_shard_rebalance,
      [this](std::unique_ptr<ss::http::request> req) {
          return trigger_shard_rebalance_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::partition_json::get_partition_reconfigurations,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_reconfigurations_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::partition_json::majority_lost,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_majority_lost_partitions(std::move(req));
      });

    register_route<user>(
      ss::httpd::partition_json::force_recover_from_nodes,
      [this](std::unique_ptr<ss::http::request> req) {
          return force_recover_partitions_from_nodes(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::set_partition_replica_core,
      [this](std::unique_ptr<ss::http::request> req) {
          return set_partition_replica_core_handler(std::move(req));
      });
}
namespace {
ss::httpd::partition_json::partition
build_controller_partition(cluster::metadata_cache& cache) {
    ss::httpd::partition_json::partition p;
    p.ns = model::controller_ntp.ns;
    p.topic = model::controller_ntp.tp.topic;
    p.partition_id = model::controller_ntp.tp.partition;
    p.leader_id = -1;

    // Controller topic is on all nodes.  Report all nodes,
    // with the leader first.
    auto leader_opt = cache.get_controller_leader_id();
    if (leader_opt.has_value()) {
        ss::httpd::partition_json::assignment a;
        a.node_id = leader_opt.value();
        a.core = cluster::controller_stm_shard;
        p.replicas.push(a);
        p.leader_id = *leader_opt;
    }
    // special case, controller is raft group 0
    p.raft_group_id = 0;
    for (const auto& i : cache.node_ids()) {
        if (!leader_opt.has_value() || leader_opt.value() != i) {
            ss::httpd::partition_json::assignment a;
            a.node_id = i;
            a.core = cluster::controller_stm_shard;
            p.replicas.push(a);
        }
    }

    // Controller topic does not have a reconciliation state,
    // but include the field anyway to keep the API output
    // consistent.
    p.status = ssx::sformat("{}", cluster::reconciliation_status::done);
    return p;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_partition_handler(std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);
    const bool is_controller = ntp == model::controller_ntp;

    if (!is_controller && !_metadata_cache.local().contains(ntp)) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find ntp: {}", ntp));
    }

    ss::httpd::partition_json::partition p;
    p.ns = ntp.ns;
    p.topic = ntp.tp.topic;
    p.partition_id = ntp.tp.partition;
    p.leader_id = -1;

    // Logic for fetching replicas+status is different for normal
    // topics vs. the special controller topic.
    if (is_controller) {
        return ss::make_ready_future<ss::json::json_return_type>(
          build_controller_partition(_metadata_cache.local()));

    } else {
        // Normal topic
        auto assignment
          = _controller->get_topics_state().local().get_partition_assignment(
            ntp);

        if (assignment) {
            for (auto& r : assignment->replicas) {
                ss::httpd::partition_json::assignment a;
                a.node_id = r.node_id;
                a.core = r.shard;
                p.replicas.push(a);
            }
            p.raft_group_id = assignment->group;
        }
        auto leader = _metadata_cache.local().get_leader_id(ntp);
        if (leader) {
            p.leader_id = *leader;
        }

        p.disabled = _controller->get_topics_state().local().is_disabled(ntp);

        return _controller->get_api()
          .local()
          .get_reconciliation_state(ntp)
          .then([p](const cluster::ntp_reconciliation_state& state) mutable {
              p.status = ssx::sformat("{}", state.status());
              return ss::json::json_return_type(std::move(p));
          });
    }
}
ss::future<ss::json::json_return_type>
admin_server::get_topic_partitions_handler(
  std::unique_ptr<ss::http::request> req) {
    model::topic_namespace tp_ns(
      model::ns(req->get_path_param("namespace")),
      model::topic(req->get_path_param("topic")));
    const bool is_controller_topic = tp_ns.ns == model::controller_ntp.ns
                                     && tp_ns.tp
                                          == model::controller_ntp.tp.topic;

    // Logic for fetching replicas+status is different for normal
    // topics vs. the special controller topic.
    if (is_controller_topic) {
        co_return ss::json::json_return_type(
          build_controller_partition(_metadata_cache.local()));
    }

    auto tp_md = _metadata_cache.local().get_topic_metadata_ref(tp_ns);
    if (!tp_md) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find topic: {}/{}", tp_ns.ns, tp_ns.tp));
    }
    using partition_t = ss::httpd::partition_json::partition;
    fragmented_vector<partition_t> partitions;
    const auto& assignments = tp_md->get().get_assignments();
    partitions.reserve(assignments.size());

    const auto* disabled_set
      = _controller->get_topics_state().local().get_topic_disabled_set(tp_ns);

    // Normal topic
    for (const auto& [_, p_as] : assignments) {
        partition_t p;
        p.ns = tp_ns.ns;
        p.topic = tp_ns.tp;
        p.partition_id = p_as.id;
        p.raft_group_id = p_as.group;
        for (auto& r : p_as.replicas) {
            ss::httpd::partition_json::assignment a;
            a.node_id = r.node_id;
            a.core = r.shard;
            p.replicas.push(a);
        }
        auto leader = _metadata_cache.local().get_leader_id(tp_ns, p_as.id);
        if (leader) {
            p.leader_id = *leader;
        }
        p.disabled = disabled_set && disabled_set->is_disabled(p_as.id);
        partitions.push_back(std::move(p));
    }

    co_await ss::max_concurrent_for_each(
      partitions, 32, [this, &tp_ns](partition_t& p) {
          return _controller->get_api()
            .local()
            .get_reconciliation_state(model::ntp(
              tp_ns.ns, tp_ns.tp, model::partition_id(p.partition_id())))
            .then([&p](const cluster::ntp_reconciliation_state& state) mutable {
                p.status = ssx::sformat("{}", state.status());
            });
      });

    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      lw_shared_container(std::move(partitions)), [](auto& p) { return p; }));
}

ss::future<ss::json::json_return_type>
admin_server::get_majority_lost_partitions(
  std::unique_ptr<ss::http::request> request) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*request, model::controller_ntp);
    }

    auto input = request->get_query_param("dead_nodes");
    if (input.length() <= 0) {
        throw ss::httpd::bad_param_exception(
          "Query parameter dead_nodes not set, expecting a csv of integers "
          "(broker_ids)");
    }

    std::vector<ss::sstring> tokens;
    boost::split(tokens, input, boost::is_any_of(","));

    std::vector<model::node_id> dead_nodes;
    dead_nodes.reserve(tokens.size());
    for (auto& token : tokens) {
        try {
            dead_nodes.emplace_back(std::stoi(token));
        } catch (...) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "Token {} doesn't parse to an integer in input: {}, expecting a "
              "csv of integer broker_ids",
              token,
              input));
        }
    }

    if (dead_nodes.size() == 0) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Malformed input query parameter: {}, expecting a csv of "
          "integers (broker_ids)",
          input));
    }

    vlog(
      adminlog.info,
      "Request for majority loss partitions from input defunct nodes: {}",
      dead_nodes);

    auto result = co_await _controller->get_topics_frontend()
                    .local()
                    .partitions_with_lost_majority(std::move(dead_nodes));
    if (!result) {
        if (
          result.error().category() == cluster::error_category()
          && result.error() == cluster::errc::concurrent_modification_error) {
            throw ss::httpd::base_exception(
              "Concurrent changes to topics while the operation, retry after "
              "some time, ensure there are no reconfigurations in progress.",
              ss::http::reply::status_type::service_unavailable);
        } else if (
          result.error().category() == cluster::error_category()
          && result.error() == cluster::errc::invalid_request) {
            throw ss::httpd::base_exception{
              "Invalid request, check the broker log for details.",
              ss::http::reply::status_type::bad_request};
        }
        throw ss::httpd::base_exception(
          fmt::format(
            "Internal error while processing request: {}",
            result.error().message()),
          ss::http::reply::status_type::internal_server_error);
    }
    co_return ss::json::json_return_type(ss::json::stream_range_as_array(
      lw_shared_container(std::move(result.value())),
      [](const cluster::ntp_with_majority_loss& ntp) mutable {
          ss::httpd::partition_json::ntp ntp_json;
          ntp_json.ns = ntp.ntp.ns();
          ntp_json.topic = ntp.ntp.tp.topic();
          ntp_json.partition = ntp.ntp.tp.partition();

          ss::httpd::partition_json::ntp_with_majority_loss result;
          result.ntp = std::move(ntp_json);
          result.topic_revision = ntp.topic_revision;
          for (auto& replica : ntp.assignment) {
              ss::httpd::partition_json::assignment assignment;
              assignment.node_id = replica.node_id;
              assignment.core = replica.shard;
              result.replicas.push(assignment);
          }
          for (auto& node : ntp.dead_nodes) {
              result.dead_nodes.push(node());
          }
          return result;
      }));
}

namespace {

json::validator make_node_id_array_validator() {
    const std::string schema = R"(
    {
      "type": "array",
      "items": {
        "type": "number"
      }
    }
  )";
    return json::validator(schema);
}

std::vector<model::node_id>
parse_node_ids_from_json(const json::Document::ValueType& val) {
    static thread_local json::validator validator(
      make_node_id_array_validator());
    apply_validator(validator, val);
    std::vector<model::node_id> nodes;
    for (auto& r : val.GetArray()) {
        nodes.emplace_back(r.GetInt());
    }
    return nodes;
}

json::validator make_force_recover_partitions_validator() {
    const std::string schema = R"(
{
  "type": "object",
  "properties": {
    "dead_nodes": {
      "type": "array",
      "items": {
        "type": "number"
      }
    },
    "partitions_to_force_recover": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "ntp": {
            "type": "object",
            "properties": {
              "ns": {
                "type": "string"
              },
              "topic": {
                "type": "string"
              },
              "partition": {
                "type": "number"
              }
            },
            "required": [
              "ns",
              "topic",
              "partition"
            ]
          },
          "topic_revision": {
            "type": "number"
          },
          "replicas": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "node_id": {
                  "type": "number"
                },
                "core": {
                  "type": "number"
                }
              },
              "required": [
                "node_id",
                "core"
              ]
            }
          },
          "dead_nodes": {
            "type": "array",
            "items": {
              "type": "number"
            }
          }
        },
        "required": [
          "ntp",
          "topic_revision",
          "replicas",
          "dead_nodes"
        ]
      }
    }
  },
  "required": [
    "dead_nodes",
    "partitions_to_force_recover"
  ]
}
)";
    return json::validator(schema);
}

json::validator make_ntp_validator() {
    const std::string schema = R"(
{
  "type": "object",
  "properties": {
    "ns": {
      "type": "string"
    },
    "topic": {
      "type": "string"
    },
    "partition": {
      "type": "number"
    }
  },
  "required": [
    "ns",
    "topic",
    "partition"
  ]
}
)";
    return json::validator(schema);
}

model::ntp parse_ntp_from_json(const json::Document::ValueType& value) {
    static thread_local json::validator validator(make_ntp_validator());
    apply_validator(validator, value);
    return {
      model::ns{value["ns"].GetString()},
      model::topic(value["topic"].GetString()),
      model::partition_id(value["partition"].GetInt())};
}

json::validator make_replicas_validator() {
    const std::string schema = R"(
{
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "node_id": {
        "type": "number"
      },
      "core": {
        "type": "number"
      }
    },
    "required": [
      "node_id",
      "core"
    ]
  }
}
  )";
    return json::validator(schema);
}

std::vector<model::broker_shard>
parse_replicas_from_json(const json::Document::ValueType& value) {
    static thread_local json::validator validator(make_replicas_validator());
    apply_validator(validator, value);
    std::vector<model::broker_shard> replicas;
    replicas.reserve(value.GetArray().Size());
    for (auto& r : value.GetArray()) {
        model::broker_shard bs;
        bs.node_id = model::node_id(r["node_id"].GetInt());
        bs.shard = r["core"].GetInt();
        replicas.push_back(bs);
    }
    return replicas;
}

} // namespace

ss::future<ss::json::json_return_type>
admin_server::force_recover_partitions_from_nodes(
  std::unique_ptr<ss::http::request> request) {
    if (!_controller->get_feature_table().local().is_active(
          features::feature::enhanced_force_reconfiguration)) {
        throw ss::httpd::bad_request_exception(
          "Required feature is not active yet which indicates the cluster has "
          "not fully upgraded yet, retry after a successful upgrade.");
    }

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*request, model::controller_ntp);
    }

    static thread_local json::validator validator(
      make_force_recover_partitions_validator());

    auto doc = co_await parse_json_body(request.get());
    apply_validator(validator, doc);

    // parse the json body into a controller command.
    std::vector<model::node_id> dead_nodes = parse_node_ids_from_json(
      doc["dead_nodes"]);
    fragmented_vector<cluster::ntp_with_majority_loss>
      partitions_to_force_recover;
    for (auto& r : doc["partitions_to_force_recover"].GetArray()) {
        auto ntp = parse_ntp_from_json(r["ntp"]);
        auto replicas = parse_replicas_from_json(r["replicas"]);
        auto topic_revision = model::revision_id(
          r["topic_revision"].GetInt64());
        auto dead_replicas = parse_node_ids_from_json(r["dead_nodes"]);

        cluster::ntp_with_majority_loss ntp_entry;
        ntp_entry.ntp = std::move(ntp);
        ntp_entry.assignment = std::move(replicas);
        ntp_entry.topic_revision = topic_revision;
        ntp_entry.dead_nodes = std::move(dead_replicas);
        partitions_to_force_recover.push_back(std::move(ntp_entry));
    }

    auto ec = co_await _controller->get_topics_frontend()
                .local()
                .force_recover_partitions_from_nodes(
                  std::move(dead_nodes),
                  std::move(partitions_to_force_recover),
                  model::timeout_clock::now() + 5s);

    co_await throw_on_error(*request, ec, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::trigger_on_demand_rebalance_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ec = co_await _controller->get_partition_balancer().invoke_on(
      cluster::controller_stm_shard,
      [](cluster::partition_balancer_backend& pb) {
          return pb.request_rebalance();
      });

    co_await throw_on_error(*req, ec, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::trigger_shard_rebalance_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ec = co_await _controller->get_topics_frontend()
                .local()
                .trigger_local_partition_shard_rebalance();

    co_await throw_on_error(*req, ec, model::ntp{});
    co_return ss::json::json_return_type(ss::json::json_void());
}
