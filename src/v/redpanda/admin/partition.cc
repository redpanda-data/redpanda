#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/controller_stm.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "config/node_config.h"
#include "coproc/partition_manager.h"
#include "redpanda/admin/api-doc/partition.json.h"
#include "redpanda/admin/server.h"
#include "vlog.h"

namespace admin {

namespace {
model::ntp parse_ntp_from_request(ss::httpd::parameters& param) {
    auto ns = model::ns(param["namespace"]);
    auto topic = model::topic(param["topic"]);

    model::partition_id partition;
    try {
        partition = model::partition_id(std::stoi(param["partition"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Partition id must be an integer: {}", param["partition"]));
    }

    if (partition() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid partition id {}", partition));
    }

    return model::ntp(std::move(ns), std::move(topic), partition);
}

bool need_redirect_to_leader(
  model::ntp ntp, ss::sharded<cluster::metadata_cache>& metadata_cache) {
    auto leader_id_opt = metadata_cache.local().get_leader_id(ntp);
    if (!leader_id_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt_with_ctx(
            fmt::format,
            "Partition {} does not have a leader, cannot redirect",
            ntp),
          ss::httpd::reply::status_type::service_unavailable);
    }

    return leader_id_opt.value() != config::node().node_id();
}

json_validator make_set_replicas_validator() {
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
    return json_validator(schema);
}
} // namespace

void admin_server::register_partition_routes() {
    /*
     * Get a list of partition summaries.
     */
    ss::httpd::partition_json::get_partitions.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          using summary = ss::httpd::partition_json::partition_summary;
          auto get_summaries = [](auto& partition_manager, bool materialized) {
              return partition_manager.map_reduce0(
                [materialized](auto& pm) {
                    std::vector<summary> partitions;
                    partitions.reserve(pm.partitions().size());
                    for (const auto& it : pm.partitions()) {
                        summary p;
                        p.ns = it.first.ns;
                        p.topic = it.first.tp.topic;
                        p.partition_id = it.first.tp.partition;
                        p.core = ss::this_shard_id();
                        p.materialized = materialized;
                        partitions.push_back(std::move(p));
                    }
                    return partitions;
                },
                std::vector<summary>{},
                [](std::vector<summary> acc, std::vector<summary> update) {
                    acc.insert(acc.end(), update.begin(), update.end());
                    return acc;
                });
          };
          auto f1 = get_summaries(_partition_manager, false);
          auto f2 = get_summaries(_cp_partition_manager, true);
          return ss::when_all_succeed(std::move(f1), std::move(f2))
            .then([](auto summaries) {
                auto& [partitions, s2] = summaries;
                for (auto& s : s2) {
                    partitions.push_back(std::move(s));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(partitions));
            });
      });

    /*
     * Get detailed information about a partition.
     */
    ss::httpd::partition_json::get_partition.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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
              // Controller topic is on all nodes.  Report all nodes,
              // with the leader first.
              auto leader_opt
                = _metadata_cache.local().get_controller_leader_id();
              if (leader_opt.has_value()) {
                  ss::httpd::partition_json::assignment a;
                  a.node_id = leader_opt.value();
                  a.core = cluster::controller_stm_shard;
                  p.replicas.push(a);
                  p.leader_id = *leader_opt;
              }
              // special case, controller is raft group 0
              p.raft_group_id = 0;
              for (const auto& i : _metadata_cache.local().all_broker_ids()) {
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
              p.status = ssx::sformat(
                "{}", cluster::reconciliation_status::done);
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(p));

          } else {
              // Normal topic
              auto assignment = _controller->get_topics_state()
                                  .local()
                                  .get_partition_assignment(ntp);

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

              return _controller->get_api()
                .local()
                .get_reconciliation_state(ntp)
                .then(
                  [p](const cluster::ntp_reconciliation_state& state) mutable {
                      p.status = ssx::sformat("{}", state.status());
                      return ss::make_ready_future<ss::json::json_return_type>(
                        std::move(p));
                  });
          }
      });

    /*
     * Get detailed information about transactions for partition.
     */
    ss::httpd::partition_json::get_transactions.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
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
            [_ntp = std::move(ntp), _req = std::move(req), this](
              cluster::partition_manager& pm) mutable
            -> ss::future<ss::json::json_return_type> {
                auto ntp = std::move(_ntp);
                auto req = std::move(_req);
                auto partition = pm.get(ntp);
                if (!partition) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format, "Can not find partition {}", partition));
                }

                auto rm_stm_ptr = partition->rm_stm();

                if (!rm_stm_ptr) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format,
                      "Can not get rm_stm for partition {}",
                      partition));
                }

                auto transactions = co_await rm_stm_ptr->get_transactions();

                if (transactions.has_error()) {
                    co_await throw_on_error(*req, transactions.error(), ntp);
                }
                ss::httpd::partition_json::transactions ans;

                auto offset_translator
                  = partition->get_offset_translator_state();

                for (auto& [id, tx_info] : transactions.value()) {
                    ss::httpd::partition_json::producer_identity pid;
                    pid.id = id.get_id();
                    pid.epoch = id.get_epoch();

                    ss::httpd::partition_json::transaction new_tx;
                    new_tx.producer_id = pid;
                    new_tx.status = ss::sstring(tx_info.get_status());

                    new_tx.lso_bound = offset_translator->from_log_offset(
                      tx_info.lso_bound);

                    auto staleness = tx_info.get_staleness();
                    // -1 is returned for expired transaction, because how long
                    // transaction do not do progress is useless for expired tx.
                    new_tx.staleness_ms = staleness.has_value()
                                            ? staleness.value().count()
                                            : -1;
                    auto timeout = tx_info.get_timeout();
                    // -1 is returned for expired transaction, because timeout
                    // is useless for expired tx.
                    new_tx.timeout_ms = timeout.has_value()
                                          ? timeout.value().count()
                                          : -1;

                    if (tx_info.is_expired()) {
                        ans.expired_transactions.push(new_tx);
                    } else {
                        ans.active_transactions.push(new_tx);
                    }
                }

                co_return ss::json::json_return_type(ans);
            });
      });

    /*
     * Abort transaction for partition
     */
    ss::httpd::partition_json::mark_transaction_expired.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          const model::ntp ntp = parse_ntp_from_request(req->param);

          model::producer_identity pid;
          auto node = req->get_query_param("id");
          try {
              pid.id = std::stoi(node);
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt_with_ctx(
                fmt::format, "Transaction id must be an integer: {}", node));
          }
          node = req->get_query_param("epoch");
          try {
              int64_t epoch = std::stoi(node);
              if (
                epoch < std::numeric_limits<int16_t>::min()
                || epoch > std::numeric_limits<int16_t>::max()) {
                  throw ss::httpd::bad_param_exception(fmt_with_ctx(
                    fmt::format, "Invalid transaction epoch {}", epoch));
              }
              pid.epoch = epoch;
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt_with_ctx(
                fmt::format, "Transaction epoch must be an integer: {}", node));
          }

          vlog(logger.info, "Mark transaction expired for pid:{}", pid);

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
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format, "Can not find partition {}", partition));
                }

                auto rm_stm_ptr = partition->rm_stm();

                if (!rm_stm_ptr) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format,
                      "Can not get rm_stm for partition {}",
                      partition));
                }

                auto res = co_await rm_stm_ptr->mark_expired(pid);
                co_await throw_on_error(*req, res, ntp);

                co_return ss::json::json_return_type(ss::json::json_void());
            });
      });

    // make sure to call reset() before each use
    static thread_local json_validator set_replicas_validator(
      make_set_replicas_validator());

    ss::httpd::partition_json::set_partition_replicas.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto ns = model::ns(req->param["namespace"]);
          auto topic = model::topic(req->param["topic"]);

          model::partition_id partition;
          try {
              partition = model::partition_id(
                std::stoi(req->param["partition"]));
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Partition id must be an integer: {}",
                req->param["partition"]));
          }

          if (partition() < 0) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Invalid partition id {}", partition));
          }

          const model::ntp ntp(std::move(ns), std::move(topic), partition);

          if (ntp == model::controller_ntp) {
              throw ss::httpd::bad_request_exception(
                fmt::format("Can't reconfigure a controller"));
          }

          auto doc = parse_json_body(*req);
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
              const auto shard = static_cast<uint32_t>(r["core"].GetInt());

              // Validate node ID and shard - subsequent code assumes
              // they exist and may assert if not.
              bool is_valid = co_await _controller->get_topics_frontend()
                                .local()
                                .validate_shard(node_id, shard);
              if (!is_valid) {
                  throw ss::httpd::bad_request_exception(fmt::format(
                    "Replica set refers to non-existent node/shard (node {} "
                    "shard {})",
                    node_id,
                    shard));
              }

              replicas.push_back(
                model::broker_shard{.node_id = node_id, .shard = shard});
          }

          auto current_assignment
            = _controller->get_topics_state().local().get_partition_assignment(
              ntp);

          // For a no-op change, just return success here, to avoid doing
          // all the raft writes and consensus restarts for a config change
          // that will do nothing.
          if (current_assignment && current_assignment->replicas == replicas) {
              vlog(
                logger.info,
                "Request to change ntp {} replica set to {}, no change",
                ntp,
                replicas);
              co_return ss::json::json_void();
          }

          vlog(
            logger.info,
            "Request to change ntp {} replica set to {}",
            ntp,
            replicas);

          auto err
            = co_await _controller->get_topics_frontend()
                .local()
                .move_partition_replicas(
                  ntp,
                  replicas,
                  model::timeout_clock::now()
                    + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

          vlog(
            logger.debug,
            "Request to change ntp {} replica set to {}: err={}",
            ntp,
            replicas,
            err);

          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_void();
      });
}

} // namespace admin
