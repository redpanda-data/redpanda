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
#include "cloud_storage/cache_service.h"
#include "cluster/cloud_storage_size_reducer.h"
#include "cluster/controller.h"
#include "cluster/controller_stm.h"
#include "cluster/members_manager.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "container/lw_shared_container.h"
#include "json/validator.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "resource_mgmt/cpu_profiler.h"
#include "serde/rw/rw.h"
#include "storage/kvstore.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/json/json_elements.hh>

namespace {
ss::future<result<std::vector<cluster::partition_state>>>
get_partition_state(model::ntp ntp, cluster::controller& controller) {
    if (ntp == model::controller_ntp) {
        return controller.get_controller_partition_state();
    }
    return controller.get_topics_frontend().local().get_partition_state(
      std::move(ntp));
}

void fill_raft_state(
  ss::httpd::debug_json::partition_replica_state& replica,
  cluster::partition_state state) {
    ss::httpd::debug_json::raft_replica_state raft_state;
    auto& src = state.raft_state;
    raft_state.node_id = src.node();
    raft_state.term = src.term();
    raft_state.offset_translator_state = std::move(src.offset_translator_state);
    raft_state.group_configuration = std::move(src.group_configuration);
    raft_state.confirmed_term = src.confirmed_term();
    raft_state.flushed_offset = src.flushed_offset();
    raft_state.commit_index = src.commit_index();
    raft_state.majority_replicated_index = src.majority_replicated_index();
    raft_state.visibility_upper_bound_index
      = src.visibility_upper_bound_index();
    raft_state.last_quorum_replicated_index
      = src.last_quorum_replicated_index();
    raft_state.last_snapshot_term = src.last_snapshot_term();
    raft_state.received_snapshot_bytes = src.received_snapshot_bytes;
    raft_state.last_snapshot_index = src.last_snapshot_index();
    raft_state.received_snapshot_index = src.received_snapshot_index();
    raft_state.has_pending_flushes = src.has_pending_flushes;
    raft_state.is_leader = src.is_leader;
    raft_state.is_elected_leader = src.is_elected_leader;
    raft_state.write_caching_enabled = src.write_caching_enabled;
    raft_state.flush_bytes = src.flush_bytes;
    raft_state.flush_ms = src.flush_ms.count();
    raft_state.time_since_last_flush = src.time_since_last_flush / 1ms;
    raft_state.replication_monitor_state = src.replication_monitor_state;
    if (src.followers) {
        for (const auto& f : *src.followers) {
            ss::httpd::debug_json::raft_follower_state follower_state;
            follower_state.id = f.node();
            follower_state.last_flushed_log_index = f.last_flushed_log_index();
            follower_state.last_dirty_log_index = f.last_dirty_log_index();
            follower_state.match_index = f.match_index();
            follower_state.next_index = f.next_index();
            follower_state.expected_log_end_offset
              = f.expected_log_end_offset();
            follower_state.heartbeats_failed = f.heartbeats_failed;
            follower_state.is_learner = f.is_learner;
            follower_state.ms_since_last_heartbeat = f.ms_since_last_heartbeat;
            follower_state.last_sent_seq = f.last_sent_seq;
            follower_state.last_received_seq = f.last_received_seq;
            follower_state.last_successful_received_seq
              = f.last_successful_received_seq;
            follower_state.suppress_heartbeats = f.suppress_heartbeats;
            follower_state.is_recovering = f.is_recovering;
            raft_state.followers.push(std::move(follower_state));
        }
    }
    for (const auto& stm : state.raft_state.stms) {
        ss::httpd::debug_json::stm_state state;
        state.name = stm.name;
        state.last_applied_offset = stm.last_applied_offset;
        state.max_collectible_offset = stm.max_collectible_offset;
        raft_state.stms.push(std::move(state));
    }
    if (src.recovery_state) {
        ss::httpd::debug_json::follower_recovery_state frs;
        frs.is_active = src.recovery_state->is_active;
        frs.pending_offset_count = src.recovery_state->pending_offset_count;
        raft_state.follower_recovery_state = std::move(frs);
    }
    replica.raft_state = std::move(raft_state);
}
} // namespace

void admin_server::register_debug_routes() {
    register_route<user>(
      ss::httpd::debug_json::stress_fiber_stop,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(adminlog.info, "Stopping stress fiber");
          return _stress_fiber_manager
            .invoke_on_all([](auto& stress_mgr) { return stress_mgr.stop(); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<user>(
      ss::httpd::debug_json::stress_fiber_start,
      [this](std::unique_ptr<ss::http::request> req) {
          vlog(adminlog.info, "Requested stress fiber");
          stress_config cfg;
          const auto parse_int =
            [&](const ss::sstring& param, std::optional<int>& val) {
                if (auto e = req->get_query_param(param); !e.empty()) {
                    try {
                        val = boost::lexical_cast<int>(e);
                    } catch (const boost::bad_lexical_cast&) {
                        throw ss::httpd::bad_param_exception(fmt::format(
                          "Invalid parameter '{}' value {{{}}}", param, e));
                    }
                } else {
                    val = std::nullopt;
                }
            };
          parse_int(
            "min_spins_per_scheduling_point",
            cfg.min_spins_per_scheduling_point);
          parse_int(
            "max_spins_per_scheduling_point",
            cfg.max_spins_per_scheduling_point);
          parse_int(
            "min_ms_per_scheduling_point", cfg.min_ms_per_scheduling_point);
          parse_int(
            "max_ms_per_scheduling_point", cfg.max_ms_per_scheduling_point);
          if (
            cfg.max_spins_per_scheduling_point.has_value()
            != cfg.min_spins_per_scheduling_point.has_value()) {
              throw ss::httpd::bad_param_exception(
                "Expected 'max_spins_per_scheduling_point' set with "
                "'min_spins_per_scheduling_point'");
          }
          if (
            cfg.max_ms_per_scheduling_point.has_value()
            != cfg.min_ms_per_scheduling_point.has_value()) {
              throw ss::httpd::bad_param_exception(
                "Expected 'max_ms_per_scheduling_point' set with "
                "'min_ms_per_scheduling_point'");
          }
          // Check that either delay or a spin count is defined.
          if (
            cfg.max_spins_per_scheduling_point.has_value()
            == cfg.max_ms_per_scheduling_point.has_value()) {
              throw ss::httpd::bad_param_exception(
                "Expected either spins or delay to be defined");
          }
          if (
            cfg.max_spins_per_scheduling_point.has_value()
            && cfg.max_spins_per_scheduling_point.value()
                 < cfg.min_spins_per_scheduling_point.value()) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Invalid parameter 'max_spins_per_scheduling_point' value "
                "is too low: {} < {}",
                cfg.max_spins_per_scheduling_point.value(),
                cfg.min_spins_per_scheduling_point.value()));
          }
          if (
            cfg.max_ms_per_scheduling_point.has_value()
            && cfg.max_ms_per_scheduling_point.value()
                 < cfg.min_ms_per_scheduling_point.value()) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Invalid parameter 'max_ms_per_scheduling_point' value "
                "is too low: {} < {}",
                cfg.max_ms_per_scheduling_point.value(),
                cfg.min_ms_per_scheduling_point.value()));
          }
          cfg.num_fibers = 1;
          if (auto e = req->get_query_param("num_fibers"); !e.empty()) {
              try {
                  cfg.num_fibers = boost::lexical_cast<int>(e);
              } catch (const boost::bad_lexical_cast&) {
                  throw ss::httpd::bad_param_exception(fmt::format(
                    "Invalid parameter 'num_fibers' value {{{}}}", e));
              }
          }
          return _stress_fiber_manager
            .invoke_on_all([cfg](auto& stress_mgr) {
                auto ran = stress_mgr.start(cfg);
                if (ran) {
                    vlog(adminlog.info, "Started stress fiber...");
                } else {
                    vlog(adminlog.info, "Stress fiber already running...");
                }
            })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<user>(
      ss::httpd::debug_json::reset_leaders_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(adminlog.info, "Request to reset leaders info");
          return _metadata_cache
            .invoke_on_all([](auto& mc) { mc.reset_leaders(); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<user>(
      ss::httpd::debug_json::refresh_disk_health_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(adminlog.info, "Request to refresh disk health info");
          return _metadata_cache.local().refresh_health_monitor().then_wrapped(
            [](ss::future<> f) {
                if (f.failed()) {
                    auto eptr = f.get_exception();
                    vlog(
                      adminlog.error,
                      "failed to refresh disk health info: {}",
                      eptr);
                    return ss::make_exception_future<
                      ss::json::json_return_type>(
                      ss::httpd::server_error_exception(
                        "Could not refresh disk health info"));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_void());
            });
      });

    register_route<user>(
      ss::httpd::debug_json::get_leaders_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(adminlog.info, "Request to get leaders info");
          using result_t = ss::httpd::debug_json::leader_info;
          using leaders = cluster::partition_leaders_table::leaders_info_t;
          using cme
            = cluster::partition_leaders_table::concurrent_modification_error;
          return _metadata_cache.local()
            .get_leaders()
            .handle_exception_type([](const cme& ex) -> ss::future<leaders> {
                throw ss::httpd::base_exception(
                  ex.what(), ss::http::reply::status_type::service_unavailable);
            })
            .then([](leaders leaders_info) {
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::stream_range_as_array(
                    lw_shared_container(std::move(leaders_info)),
                    [](const auto& leader_info) {
                        result_t info;
                        info.ns = leader_info.tp_ns.ns;
                        info.topic = leader_info.tp_ns.tp;
                        info.partition_id = leader_info.pid;
                        info.leader = leader_info.current_leader.value_or(
                          model::node_id(-1));
                        info.previous_leader = leader_info.previous_leader
                                                 .value_or(model::node_id(-1));
                        info.last_stable_leader_term
                          = leader_info.last_stable_leader_term;
                        info.update_term = leader_info.update_term;
                        info.partition_revision
                          = leader_info.partition_revision;
                        return info;
                    }));
            });
      });

    register_route<user>(
      seastar::httpd::debug_json::get_peer_status,
      [this](std::unique_ptr<ss::http::request> req) {
          model::node_id id = parse_broker_id(*req);
          auto node_status = _node_status_table.local().get_node_status(id);

          if (!node_status) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Unknown node with id {}", id));
          }

          auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
            rpc::clock_type::now() - node_status->last_seen);

          seastar::httpd::debug_json::peer_status ret;
          ret.since_last_status = delta.count();

          return ss::make_ready_future<ss::json::json_return_type>(ret);
      });

    register_route<user>(
      seastar::httpd::debug_json::is_node_isolated,
      [this](std::unique_ptr<ss::http::request>) {
          return ss::make_ready_future<ss::json::json_return_type>(
            _metadata_cache.local().is_node_isolated());
      });

    register_route<user>(
      seastar::httpd::debug_json::get_controller_status,
      [this](std::unique_ptr<ss::http::request>)
        -> ss::future<ss::json::json_return_type> {
          return ss::smp::submit_to(cluster::controller_stm_shard, [this] {
              return _controller->get_last_applied_offset().then(
                [this](auto offset) {
                    using result_t = ss::httpd::debug_json::controller_status;
                    result_t ans;
                    ans.last_applied_offset = offset;
                    ans.start_offset = _controller->get_start_offset();
                    ans.committed_index = _controller->get_commited_index();
                    ans.dirty_offset = _controller->get_dirty_offset();
                    return ss::make_ready_future<ss::json::json_return_type>(
                      ss::json::json_return_type(ans));
                });
          });
      });

    register_route<user>(
      seastar::httpd::debug_json::get_cloud_storage_usage,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return cloud_storage_usage_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::blocked_reactor_notify_ms,
      [this](std::unique_ptr<ss::http::request> req) {
          std::chrono::milliseconds timeout;
          if (auto e = req->get_query_param("timeout"); !e.empty()) {
              try {
                  timeout = std::clamp(
                    std::chrono::milliseconds(
                      boost::lexical_cast<long long>(e)),
                    1ms,
                    _default_blocked_reactor_notify);
              } catch (const boost::bad_lexical_cast&) {
                  throw ss::httpd::bad_param_exception(
                    fmt::format("Invalid parameter 'timeout' value {{{}}}", e));
              }
          }

          std::optional<std::chrono::seconds> expires;
          static constexpr std::chrono::seconds max_expire_time_sec
            = std::chrono::minutes(30);
          if (auto e = req->get_query_param("expires"); !e.empty()) {
              try {
                  expires = std::clamp(
                    std::chrono::seconds(boost::lexical_cast<long long>(e)),
                    1s,
                    max_expire_time_sec);
              } catch (const boost::bad_lexical_cast&) {
                  throw ss::httpd::bad_param_exception(
                    fmt::format("Invalid parameter 'expires' value {{{}}}", e));
              }
          }

          // This value is used when the expiration time is not set explicitly
          static constexpr std::chrono::seconds default_expiration_time
            = std::chrono::minutes(5);
          auto curr = ss::engine().get_blocked_reactor_notify_ms();

          vlog(
            adminlog.info,
            "Setting blocked_reactor_notify_ms from {} to {} for {} "
            "(default={})",
            curr,
            timeout.count(),
            expires.value_or(default_expiration_time),
            _default_blocked_reactor_notify);

          return ss::smp::invoke_on_all([timeout] {
                     ss::engine().update_blocked_reactor_notify_ms(timeout);
                 })
            .then([] {
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_void());
            })
            .finally([this, expires] {
                _blocked_reactor_notify_reset_timer.rearm(
                  ss::steady_clock_type::now()
                  + expires.value_or(default_expiration_time));
            });
      });

    register_route<superuser>(
      ss::httpd::debug_json::sampled_memory_profile,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return sampled_memory_profile_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::debug_json::restart_service,
      [this](std::unique_ptr<ss::http::request> req) {
          return restart_service_handler(std::move(req));
      });

    register_route<user>(
      seastar::httpd::debug_json::get_partition_state,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return get_partition_state_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::cpu_profile,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return cpu_profile_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::debug_json::set_storage_failure_injection_enabled,
      [](std::unique_ptr<ss::http::request> req) {
          auto value = req->get_query_param("value");
          if (value != "true" && value != "false") {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Invalid parameter 'value' {{{}}}. Should be 'true' or "
                "'false'",
                value));
          }

          return ss::smp::invoke_on_all([value] {
                     config::node().storage_failure_injection_enabled.set_value(
                       value == "true");
                 })
            .then([] {
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_void());
            });
      });

    register_route<user>(
      seastar::httpd::debug_json::get_local_storage_usage,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return get_local_storage_usage_handler(std::move(req));
      });

    request_handler_fn unsafe_reset_metadata_handler = [this](
                                                         auto req, auto reply) {
        return unsafe_reset_metadata(std::move(req), std::move(reply));
    };

    register_route<superuser>(
      ss::httpd::debug_json::unsafe_reset_metadata,
      std::move(unsafe_reset_metadata_handler));

    register_route<superuser>(
      ss::httpd::debug_json::get_disk_stat,
      [this](std::unique_ptr<ss::http::request> request) {
          return get_disk_stat_handler(std::move(request));
      });

    register_route<superuser>(
      ss::httpd::debug_json::get_local_offsets_translated,
      [this](std::unique_ptr<ss::http::request> request) {
          return get_local_offsets_translated_handler(std::move(request));
      });

    register_route<superuser>(
      ss::httpd::debug_json::put_disk_stat,
      [this](std::unique_ptr<ss::http::request> request) {
          return put_disk_stat_handler(std::move(request));
      });

    register_route<superuser>(
      ss::httpd::debug_json::override_broker_uuid,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return override_node_uuid_handler(std::move(req));
      });
    register_route<user>(
      ss::httpd::debug_json::get_broker_uuid,
      [this](std::unique_ptr<ss::http::request>)
        -> ss::future<ss::json::json_return_type> {
          return get_node_uuid_handler();
      });
}

using admin::apply_validator;

ss::future<ss::json::json_return_type>
admin_server::cpu_profile_handler(std::unique_ptr<ss::http::request> req) {
    vlog(adminlog.info, "Request to sampled cpu profile");

    std::optional<size_t> shard_id;
    if (auto e = req->get_query_param("shard"); !e.empty()) {
        try {
            shard_id = boost::lexical_cast<size_t>(e);
        } catch (const boost::bad_lexical_cast&) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid parameter 'shard_id' value {{{}}}", e));
        }
    }

    if (shard_id.has_value()) {
        auto all_cpus = ss::smp::all_cpus();
        auto max_shard_id = std::max_element(all_cpus.begin(), all_cpus.end());
        if (*shard_id > *max_shard_id) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "Shard id too high, max shard id is {}", *max_shard_id));
        }
    }

    std::optional<std::chrono::milliseconds> wait_ms;
    if (auto e = req->get_query_param("wait_ms"); !e.empty()) {
        try {
            wait_ms = std::chrono::milliseconds(
              boost::lexical_cast<uint64_t>(e));
        } catch (const boost::bad_lexical_cast&) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid parameter 'wait_ms' value {{{}}}", e));
        }
    }

    if (wait_ms.has_value()) {
        if (*wait_ms < 1ms || *wait_ms > 15min) {
            throw ss::httpd::bad_param_exception(
              "wait_ms must be between 1ms and 15min");
        }
    }

    std::vector<resources::cpu_profiler::shard_samples> profiles;
    if (!wait_ms) {
        profiles = co_await _cpu_profiler.local().results(shard_id);
    } else {
        profiles = co_await _cpu_profiler.local().collect_results_for_period(
          *wait_ms, shard_id);
    }

    co_return co_await ss::make_ready_future<ss::json::json_return_type>(
      ss::json::stream_range_as_array(
        lw_shared_container(std::move(profiles)),
        [](const resources::cpu_profiler::shard_samples& profile) {
            ss::httpd::debug_json::cpu_profile_shard_samples ret;
            ret.shard_id = profile.shard;
            ret.dropped_samples = profile.dropped_samples;

            for (auto& sample : profile.samples) {
                ss::httpd::debug_json::cpu_profile_sample s;
                s.occurrences = sample.occurrences;
                s.user_backtrace = sample.user_backtrace;

                ret.samples.push(s);
            }
            return ret;
        }));
}

ss::future<ss::json::json_return_type>
admin_server::get_local_offsets_translated_handler(
  std::unique_ptr<ss::http::request> req) {
    static const std::string_view to_kafka = "kafka";
    static const std::string_view to_rp = "redpanda";
    const model::ntp ntp = parse_ntp_from_request(req->param);
    const auto shard = _controller->get_shard_table().local().shard_for(ntp);
    auto translate_to = to_kafka;
    if (auto e = req->get_query_param("translate_to"); !e.empty()) {
        if (e != to_kafka && e != to_rp) {
            throw ss::httpd::bad_request_exception(fmt::format(
              "'translate_to' parameter must be one of either {} or {}",
              to_kafka,
              to_rp));
        }
        translate_to = e;
    }
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format("ntp {} could not be found on the node", ntp));
    }
    const auto doc = co_await parse_json_body(req.get());
    if (!doc.IsArray()) {
        throw ss::httpd::bad_request_exception(
          "Request body must be JSON array of integers");
    }
    std::vector<model::offset> input;
    for (auto& item : doc.GetArray()) {
        if (!item.IsInt()) {
            throw ss::httpd::bad_request_exception(
              "Offsets must all be integers");
        }
        input.emplace_back(item.GetInt());
    }
    co_return co_await _controller->get_partition_manager().invoke_on(
      *shard,
      [ntp, translate_to, input = std::move(input)](
        cluster::partition_manager& pm) {
          auto partition = pm.get(ntp);
          if (!partition) {
              return ss::make_exception_future<ss::json::json_return_type>(
                ss::httpd::not_found_exception(fmt::format(
                  "partition with ntp {} could not be found on the node",
                  ntp)));
          }
          const auto& log = partition->log();
          std::vector<ss::httpd::debug_json::translated_offset> result;
          for (const auto offset : input) {
              try {
                  ss::httpd::debug_json::translated_offset to;
                  if (translate_to == to_kafka) {
                      to.kafka_offset = log->from_log_offset(offset);
                      to.rp_offset = offset;
                  } else {
                      to.rp_offset = log->to_log_offset(offset);
                      to.kafka_offset = offset;
                  }
                  result.push_back(std::move(to));
              } catch (const std::runtime_error&) {
                  throw ss::httpd::bad_request_exception(fmt::format(
                    "Offset provided {} was out of offset translator range",
                    offset));
              }
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(result));
      });
}

ss::future<ss::json::json_return_type>
admin_server::cloud_storage_usage_handler(
  std::unique_ptr<ss::http::request> req) {
    auto batch_size
      = cluster::topic_table_partition_generator::default_batch_size;
    if (auto batch_size_param = req->get_query_param("batch_size");
        !batch_size_param.empty()) {
        try {
            batch_size = std::stoi(batch_size_param);
        } catch (...) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "batch_size must be an integer: {}", batch_size_param));
        }
    }

    auto retries_allowed
      = cluster::cloud_storage_size_reducer::default_retries_allowed;
    if (auto retries_param = req->get_query_param("retries_allowed");
        !retries_param.empty()) {
        try {
            retries_allowed = std::stoi(retries_param);
        } catch (...) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "retries_allowed must be an integer: {}", retries_param));
        }
    }

    cluster::cloud_storage_size_reducer reducer(
      _controller->get_topics_state(),
      _controller->get_members_table(),
      _controller->get_partition_leaders(),
      _connection_cache,
      batch_size,
      retries_allowed);

    auto res = co_await reducer.reduce();

    if (res) {
        co_return ss::json::json_return_type(res.value());
    } else {
        throw ss::httpd::base_exception(
          fmt::format("Failed to generate total cloud storage usage. "
                      "Please retry."),
          ss::http::reply::status_type::service_unavailable);
    }
}

ss::future<ss::json::json_return_type>
admin_server::sampled_memory_profile_handler(
  std::unique_ptr<ss::http::request> req) {
    vlog(adminlog.info, "Request to sampled memory profile");

    std::optional<size_t> shard_id;
    if (auto e = req->get_query_param("shard"); !e.empty()) {
        try {
            shard_id = boost::lexical_cast<size_t>(e);
        } catch (const boost::bad_lexical_cast&) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid parameter 'shard_id' value {{{}}}", e));
        }
    }

    if (shard_id.has_value()) {
        auto max_shard_id = ss::smp::count;
        if (*shard_id > max_shard_id) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "Shard id too high, max shard id is {}", max_shard_id));
        }
    }

    auto profiles = co_await _memory_sampling_service.local()
                      .get_sampled_memory_profiles(shard_id);

    std::vector<ss::httpd::debug_json::memory_profile> resp(profiles.size());
    for (size_t i = 0; i < resp.size(); ++i) {
        resp[i].shard = profiles[i].shard_id;

        for (auto& allocation_sites : profiles[i].allocation_sites) {
            ss::httpd::debug_json::allocation_site allocation_site;
            allocation_site.size = allocation_sites.size;
            allocation_site.count = allocation_sites.count;
            allocation_site.backtrace = std::move(allocation_sites.backtrace);
            resp[i].allocation_sites.push(allocation_site);
        }
    }

    co_return co_await ss::make_ready_future<ss::json::json_return_type>(
      std::move(resp));
}

ss::future<ss::json::json_return_type>
admin_server::get_local_storage_usage_handler(
  std::unique_ptr<ss::http::request>) {
    /*
     * In order to get an accurate view of disk usage we need to account for
     * three things:
     *
     * 1. partition-based log usage (controller, kafka topics, etc...)
     * 2. non-partition-based logs (kvstore, ...)
     * 3. everything else (stm snapshots, etc...)
     *
     * The storage API disk usage interface gives is 1 and 2. But we need to
     * access the partition abstraction to reason about the usage that state
     * machines and raft account for.
     *
     * TODO: this accumulation across sub-systems will be moved up a level in
     * short order and wont' remain here in the admin interface for long.
     */
    const auto other
      = co_await _partition_manager.local().non_log_disk_size_bytes();

    const auto disk = co_await _controller->get_storage().local().disk_usage();

    seastar::httpd::debug_json::local_storage_usage ret;
    ret.data = disk.usage.data + other;
    ret.index = disk.usage.index;
    ret.compaction = disk.usage.compaction;
    ret.reclaimable_by_retention = disk.reclaim.retention;
    ret.target_min_capacity = disk.target.min_capacity;
    ret.target_min_capacity_wanted = disk.target.min_capacity_wanted;

    if (_cloud_storage_cache.local_is_initialized()) {
        auto [cache_bytes, cache_objects]
          = co_await _cloud_storage_cache.invoke_on(
            ss::shard_id{0},
            [](cloud_storage::cache& cache) -> std::pair<uint64_t, size_t> {
                return {cache.get_usage_bytes(), cache.get_usage_objects()};
            });

        ret.cloud_storage_cache_bytes = cache_bytes;
        ret.cloud_storage_cache_objects = cache_objects;
    } else {
        ret.cloud_storage_cache_bytes = 0;
        ret.cloud_storage_cache_objects = 0;
    }

    co_return ret;
}

ss::future<ss::json::json_return_type>
admin_server::get_partition_state_handler(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);
    auto result = co_await get_partition_state(ntp, *_controller);
    if (result.has_error()) {
        throw ss::httpd::server_error_exception(fmt::format(
          "Error {} processing partition state for ntp: {}",
          result.error(),
          ntp));
    }

    ss::httpd::debug_json::partition_state response;
    response.ntp = fmt::format("{}", ntp);
    const auto& states = result.value();
    for (const auto& state : states) {
        ss::httpd::debug_json::partition_replica_state replica;
        replica.start_offset = state.start_offset;
        replica.committed_offset = state.committed_offset;
        replica.last_stable_offset = state.last_stable_offset;
        replica.high_watermark = state.high_water_mark;
        replica.dirty_offset = state.dirty_offset;
        replica.latest_configuration_offset = state.latest_configuration_offset;
        replica.revision_id = state.revision_id;
        replica.log_size_bytes = state.log_size_bytes;
        replica.non_log_disk_size_bytes = state.non_log_disk_size_bytes;
        replica.is_read_replica_mode_enabled
          = state.is_read_replica_mode_enabled;
        replica.read_replica_bucket = state.read_replica_bucket;
        replica.is_remote_fetch_enabled = state.is_remote_fetch_enabled;
        replica.is_cloud_data_available = state.is_cloud_data_available;
        replica.start_cloud_offset = state.start_cloud_offset;
        replica.next_cloud_offset = state.next_cloud_offset;
        replica.iceberg_enabled = state.iceberg_enabled;
        fill_raft_state(replica, std::move(state));
        response.replicas.push(std::move(replica));
    }
    co_return ss::json::json_return_type(std::move(response));
}

ss::future<ss::json::json_return_type> admin_server::get_node_uuid_handler() {
    ss::httpd::debug_json::broker_uuid uuid;
    uuid.node_uuid = ssx::sformat(
      "{}", _controller->get_storage().local().node_uuid());

    if (config::node().node_id().has_value()) {
        uuid.node_id = config::node().node_id().value();
    }

    co_return ss::json::json_return_type(std::move(uuid));
}

static json::validator make_broker_id_override_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "current_node_uuid": {
            "type": "string"
        },
        "new_node_uuid": {
            "type": "string"
        },
        "new_node_id": {
          "type" : "integer"
        }
    },
    "additionalProperties": false,
    "required": ["current_node_uuid", "new_node_id","new_node_uuid"]
})";
    return json::validator(schema);
}

namespace {
ss::future<> override_id_and_uuid(
  storage::kvstore& kvs, model::node_uuid uuid, model::node_id id) {
    static const auto node_uuid_key = bytes::from_string("node_uuid");
    co_await kvs.put(
      storage::kvstore::key_space::controller,
      node_uuid_key,
      serde::to_iobuf(uuid));
    auto invariants_iobuf = kvs.get(
      storage::kvstore::key_space::controller,
      cluster::controller::invariants_key());
    if (invariants_iobuf) {
        auto invariants
          = reflection::from_iobuf<cluster::configuration_invariants>(
            std::move(invariants_iobuf.value()));
        invariants.node_id = id;

        co_await kvs.put(
          storage::kvstore::key_space::controller,
          cluster::controller::invariants_key(),
          reflection::to_iobuf(std::move(invariants)));
    }
}
} // namespace

ss::future<ss::json::json_return_type> admin_server::override_node_uuid_handler(
  std::unique_ptr<ss::http::request> req) {
    static thread_local auto override_id_validator(
      make_broker_id_override_validator());
    const auto doc = co_await parse_json_body(req.get());
    /**
     * Validate the request body
     */
    using admin::apply_validator;
    apply_validator(override_id_validator, doc);
    /**
     * Validate if current UUID matches this node UUID, if not request an error
     * is returned as the request may have been sent to incorrect node
     */
    model::node_uuid current_uuid;
    try {
        current_uuid = model::node_uuid(
          uuid_t::from_string(doc["current_node_uuid"].GetString()));
    } catch (const std::runtime_error& e) {
        throw ss::httpd::bad_request_exception(ssx::sformat(
          "failed parsing current_node_uuid: {} - {}",
          doc["current_node_uuid"].GetString(),
          e.what()));
    }
    auto& storage = _controller->get_storage().local();
    if (storage.node_uuid() != current_uuid) {
        throw ss::httpd::bad_request_exception(ssx::sformat(
          "Requested current node UUID: {} does not match node UUID: {}",
          storage.node_uuid(),
          current_uuid));
    }
    model::node_uuid new_node_uuid;
    try {
        new_node_uuid = model::node_uuid(
          uuid_t::from_string(doc["new_node_uuid"].GetString()));
    } catch (const std::runtime_error& e) {
        throw ss::httpd::bad_request_exception(ssx::sformat(
          "failed parsing new_node_uuid: {} - {}",
          doc["new_node_uuid"].GetString(),
          e.what()));
    }
    model::node_id new_node_id;
    try {
        new_node_id = model::node_id(doc["new_node_id"].GetInt());
        if (new_node_id < model::node_id{0}) {
            throw ss::httpd::bad_request_exception(
              "node_id must not be negative");
        }
    } catch (const std::runtime_error& e) {
        throw ss::httpd::bad_request_exception(
          ssx::sformat("failed parsing new node_id: {}", e.what()));
    }

    vlog(
      adminlog.warn,
      "Requested to override node id with new node id: {} and UUID: {}",
      new_node_id,
      new_node_uuid);

    co_await _controller->get_storage().invoke_on(
      cluster::controller_stm_shard,
      [new_node_uuid, new_node_id](storage::api& local_storage) {
          return override_id_and_uuid(
            local_storage.kvs(), new_node_uuid, new_node_id);
      });

    co_return ss::json::json_return_type(ss::json::json_void());
}
