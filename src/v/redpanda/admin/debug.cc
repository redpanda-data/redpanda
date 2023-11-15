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
#include "cluster/cloud_storage_size_reducer.h"
#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "redpanda/admin/server.h"

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
          std::vector<result_t> ans;

          auto leaders_info = _metadata_cache.local().get_leaders();
          ans.reserve(leaders_info.size());
          for (const auto& leader_info : leaders_info) {
              result_t info;
              info.ns = leader_info.tp_ns.ns;
              info.topic = leader_info.tp_ns.tp;
              info.partition_id = leader_info.pid;
              info.leader = leader_info.current_leader.has_value()
                              ? leader_info.current_leader.value()
                              : -1;
              info.previous_leader = leader_info.previous_leader.has_value()
                                       ? leader_info.previous_leader.value()
                                       : -1;
              info.last_stable_leader_term
                = leader_info.last_stable_leader_term;
              info.update_term = leader_info.update_term;
              info.partition_revision = leader_info.partition_revision;
              ans.push_back(std::move(info));
          }

          return ss::make_ready_future<ss::json::json_return_type>(ans);
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
}

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

    auto profiles = co_await _cpu_profiler.local().results(shard_id);

    std::vector<ss::httpd::debug_json::cpu_profile_shard_samples> response{
      profiles.size()};
    for (size_t i = 0; i < profiles.size(); i++) {
        response[i].shard_id = profiles[i].shard;
        response[i].dropped_samples = profiles[i].dropped_samples;

        for (auto& sample : profiles[i].samples) {
            ss::httpd::debug_json::cpu_profile_sample s;
            s.occurrences = sample.occurrences;
            s.user_backtrace = sample.user_backtrace;

            response[i].samples.push(s);
        }
    }

    co_return co_await ss::make_ready_future<ss::json::json_return_type>(
      std::move(response));
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
          const auto ots = partition->get_offset_translator_state();
          std::vector<ss::httpd::debug_json::translated_offset> result;
          for (const auto offset : input) {
              try {
                  ss::httpd::debug_json::translated_offset to;
                  if (translate_to == to_kafka) {
                      to.kafka_offset = ots->from_log_offset(offset);
                      to.rp_offset = offset;
                  } else {
                      to.rp_offset = ots->to_log_offset(offset);
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
