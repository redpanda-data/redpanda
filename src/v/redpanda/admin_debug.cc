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
#include "cluster/shard_table.h"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "redpanda/admin_server.h"

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

ss::future<ss::json::json_return_type>
admin_server::restart_service_handler(std::unique_ptr<ss::http::request> req) {
    auto service_param = req->get_query_param("service");
    std::optional<service_kind> service = from_string_view<service_kind>(
      service_param);
    if (!service.has_value()) {
        throw ss::httpd::not_found_exception(
          fmt::format("Invalid service: {}", service_param));
    }

    vlog(
      adminlog.info, "Restart redpanda service: {}", to_string_view(*service));
    co_await restart_redpanda_service(*service);
    co_return ss::json::json_return_type(ss::json::json_void());
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
