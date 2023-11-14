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
