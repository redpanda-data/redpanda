/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/utils/helpers.h"

#include <algorithm>

coproc::enable_copros_request::data make_enable_req(
  uint32_t id,
  std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>> topics) {
    std::vector<coproc::enable_copros_request::data::topic_mode>
      topics_and_modes;
    topics_and_modes.reserve(topics.size());
    for (auto& p : topics) {
        topics_and_modes.emplace_back(
          model::topic(std::move(p.first)), p.second);
    }
    return coproc::enable_copros_request::data{
      .id = coproc::script_id(id), .topics = std::move(topics_and_modes)};
}

ss::future<result<rpc::client_context<coproc::enable_copros_reply>>>
register_coprocessors(
  rpc::client<coproc::script_manager_client_protocol>& client,
  std::vector<coproc::enable_copros_request::data>&& data) {
    coproc::enable_copros_request req{.inputs = std::move(data)};
    return client.enable_copros(
      std::move(req), rpc::client_opts(rpc::no_timeout));
}

ss::future<result<rpc::client_context<coproc::disable_copros_reply>>>
deregister_coprocessors(
  rpc::client<coproc::script_manager_client_protocol>& client,
  std::vector<uint32_t>&& sids) {
    std::vector<coproc::script_id> script_ids;
    script_ids.reserve(sids.size());
    std::transform(
      sids.begin(),
      sids.end(),
      std::back_inserter(script_ids),
      [](uint32_t id) { return coproc::script_id(id); });
    coproc::disable_copros_request req{.ids = std::move(script_ids)};
    return client.disable_copros(
      std::move(req), rpc::client_opts(rpc::no_timeout));
}
