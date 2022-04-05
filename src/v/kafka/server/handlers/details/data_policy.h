/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/server/handlers/topics/types.h"
#include "v8_engine/data_policy.h"
#include "vlog.h"

namespace kafka {

struct data_policy_parser {
    cluster::incremental_update_operation op{
      cluster::incremental_update_operation::none};

    std::optional<ss::sstring> function_name;
    std::optional<ss::sstring> script_name;
};

inline std::optional<v8_engine::data_policy>
data_policy_from_parser(const data_policy_parser& parser) {
    if (!parser.function_name.has_value() && !parser.script_name.has_value()) {
        return std::nullopt;
    }

    if (!parser.function_name.has_value()) {
        throw v8_engine::data_policy_exeption(fmt_with_ctx(
          fmt::format,
          "Can not find {} in update for data-policy",
          topic_property_data_policy_function_name));
    }

    if (!parser.script_name.has_value()) {
        throw v8_engine::data_policy_exeption(fmt_with_ctx(
          fmt::format,
          "Can not find {} in update for data-policy",
          topic_property_data_policy_script_name));
    }

    return v8_engine::data_policy(
      parser.function_name.value(), parser.script_name.value());
}

inline void update_data_policy_op(
  data_policy_parser& parser, cluster::incremental_update_operation op) {
    if (
      parser.op != cluster::incremental_update_operation::none
      && parser.op != op) {
        throw v8_engine::data_policy_exeption(fmt_with_ctx(
          fmt::format,
          "Operation for redpanda.datapolicy can not be different(previous:{}, "
          "current:{})",
          parser.op,
          op));
    } else if (parser.op == cluster::incremental_update_operation::none) {
        parser.op = op;
    }
}

inline bool update_data_policy_parser(
  data_policy_parser& parser,
  std::string_view key,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    if (op == config_resource_operation::remove) {
        update_data_policy_op(
          parser, cluster::incremental_update_operation::remove);
    }
    if (op == config_resource_operation::set) {
        update_data_policy_op(
          parser, cluster::incremental_update_operation::set);
    }

    if (key == topic_property_data_policy_function_name) {
        parser.function_name = value;
        return true;
    }

    if (key == topic_property_data_policy_script_name) {
        parser.script_name = value;
        return true;
    }

    return false;
}

} // namespace kafka
