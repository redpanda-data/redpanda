/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/table_id_provider.h"

#include "config/configuration.h"

#include <boost/algorithm/string/replace.hpp>

namespace datalake {

model::topic table_id_provider::sanitize_topic_name(const model::topic& topic) {
    const auto& dot_replacement
      = config::shard_local_cfg().iceberg_topic_name_dot_replacement();
    if (!dot_replacement.has_value()) {
        return topic;
    }
    return model::topic(
      boost::replace_all_copy(topic(), ".", *dot_replacement));
}

iceberg::table_identifier table_id_provider::table_id(const model::topic& t) {
    return {
      // TODO: namespace as a topic property? Keep it in the table metadata?
      .ns = {"redpanda"},
      .table = sanitize_topic_name(t),
    };
}

iceberg::table_identifier
table_id_provider::dlq_table_id(const model::topic& t) {
    return {
      // TODO: namespace as a topic property? Keep it in the table metadata?
      .ns = {"redpanda"},
      .table = fmt::format("{}~dlq", sanitize_topic_name(t)),
    };
}

} // namespace datalake
