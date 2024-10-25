/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "redpanda/admin/data_migration_utils.h"

#include "model/namespace.h"

#include <absl/strings/str_split.h>

#include <string_view>

model::topic_namespace parse_topic_namespace(json::Value& json) {
    if (json.HasMember("ns")) {
        return {
          model::ns(json["ns"].GetString()),
          model::topic(json["topic"].GetString())};
    } else {
        return {
          model::kafka_namespace, model::topic(json["topic"].GetString())};
    }
}

cluster::data_migrations::inbound_topic parse_inbound_topic(json::Value& json) {
    cluster::data_migrations::inbound_topic ret;
    ret.source_topic_name = parse_topic_namespace(
      json["source_topic_reference"]);

    // extract location hint from topic name
    std::pair<std::string, std::string> split = absl::StrSplit(
      std::string_view(model::topic_view(ret.source_topic_name.tp)),
      absl::MaxSplits('/', 1));
    if (split.second != "") {
        ret.source_topic_name.tp = model::topic(split.first);
        ret.cloud_storage_location = {.hint = split.second};
    }

    if (json.HasMember("alias")) {
        ret.alias = parse_topic_namespace(json["alias"]);
    }
    return ret;
}

chunked_vector<cluster::data_migrations::inbound_topic>
parse_inbound_topics(json::Value& json) {
    chunked_vector<cluster::data_migrations::inbound_topic> ret;
    auto topics_array = json["topics"].GetArray();
    ret.reserve(topics_array.Size());
    std::ranges::transform(
      topics_array, std::back_inserter(ret), &parse_inbound_topic);

    return ret;
}

chunked_vector<model::topic_namespace> parse_topics(json::Value& json) {
    chunked_vector<model::topic_namespace> ret;
    auto topics_array = json["topics"].GetArray();
    ret.reserve(topics_array.Size());
    std::ranges::transform(
      topics_array, std::back_inserter(ret), &parse_topic_namespace);

    return ret;
}
