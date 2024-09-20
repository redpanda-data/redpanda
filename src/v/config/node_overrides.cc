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

#include "config/node_overrides.h"

#include "re2/re2.h"

#include <iostream>
#include <string>

namespace config {

std::ostream& operator<<(std::ostream& os, const config::node_id_override& v) {
    return os << ssx::sformat("{}:{}:{}", v.key, v.uuid, v.id);
}

std::istream& operator>>(std::istream& is, config::node_id_override& v) {
    std::string s;
    is >> s;

    static const re2::RE2 pattern{R"(^([^:]+):([^:]+):([^:]+)$)"};
    vassert(pattern.ok(), "Regex compilation failed");

    std::string curr, uuid, id;

    if (!re2::RE2::FullMatch(s, pattern, &curr, &uuid, &id)) {
        throw std::runtime_error(fmt::format(
          R"(Formatting error: '{}', must be of form '<uuid>:<uuid>:<id>')",
          s));
    }

    v.key = boost::lexical_cast<model::node_uuid>(curr);
    v.uuid = boost::lexical_cast<model::node_uuid>(uuid);
    v.id = boost::lexical_cast<model::node_id>(id);

    return is;
}

void node_override_store::maybe_set_overrides(
  model::node_uuid node_uuid,
  const std::vector<config::node_id_override>& overrides) {
    vassert(ss::this_shard_id() == 0, "Only set overrides on shard 0");
    for (const auto& o : overrides) {
        if (o.key == node_uuid) {
            if (_uuid_override.has_value() || _id_override.has_value()) {
                throw std::runtime_error(
                  "Invalid node ID override: Limit one override per broker");
                break;
            }
            _uuid_override.emplace(o.uuid);
            _id_override.emplace(o.id);
        }
    }
}

const std::optional<model::node_uuid>&
node_override_store::node_uuid() const noexcept {
    vassert(ss::this_shard_id() == 0, "Only get overrides on shard 0");
    return _uuid_override;
}

const std::optional<model::node_id>&
node_override_store::node_id() const noexcept {
    vassert(ss::this_shard_id() == 0, "Only get overrides on shard 0");
    return _id_override;
}

} // namespace config
