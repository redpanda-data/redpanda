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

#pragma once

#include "config/convert.h"
#include "model/fundamental.h"
#include "ssx/sformat.h"

#include <yaml-cpp/yaml.h>

#include <iosfwd>
#include <vector>

namespace config {

/**
 * In-memory representation of a Node ID/UUID override candidate.
 *
 * Semantics surrounding when and how to apply an override
 * are not represented here and should be carefully considered at the
 * point of usage.
 */
struct node_id_override {
    node_id_override() = default;
    node_id_override(
      model::node_uuid key,
      model::node_uuid uuid_value,
      model::node_id id_value)
      : key(key)
      , uuid(uuid_value)
      , id(id_value) {}
    model::node_uuid key{};
    model::node_uuid uuid{};
    model::node_id id{};

private:
    friend std::ostream&
    operator<<(std::ostream& os, const node_id_override& v);
    friend std::istream& operator>>(std::istream& is, node_id_override& v);
    friend bool operator==(const node_id_override&, const node_id_override&)
      = default;
};

/**
 * Thin data structure to encapsulate 'node_id_override' filtering and
 * short-term storage.
 *
 * Given an array of 'node_id_override's, select the appropriate override as:
 *
 * For some instance 'node_id_override O', 'O.uuid' and 'O.id' should
 * be applied to some node if and only if its _current_ UUID matches
 * 'O.key'.
 */
struct node_override_store {
    node_override_store() = default;

    /**
     * From the provided vector, accept the override (if one exists) whose
     * 'key' matches 'node_uuid'.
     *
     * Throws if multiple overrides match 'node_uuid'
     */
    void maybe_set_overrides(
      model::node_uuid node_uuid,
      const std::vector<config::node_id_override>& overrides);

    const std::optional<model::node_uuid>& node_uuid() const noexcept;
    const std::optional<model::node_id>& node_id() const noexcept;

private:
    std::optional<model::node_uuid> _uuid_override;
    std::optional<model::node_id> _id_override;
};

} // namespace config

namespace YAML {

template<>
struct convert<config::node_id_override> {
    using type = config::node_id_override;
    static inline Node encode(const type& rhs) {
        Node node;
        node["current_uuid"] = ssx::sformat("{}", rhs.key);
        node["new_uuid"] = ssx::sformat("{}", rhs.uuid);
        node["new_id"] = ssx::sformat("{}", rhs.id);

        return node;
    }

    static inline bool decode(const Node& node, type& rhs) {
        if (!node["current_uuid"] || !node["new_uuid"] || !node["new_id"]) {
            return false;
        }
        rhs.key = node["current_uuid"].as<model::node_uuid>();
        rhs.uuid = node["new_uuid"].as<model::node_uuid>();
        rhs.id = node["new_id"].as<model::node_id>();
        return true;
    }
};

} // namespace YAML
