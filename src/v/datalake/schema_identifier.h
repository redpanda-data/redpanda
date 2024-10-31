/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "pandaproxy/schema_registry/types.h"
#include "serde/rw/envelope.h"

#include <boost/container_hash/hash.hpp>

#include <optional>

namespace datalake {

// Uniquely identifies the structure of a record component schema as it exists
// in the schema registry.
struct schema_identifier
  : serde::
      envelope<schema_identifier, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    auto serde_fields() { return std::tie(schema_id, protobuf_offsets); }

    pandaproxy::schema_registry::schema_id schema_id;
    std::optional<std::vector<int32_t>> protobuf_offsets;
    bool operator==(const schema_identifier&) const = default;
};

// The components required to build the Iceberg schema of a record.
struct record_schema_components
  : serde::envelope<
      record_schema_components,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    auto serde_fields() { return std::tie(key_identifier, val_identifier); }

    std::optional<schema_identifier> key_identifier;
    std::optional<schema_identifier> val_identifier;
    bool operator==(const record_schema_components&) const = default;
};

} // namespace datalake

namespace std {
template<>
struct hash<datalake::schema_identifier> {
    size_t operator()(const datalake::schema_identifier& id) const {
        namespace ppsr = pandaproxy::schema_registry;
        size_t h = 0;
        boost::hash_combine(h, hash<ppsr::schema_id>()(id.schema_id));
        if (id.protobuf_offsets.has_value()) {
            for (auto i : id.protobuf_offsets.value()) {
                boost::hash_combine(h, hash<int32_t>()(i));
            }
        }
        return h;
    }
};
template<>
struct hash<datalake::record_schema_components> {
    size_t operator()(const datalake::record_schema_components& c) const {
        size_t h = 0;
        if (c.key_identifier.has_value()) {
            boost::hash_combine(
              h, hash<datalake::schema_identifier>()(*c.key_identifier));
        }
        if (c.val_identifier.has_value()) {
            boost::hash_combine(
              h, hash<datalake::schema_identifier>()(*c.val_identifier));
        }
        return h;
    }
};
} // namespace std
