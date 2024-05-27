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
#include "cluster/data_migration_types.h"

#include "ssx/sformat.h"
#include "utils/to_string.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

namespace cluster {

namespace {
ss::sstring print_migration(const data_migration& dm) {
    return ss::visit(
      dm,
      [&](const inbound_data_migration& idm) {
          return ssx::sformat("{{inbound_migration: {}}}", idm);
      },
      [&](const outbound_data_migration& odm) {
          return ssx::sformat("{{outbound_migration: {}}}", odm);
      });
}
} // namespace
data_migration copy_migration(const data_migration& migration) {
    if (std::holds_alternative<inbound_data_migration>(migration)) {
        return std::get<inbound_data_migration>(migration).copy();
    }
    return std::get<outbound_data_migration>(migration).copy();
}

outbound_data_migration outbound_data_migration::copy() const {
    return outbound_data_migration{
      .topics = topics.copy(), .groups = groups.copy(), .copy_to = copy_to};
}
inbound_data_migration inbound_data_migration::copy() const {
    return inbound_data_migration{
      .topics = topics.copy(), .groups = groups.copy()};
}

std::ostream& operator<<(std::ostream& o, data_migration_state state) {
    switch (state) {
    case data_migration_state::planned:
        return o << "planned";
    case data_migration_state::preparing:
        return o << "preparing";
    case data_migration_state::prepared:
        return o << "prepared";
    case data_migration_state::executing:
        return o << "executing";
    case data_migration_state::executed:
        return o << "executed";
    case data_migration_state::finished:
        return o << "finished";
    case data_migration_state::canceling:
        return o << "canceling";
    case data_migration_state::cancelled:
        return o << "cancelled";
    }
}

std::ostream& operator<<(std::ostream& o, migrated_resource_state state) {
    switch (state) {
    case migrated_resource_state::non_restricted:
        return o << "non-restricted";
    case migrated_resource_state::restricted:
        return o << "restricted";
    case migrated_resource_state::blocked:
        return o << "blocked";
    }
}

std::ostream& operator<<(std::ostream& o, const inbound_topic& topic) {
    fmt::print(
      o,
      "{{source_topic_name: {}, alias: {}, cloud_storage_location: {}}}",
      topic.source_topic_name,
      topic.alias,
      topic.cloud_storage_location);
    return o;
}

std::ostream& operator<<(std::ostream& o, const cloud_storage_location&) {
    fmt::print(o, "{{}}");
    return o;
}
std::ostream& operator<<(std::ostream& o, const copy_target& t) {
    fmt::print(o, "{{bucket: {}}}", t.bucket);
    return o;
}

std::ostream& operator<<(std::ostream& o, const inbound_data_migration& dm) {
    fmt::print(
      o,
      "{{topics: {}, consumer_groups: {}}}",
      fmt::join(dm.topics, ", "),
      fmt::join(dm.groups, ", "));
    return o;
}

std::ostream& operator<<(std::ostream& o, const outbound_data_migration& dm) {
    fmt::print(
      o,
      "{{topics: {}, consumer_groups: {}, copy_to: {}}}",
      fmt::join(dm.topics, ", "),
      fmt::join(dm.groups, ", "),
      dm.copy_to);
    return o;
}

std::ostream& operator<<(std::ostream& o, const data_migration_metadata& m) {
    fmt::print(
      o,
      "{{id: {}, migration: {}, state: {}}}",
      m.id,
      print_migration(m.migration),
      m.state);
    return o;
}
