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

namespace cluster::data_migrations {
namespace {
ss::sstring print_migration(const data_migration& dm) {
    return ss::visit(
      dm,
      [&](const inbound_migration& idm) {
          return ssx::sformat("{{inbound_migration: {}}}", idm);
      },
      [&](const outbound_migration& odm) {
          return ssx::sformat("{{outbound_migration: {}}}", odm);
      });
}
} // namespace
data_migration copy_migration(const data_migration& migration) {
    return std::visit(
      [](const auto& migration) { return data_migration{migration.copy()}; },
      migration);
}

outbound_migration outbound_migration::copy() const {
    return outbound_migration{
      .topics = topics.copy(), .groups = groups.copy(), .copy_to = copy_to};
}
inbound_migration inbound_migration::copy() const {
    return inbound_migration{.topics = topics.copy(), .groups = groups.copy()};
}

std::ostream& operator<<(std::ostream& o, state state) {
    switch (state) {
    case state::planned:
        return o << "planned";
    case state::preparing:
        return o << "preparing";
    case state::prepared:
        return o << "prepared";
    case state::executing:
        return o << "executing";
    case state::executed:
        return o << "executed";
    case state::finished:
        return o << "finished";
    case state::canceling:
        return o << "canceling";
    case state::cancelled:
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
    fmt::print(o, "{{cloud_storage_location}}");
    return o;
}
std::ostream& operator<<(std::ostream& o, const copy_target& t) {
    fmt::print(o, "{{bucket: {}}}", t.bucket);
    return o;
}

std::ostream& operator<<(std::ostream& o, const inbound_migration& dm) {
    fmt::print(
      o,
      "{{topics: {}, consumer_groups: {}}}",
      fmt::join(dm.topics, ", "),
      fmt::join(dm.groups, ", "));
    return o;
}

std::ostream& operator<<(std::ostream& o, const outbound_migration& dm) {
    fmt::print(
      o,
      "{{topics: {}, consumer_groups: {}, copy_to: {}}}",
      fmt::join(dm.topics, ", "),
      fmt::join(dm.groups, ", "),
      dm.copy_to);
    return o;
}

std::ostream& operator<<(std::ostream& o, const migration_metadata& m) {
    fmt::print(
      o,
      "{{id: {}, migration: {}, state: {}}}",
      m.id,
      print_migration(m.migration),
      m.state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_migration_cmd_data& d) {
    fmt::print(
      o, "{{id: {}, migration: {}}}", d.id, print_migration(d.migration));
    return o;
}

std::ostream&
operator<<(std::ostream& o, const update_migration_state_cmd_data& d) {
    fmt::print(o, "{{id: {}, requested_state: {}}}", d.id, d.requested_state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const remove_migration_cmd_data& d) {
    fmt::print(o, "{{id: {}}}", d.id);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_migration_request& r) {
    fmt::print(o, "{{migration: {}}}", print_migration(r.migration));
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_migration_reply& r) {
    fmt::print(o, "{{id: {}, error_code: {}}}", r.id, r.ec);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const update_migration_state_request& r) {
    fmt::print(o, "{{id: {}, state: {}}}", r.id, r.state);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const update_migration_state_reply& r) {
    fmt::print(o, "{{error_code: {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const remove_migration_request& r) {
    fmt::print(o, "{{id: {}}}", r.id);
    return o;
}

std::ostream& operator<<(std::ostream& o, const remove_migration_reply& r) {
    fmt::print(o, "{{error_code: {}}}", r.ec);
    return o;
}

} // namespace cluster::data_migrations
