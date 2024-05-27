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

#include "cluster/errc.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"
#include "serde/envelope.h"
#include "serde/rw/variant.h"
#include "utils/named_type.h"

#include <absl/container/flat_hash_set.h>

namespace cluster {
/**
 * Identifier of data migration, the identifier is guaranteed to be unique
 * within the same cluster but migrations in different clusters may have the
 * same identifiers.
 */
using data_migration_id = named_type<int64_t, struct data_migration_type_tag>;
using consumer_group = named_type<ss::sstring, struct consumer_group_tag>;
/**
 * Migration state
 *  ┌─────────┐
 *  │ planned ├────────────────────┐
 *  └────┬────┘                    │
 *       │                         │
 * ┌─────▼─────┐                   │
 * │ preparing ├─────────────────┐ │
 * └─────┬─────┘                 │ │
 *       │                       │ │
 * ┌─────▼────┐          ┌───────▼─▼──┐
 * │ prepared ├──────────► cancelling │
 * └─────┬────┘          └▲──▲─┬──────┘
 *       │                │  │ │
 * ┌─────▼─────┐          │  │ │
 * │ executing ├──────────┘  │ │
 * └─────┬─────┘             │ │
 *       │                   │ │
 * ┌─────▼────┐              │ │
 * │ executed ├──────────────┘ │
 * └─────┬────┘                │
 *       │                     │
 * ┌─────▼────┐          ┌─────▼─────┐
 * │ finished │          │ cancelled │
 * └──────────┘          └───────────┘
 */
enum class data_migration_state {
    planned,
    preparing,
    prepared,
    executing,
    executed,
    finished,
    canceling,
    cancelled,
};
std::ostream& operator<<(std::ostream& o, data_migration_state);

/**
 * State of migrated resource i.e. either topic or consumer group, when resource
 * is blocked all the writes should be disabled, when it is restricted a
 * resource properties should not be updated.
 */
enum class migrated_resource_state {
    non_restricted = 0,
    restricted = 1,
    blocked = 2,
};

std::ostream& operator<<(std::ostream& o, migrated_resource_state);

/**
 * All migration related services other than data_migrations_frontend are only
 * instantiated on single shard.
 */
static constexpr ss::shard_id data_migrations_shard = 0;

/**
 * Override indicating a location of the inbound topic data
 * TODO: consult storage team to fill in the details.
 */
struct cloud_storage_location
  : serde::envelope<
      cloud_storage_location,
      serde::version<0>,
      serde::compat_version<0>> {
    friend std::ostream&
    operator<<(std::ostream&, const cloud_storage_location&);

    friend bool
    operator==(const cloud_storage_location&, const cloud_storage_location&)
      = default;
};

/**
 * Inbound topic describes a topic that ownership is being acquired in the
 * migration process.
 *
 * Inbound topic defines a source topic name and an optional alias. The alias
 * may be used to acquire an ownership under a different name.
 */
struct inbound_topic
  : serde::
      envelope<inbound_topic, serde::version<0>, serde::compat_version<0>> {
    // name of the source topic to acquire ownership in the migration process.
    model::topic_namespace source_topic_name;

    // alias name of the topic on the cluster that acquires the ownership
    std::optional<model::topic_namespace> alias;

    // parameter describing non standard location of the inbound topic data in
    // the object store
    std::optional<cloud_storage_location> cloud_storage_location;

    const model::topic_namespace& effective_topic_name() const {
        if (alias.has_value()) {
            return *alias;
        }
        return source_topic_name;
    }

    auto serde_fields() {
        return std::tie(source_topic_name, alias, cloud_storage_location);
    }

    friend bool operator==(const inbound_topic&, const inbound_topic&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const inbound_topic&);
};
/**
 * Inbound migration object representing topics and consumer groups that
 * ownership should be acquired.
 *
 * TODO: think about the consumer group data type.
 */
struct inbound_data_migration
  : serde::envelope<
      inbound_data_migration,
      serde::version<0>,
      serde::compat_version<0>> {
    chunked_vector<inbound_topic> topics;
    chunked_vector<consumer_group> groups;

    inbound_data_migration copy() const;

    auto serde_fields() { return std::tie(topics, groups); }

    friend bool
    operator==(const inbound_data_migration&, const inbound_data_migration&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const inbound_data_migration&);
};

/**
 * Type describing a target where data should be copied in a process of outbound
 * migration.
 *
 * TODO: consult storage team about the details required to connect to different
 * bucket.
 */
struct copy_target
  : serde::envelope<copy_target, serde::version<0>, serde::compat_version<0>> {
    ss::sstring bucket;

    friend bool operator==(const copy_target&, const copy_target&) = default;
    friend std::ostream& operator<<(std::ostream&, const copy_target&);
};

/**
 * Outbound migration object representing topics and consumer groups that
 * ownership should be released.
 */
struct outbound_data_migration
  : serde::envelope<
      outbound_data_migration,
      serde::version<0>,
      serde::compat_version<0>> {
    // topics which ownership should be released
    chunked_vector<model::topic_namespace> topics;
    // consumer groups which ownership should be released
    chunked_vector<consumer_group> groups;
    // optional target where the data should be copied to in the process of
    // migration
    std::optional<copy_target> copy_to;

    outbound_data_migration copy() const;

    auto serde_fields() { return std::tie(topics, groups, copy_to); }

    friend bool
    operator==(const outbound_data_migration&, const outbound_data_migration&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const outbound_data_migration&);
};
/**
 * Variant representing a migration. It can be either inbound or outbound data
 * migration.
 */
using data_migration
  = serde::variant<inbound_data_migration, outbound_data_migration>;

data_migration copy_migration(const data_migration& migration);

/**
 * Data migration metadata containing a migration definition, its id and current
 * state.
 */
struct data_migration_metadata
  : serde::envelope<
      data_migration_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    data_migration_id id;
    data_migration migration;
    /**
     * Data migration starts in a planned state.
     */
    data_migration_state state = data_migration_state::planned;

    data_migration_metadata copy() const {
        return data_migration_metadata{
          .id = id, .migration = copy_migration(migration), .state = state};
    }

    auto serde_fields() { return std::tie(id, migration, state); }

    friend bool
    operator==(const data_migration_metadata&, const data_migration_metadata&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const data_migration_metadata&);
};

struct create_data_migration_cmd_data
  : serde::envelope<
      create_data_migration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    data_migration_id id;
    data_migration migration;

    auto serde_fields() { return std::tie(id, migration); }
    friend bool operator==(
      const create_data_migration_cmd_data&,
      const create_data_migration_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const create_data_migration_cmd_data&);
};

struct update_data_migration_state_cmd_data
  : serde::envelope<
      update_data_migration_state_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    data_migration_id id;
    data_migration_state requested_state;

    auto serde_fields() { return std::tie(id, requested_state); }
    friend bool operator==(
      const update_data_migration_state_cmd_data&,
      const update_data_migration_state_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const update_data_migration_state_cmd_data&);
};

struct remove_data_migration_cmd_data
  : serde::envelope<
      remove_data_migration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    data_migration_id id;

    auto serde_fields() { return std::tie(id); }
    friend bool operator==(
      const remove_data_migration_cmd_data&,
      const remove_data_migration_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const remove_data_migration_cmd_data&);
};

struct create_data_migration_request
  : serde::envelope<
      create_data_migration_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    data_migration migration;

    auto serde_fields() { return std::tie(migration); }
    friend bool operator==(
      const create_data_migration_request&,
      const create_data_migration_request&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const create_data_migration_request&);
};
struct create_data_migration_reply
  : serde::envelope<
      create_data_migration_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    data_migration_id id{-1};
    cluster::errc ec;
    auto serde_fields() { return std::tie(id, ec); }

    friend bool operator==(
      const create_data_migration_reply&, const create_data_migration_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const create_data_migration_reply&);
};

struct update_data_migration_state_request
  : serde::envelope<
      update_data_migration_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    data_migration_id id;
    data_migration_state state;
    auto serde_fields() { return std::tie(id, state); }
    friend bool operator==(
      const update_data_migration_state_request&,
      const update_data_migration_state_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const update_data_migration_state_request&);
};

struct update_data_migration_state_reply
  : serde::envelope<
      update_data_migration_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    cluster::errc ec;
    auto serde_fields() { return std::tie(ec); }

    friend bool operator==(
      const update_data_migration_state_reply&,
      const update_data_migration_state_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const update_data_migration_state_reply&);
};
struct remove_data_migration_request
  : serde::envelope<
      remove_data_migration_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    data_migration_id id;
    auto serde_fields() { return std::tie(id); }
    friend bool operator==(
      const remove_data_migration_request&,
      const remove_data_migration_request&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const remove_data_migration_request&);
};

struct remove_data_migration_reply
  : serde::envelope<
      remove_data_migration_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    cluster::errc ec;

    auto serde_fields() { return std::tie(ec); }

    friend bool operator==(
      const remove_data_migration_reply&, const remove_data_migration_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const remove_data_migration_reply&);
};

} // namespace cluster
