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

namespace cluster::data_migrations {
/**
 * Identifier of data migration, the identifier is guaranteed to be unique
 * within the same cluster but migrations in different clusters may have the
 * same identifiers.
 */
using id = named_type<int64_t, struct data_migration_type_tag>;
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
enum class state {
    planned,
    preparing,
    prepared,
    executing,
    executed,
    finished,
    canceling,
    cancelled,
};
std::ostream& operator<<(std::ostream& o, state);

/**
 * State of migrated resource i.e. either topic or consumer group, when resource
 * is blocked all the writes should be disabled, when it is restricted a
 * resource properties should not be updated.
 */
enum class migrated_resource_state {
    non_restricted,
    restricted,
    blocked,
};

std::ostream& operator<<(std::ostream& o, migrated_resource_state);

/**
 * All migration related services other than data_migrations_frontend are only
 * instantiated on single shard.
 */
inline constexpr ss::shard_id data_migrations_shard = 0;

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
    // the object store. By default the location will allow to override the
    // cluster UUID and ntp of the source topic.
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
struct inbound_migration
  : serde::
      envelope<inbound_migration, serde::version<0>, serde::compat_version<0>> {
    chunked_vector<inbound_topic> topics;
    chunked_vector<consumer_group> groups;

    inbound_migration copy() const;

    auto serde_fields() { return std::tie(topics, groups); }

    friend bool operator==(const inbound_migration&, const inbound_migration&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const inbound_migration&);
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
struct outbound_migration
  : serde::envelope<
      outbound_migration,
      serde::version<0>,
      serde::compat_version<0>> {
    // topics which ownership should be released
    chunked_vector<model::topic_namespace> topics;
    // consumer groups which ownership should be released
    chunked_vector<consumer_group> groups;
    // optional target where the data should be copied to in the process of
    // migration
    std::optional<copy_target> copy_to;

    outbound_migration copy() const;

    auto serde_fields() { return std::tie(topics, groups, copy_to); }

    friend bool operator==(const outbound_migration&, const outbound_migration&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const outbound_migration&);
};
/**
 * Variant representing a migration. It can be either inbound or outbound data
 * migration.
 */
using data_migration = serde::variant<inbound_migration, outbound_migration>;

data_migration copy_migration(const data_migration& migration);

/**
 * Data migration metadata containing a migration definition, its id and current
 * state.
 */
struct migration_metadata
  : serde::envelope<
      migration_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;
    data_migration migration;
    /**
     * Data migration starts in a planned state.
     */
    state state = state::planned;

    migration_metadata copy() const {
        return migration_metadata{
          .id = id, .migration = copy_migration(migration), .state = state};
    }

    auto serde_fields() { return std::tie(id, migration, state); }

    friend bool operator==(const migration_metadata&, const migration_metadata&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const migration_metadata&);
};

struct create_migration_cmd_data
  : serde::envelope<
      create_migration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;
    data_migration migration;

    auto serde_fields() { return std::tie(id, migration); }
    friend bool operator==(
      const create_migration_cmd_data&, const create_migration_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const create_migration_cmd_data&);
};

struct update_migration_state_cmd_data
  : serde::envelope<
      update_migration_state_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;
    state requested_state;

    auto serde_fields() { return std::tie(id, requested_state); }
    friend bool operator==(
      const update_migration_state_cmd_data&,
      const update_migration_state_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const update_migration_state_cmd_data&);
};

struct remove_migration_cmd_data
  : serde::envelope<
      remove_migration_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;

    auto serde_fields() { return std::tie(id); }
    friend bool operator==(
      const remove_migration_cmd_data&, const remove_migration_cmd_data&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const remove_migration_cmd_data&);
};

