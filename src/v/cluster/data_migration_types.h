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

#include "absl/container/flat_hash_set.h"
#include "cluster/errc.h"
#include "cluster/offsets_snapshot.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/sstring.h"
#include "serde/rw/variant.h"
#include "serde/rw/vector.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <ranges>

namespace cluster::data_migrations {
/**
 * Identifier of data migration, the identifier is guaranteed to be unique
 * within the same cluster but migrations in different clusters may have the
 * same identifiers.
 */
using id = named_type<int64_t, struct data_migration_type_tag>;
using consumer_group = kafka::group_id;
/**
 * Migration state
 *  ┌─────────┐
 *  │ planned │
 *  └────┬────┘
 *       │
 * ┌─────▼─────┐
 * │ preparing ├────────────────┐
 * └─────┬─────┘                │
 *       │                      │
 * ┌─────▼────┐          ┌──────▼────┐
 * │ prepared ├──────────► canceling │
 * └─────┬────┘          └▲──▲─┬─────┘
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
 * │ cut_over │          │ cancelled │
 * └─────┬────┘          └───────────┘
 *       │
 * ┌─────▼────┐
 * │ finished │
 * └──────────┘
 */
enum class state {
    planned,
    preparing,
    prepared,
    executing,
    executed,
    cut_over,
    finished,
    canceling,
    cancelled,
    deleted // a migration cannot use it
};
std::ostream& operator<<(std::ostream& o, state);

/**
 * For each migration state transition that requires work on partitions
 * a partition replica has the following lifecycle:
 * - waiting_for_rpc: work requested by raft0, shard not assigned;
 * - waiting_for_controller_update: work requested by RPC request, but we
 *   haven't yet seen the corresponding raft0 update;
 * - can_run: seconded by RPC request, shard may be assigned to work on;
 * - done: shard completed work and unassigned, done.
 * Unless (or until) the shard is the partition leader, it gets stuck
 * in can_run status.
 */
enum class migrated_replica_status {
    waiting_for_rpc,
    waiting_for_controller_update,
    can_run,
    done
};
std::ostream& operator<<(std::ostream& o, migrated_replica_status);

/**
 * State of migrated resource i.e. either topic or consumer group, when resource
 * is blocked all the writes should be disabled, when it is restricted a
 * resource properties should not be updated.
 */
enum class migrated_resource_state {
    non_restricted,
    metadata_locked,
    read_only,
    create_only, // can only be created, and only by migrations
    fully_blocked
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

    ss::sstring hint;

    friend bool operator==(
      const cloud_storage_location&, const cloud_storage_location&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const cloud_storage_location&);

    auto serde_fields() { return std::tie(hint); }
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

    friend bool
    operator==(const inbound_topic&, const inbound_topic&) = default;
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
      envelope<inbound_migration, serde::version<1>, serde::compat_version<0>> {
    chunked_vector<inbound_topic> topics;
    chunked_vector<consumer_group> groups;
    // run the migration through stages without explicit user action
    bool auto_advance = false;

    inbound_migration copy() const;

    auto serde_fields() { return std::tie(topics, groups, auto_advance); }

    friend bool
    operator==(const inbound_migration&, const inbound_migration&) = default;
    friend std::ostream& operator<<(std::ostream&, const inbound_migration&);

    auto topic_nts() const {
        auto pieces = std::vector{std::as_const(topics) | std::views::all};
        if (!groups.empty()) {
            pieces.push_back(consumer_offsets_topic | std::views::all);
        }
        return std::move(pieces) | std::views::join
               | std::views::transform(&inbound_topic::effective_topic_name);
    }

private:
    static const chunked_vector<inbound_topic> consumer_offsets_topic;
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

    auto serde_fields() { return std::tie(bucket); }

    friend bool operator==(const copy_target&, const copy_target&) = default;
    friend std::ostream& operator<<(std::ostream&, const copy_target&);
};

// A struct with information needed to unambiguously find topic data in cloud
// storage.
struct topic_location
  : serde::
      envelope<topic_location, serde::version<0>, serde::compat_version<0>> {
    // Topic name when its data was first written to cloud storage.
    model::topic_namespace remote_topic;
    // Location hint used to disambiguate between different topic instances.
    // Empty for topics still using legacy (pre v24.2) cloud storage paths.
    std::optional<cloud_storage_location> location;

    auto serde_fields() { return std::tie(remote_topic, location); }

    friend bool
    operator==(const topic_location&, const topic_location&) = default;

    friend std::ostream& operator<<(std::ostream&, const topic_location&);
};

/**
 * Outbound migration object representing topics and consumer groups that
 * ownership should be released.
 */
struct outbound_migration
  : serde::envelope<
      outbound_migration,
      serde::version<2>,
      serde::compat_version<0>> {
    // topics which ownership should be released
    chunked_vector<model::topic_namespace> topics;
    // consumer groups which ownership should be released
    chunked_vector<consumer_group> groups;
    // optional target where the data should be copied to in the process of
    // migration
    std::optional<copy_target> copy_to;
    // run the migration through stages without explicit user action
    bool auto_advance = false;
    // Topic locations. If not empty, must have the same size as topics.
    chunked_vector<topic_location> topic_locations;

    outbound_migration copy() const;

    auto serde_fields() {
        return std::tie(topics, groups, copy_to, auto_advance, topic_locations);
    }

    friend bool
    operator==(const outbound_migration&, const outbound_migration&) = default;
    friend std::ostream& operator<<(std::ostream&, const outbound_migration&);

    auto topic_nts() const {
        auto pieces = std::vector{std::as_const(topics) | std::views::all};
        if (!groups.empty()) {
            pieces.push_back(consumer_offsets_topic | std::views::all);
        }
        return std::move(pieces) | std::views::join;
    }

private:
    static const chunked_vector<model::topic_namespace> consumer_offsets_topic;
};

/**
 * Variant representing a migration. It can be either inbound or outbound data
 * migration.
 */
using data_migration = serde::variant<inbound_migration, outbound_migration>;

data_migration copy_migration(const data_migration& migration);

/* Additional info worker needs from backend to work on a partition */
struct inbound_partition_work_info {
    std::optional<model::topic_namespace> source;
    std::optional<cloud_storage_location> cloud_storage_location;
    chunked_vector<consumer_group>
      groups; // not empty iff partition of consumer offsets topic
};
struct outbound_partition_work_info {
    std::optional<copy_target> copy_to;
    chunked_vector<consumer_group>
      groups; // not empty iff partition of consumer offsets topic
};
using partition_work_info
  = std::variant<inbound_partition_work_info, outbound_partition_work_info>;
struct partition_work {
    id migration_id;
    state sought_state;
    model::revision_id revision_id;
    partition_work_info info;
};

/* Additional info worker needs from backend to work on a topic */
struct inbound_topic_work_info {
    std::optional<model::topic_namespace> source;
    std::optional<cloud_storage_location> cloud_storage_location;
};
struct outbound_topic_work_info {
    std::optional<copy_target> copy_to;
};
using topic_work_info
  = std::variant<inbound_topic_work_info, outbound_topic_work_info>;
struct topic_work {
    id migration_id;
    state sought_state;
    topic_work_info info;
};
std::ostream& operator<<(std::ostream& o, const topic_work& tw);

/**
 * Data migration metadata containing a migration definition, its id and current
 * state.
 */
struct migration_metadata
  : serde::envelope<
      migration_metadata,
      serde::version<2>,
      serde::compat_version<0>> {
    id id;
    data_migration migration;
    /**
     * Data migration starts in a planned state.
     */
    state state = state::planned;
    // populated on creation
    model::timestamp created_timestamp{};
    // populated once finished or cancelled state is reached
    model::timestamp completed_timestamp{};
    // last controller revision id that modified this migration
    model::revision_id revision_id{};

    migration_metadata copy() const {
        return migration_metadata{
          .id = id,
          .migration = copy_migration(migration),
          .state = state,
          .created_timestamp = created_timestamp,
          .completed_timestamp = completed_timestamp,
          .revision_id = revision_id};
    }

    auto serde_fields() {
        return std::tie(
          id,
          migration,
          state,
          created_timestamp,
          completed_timestamp,
          revision_id);
    }

    friend bool
    operator==(const migration_metadata&, const migration_metadata&) = default;

    friend std::ostream& operator<<(std::ostream&, const migration_metadata&);
};

struct data_migration_ntp_state
  : serde::envelope<
      data_migration_ntp_state,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = data_migration_ntp_state;

    model::ntp ntp;
    id migration;
    state state;

    auto serde_fields() { return std::tie(ntp, migration, state); }

    friend bool operator==(const self&, const self&) = default;
    friend std::ostream& operator<<(std::ostream&, const self&);
};

struct create_migration_cmd_data
  : serde::envelope<
      create_migration_cmd_data,
      serde::version<2>,
      serde::compat_version<0>> {
    id id;
    data_migration migration;
    model::timestamp op_timestamp{};
    bool fill_outbound_topic_locations = false;

    auto serde_fields() {
        return std::tie(
          id, migration, op_timestamp, fill_outbound_topic_locations);
    }
    friend bool operator==(
      const create_migration_cmd_data&,
      const create_migration_cmd_data&) = default;
    friend std::ostream&
    operator<<(std::ostream&, const create_migration_cmd_data&);
};

struct update_migration_state_cmd_data
  : serde::envelope<
      update_migration_state_cmd_data,
      serde::version<1>,
      serde::compat_version<0>> {
    id id;
    state requested_state;
    model::timestamp op_timestamp{};

    auto serde_fields() { return std::tie(id, requested_state, op_timestamp); }
    friend bool operator==(
      const update_migration_state_cmd_data&,
      const update_migration_state_cmd_data&) = default;
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
      const remove_migration_cmd_data&,
      const remove_migration_cmd_data&) = default;
    friend std::ostream&
    operator<<(std::ostream&, const remove_migration_cmd_data&);
};

struct create_migration_request
  : serde::envelope<
      create_migration_request,
      serde::version<0>,
      serde::compat_version<0>> {
    data_migration migration;

    auto serde_fields() { return std::tie(migration); }
    friend bool
    operator==(const create_migration_request&, const create_migration_request&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const create_migration_request&);
};
struct create_migration_reply
  : serde::envelope<
      create_migration_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    id id{-1};
    cluster::errc ec;
    auto serde_fields() { return std::tie(id, ec); }

    friend bool operator==(
      const create_migration_reply&, const create_migration_reply&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const create_migration_reply&);
};

struct update_migration_state_request
  : serde::envelope<
      update_migration_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;
    state state;
    auto serde_fields() { return std::tie(id, state); }
    friend bool operator==(
      const update_migration_state_request&,
      const update_migration_state_request&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const update_migration_state_request&);
};

struct update_migration_state_reply
  : serde::envelope<
      update_migration_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::errc ec;
    auto serde_fields() { return std::tie(ec); }

    friend bool operator==(
      const update_migration_state_reply&,
      const update_migration_state_reply&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const update_migration_state_reply&);
};
struct remove_migration_request
  : serde::envelope<
      remove_migration_request,
      serde::version<0>,
      serde::compat_version<0>> {
    id id;
    auto serde_fields() { return std::tie(id); }
    friend bool
    operator==(const remove_migration_request&, const remove_migration_request&)
      = default;
    friend std::ostream&
    operator<<(std::ostream&, const remove_migration_request&);
};

struct remove_migration_reply
  : serde::envelope<
      remove_migration_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::errc ec;

    auto serde_fields() { return std::tie(ec); }

    friend bool operator==(
      const remove_migration_reply&, const remove_migration_reply&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const remove_migration_reply&);
};

struct check_ntp_states_request
  : serde::envelope<
      check_ntp_states_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = check_ntp_states_request;

    chunked_vector<data_migration_ntp_state> sought_states;

    auto serde_fields() { return std::tie(sought_states); }

    friend bool operator==(const self&, const self&) = default;

    friend std::ostream& operator<<(std::ostream&, const self&);
};

struct check_ntp_states_reply
  : serde::envelope<
      check_ntp_states_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using self = check_ntp_states_reply;

    chunked_vector<data_migration_ntp_state> actual_states;

    auto serde_fields() { return std::tie(actual_states); }

    friend bool operator==(const self&, const self&) = default;

    friend std::ostream& operator<<(std::ostream&, const self&);
};

struct entities_status {
    chunked_vector<group_offsets> groups;

    friend std::ostream& operator<<(std::ostream&, const entities_status&);
};

} // namespace cluster::data_migrations
