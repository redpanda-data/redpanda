/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "cluster_link/errc.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/variant.h"
#include "serde/rw/vector.h"
#include "utils/absl_sstring_hash.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"
#include "utils/uuid.h"

#include <seastar/util/bool_class.hh>

#include <expected>
#include <ostream>
#include <string_view>

namespace cluster_link::model {
/// ID of the cluster link - used internally based off of controller offset
using id_t = named_type<int64_t, struct id_tag>;
/// UUID of the cluster link - used externally
using uuid_t = named_type<uuid_t, struct uuid_tag>;
/// Name of the cluster link
using name_t = named_type<ss::sstring, struct name_tag>;
/// Type to indicate if the task is enabled or not
using enabled_t = ss::bool_class<struct enabled_tag>;

/**
 *
 *                     ┌─────────────┐
 *                     │   PAUSED    │
 *                     └─────────────┘
 *                           ▲ │
 *                           │ ▼
 *                     ┌─────────────┐
 *        ┌────────────┤   ACTIVE    ├────────────┐
 *        │            └─────────────┘            │
 *        │                  │                    │
 *        ▼                  ▼                    ▼
 *  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
 *  │FAILING_OVER │───▶│   FAILED    │◀───│ PROMOTING   │
 *  └─────────────┘    └─────────────┘    └─────────────┘
 *        │                                       │
 *        ▼                                       ▼
 *  ┌─────────────┐                      ┌─────────────┐
 *  │ FAILED_OVER │                      │  PROMOTED   │
 *  └─────────────┘                      └─────────────┘
 *        │                                       │
 *      <---     todo: add transitional states --->
 *        └───────────────┐    ┌──────────────────┘
 *                        ▼    ▼
 *                  ┌─────────────┐
 *                  │   ACTIVE    │
 *                  └─────────────┘
 *
 **/
enum class mirror_topic_status : uint8_t {
    /// Mirroring is active on the topic
    active,
    /// Mirroring has failed for the topic. This is a terminal state
    /// and non recoverable today.
    failed,
    /// Mirroring has been paused
    paused,
    /// Mirroring topic failover has been requested and in progress
    /// A failover is the processing switching the mirroring topic to active
    /// state and accept Kafka API writes. Disables replication from the source
    /// cluster
    // makes the topic read-write for consumers and producers.
    failing_over,
    /// Mirroring topic has successfully failed over and current active for
    /// Kafka API read/writes.
    failed_over,
    /// Same as failing_over but additionally waits for the topic to catch up to
    /// the source topic before completing the promote operation.
    promoting,
    /// Mirrorring topic has been successfully promoted
    promoted
};

static constexpr std::string_view to_string_view(mirror_topic_status s) {
    switch (s) {
    case mirror_topic_status::active:
        return "active";
    case mirror_topic_status::failed:
        return "failed";
    case mirror_topic_status::paused:
        return "paused";
    case mirror_topic_status::promoted:
        return "promoted";
    case mirror_topic_status::failing_over:
        return "failing_over";
    case mirror_topic_status::failed_over:
        return "failed_over";
    case mirror_topic_status::promoting:
        return "promoting";
    }
}

bool is_valid_status_transition(
  mirror_topic_status current, mirror_topic_status target) noexcept;

std::ostream& operator<<(std::ostream& os, mirror_topic_status s);

enum class task_state : uint8_t {
    /// The task is currently active and processing
    active,
    /// The task has been paused by the user
    paused,
    /// The link to the source cluster is unavailable.  This could be a
    /// temporary condition that can be resolved by changing configuration of
    /// the task or on the source cluster.  This state is informative and the
    /// task will continue to run at its set interval
    link_unavailable,
    /// The task is not configured to run
    stopped,
    /// The task has encountered an unexpected fault
    faulted,
};

static constexpr std::string_view to_string_view(task_state st) {
    switch (st) {
    case task_state::active:
        return "active";
    case task_state::paused:
        return "paused";
    case task_state::link_unavailable:
        return "link_unavailable";
    case task_state::stopped:
        return "stopped";
    case task_state::faulted:
        return "faulted";
    }
}

std::ostream& operator<<(std::ostream& os, task_state s);

/**
 * @brief SCRAM credentials to use for authentication
 */
struct scram_credentials
  : serde::
      envelope<scram_credentials, serde::version<0>, serde::compat_version<0>> {
    /// SCRAM username
    ss::sstring username;
    /// SCRAM password
    ss::sstring password;
    /// SASL-SCRAM mechanism to use
    ss::sstring mechanism;

    friend bool operator==(const scram_credentials&, const scram_credentials&)
      = default;
    auto serde_fields() { return std::tie(username, password, mechanism); }

    friend std::ostream&
    operator<<(std::ostream& os, const scram_credentials& creds);
};

using tls_file_path = named_type<ss::sstring, struct tls_file_path_tag>;
std::ostream& operator<<(std::ostream& os, const tls_file_path& p);

using tls_value = named_type<ss::sstring, struct tls_value_tag>;
std::ostream& operator<<(std::ostream& os, const tls_value& v);

using tls_file_or_value = serde::variant<tls_file_path, tls_value>;
std::ostream& operator<<(std::ostream& os, const tls_file_or_value& t);

/**
 * @brief Represents the settings for connection to a remote cluster
 */
struct connection_config
  : serde::
      envelope<connection_config, serde::version<0>, serde::compat_version<0>> {
    /// List of addresses to bootstrap the connection
    std::vector<net::unresolved_address> bootstrap_servers;
    /// Support authn variants.  Currently only SCRAM but update this to add
    /// support for OIDC or GSSAPI in the future.
    using authn_variant = serde::variant<scram_credentials>;
    /// Authentication configuration for the connection
    std::optional<authn_variant> authn_config;
    using tls_enabled_t = ss::bool_class<struct tls_enabled_tag>;
    /// Whether or not TLS is enabled
    tls_enabled_t tls_enabled{tls_enabled_t::no};
    /// certificate file to use
    std::optional<tls_file_or_value> cert;
    /// key to use (when mTLS is in use)
    std::optional<tls_file_or_value> key;
    /// The CA file to use
    std::optional<tls_file_or_value> ca;
    /// The client ID to use
    ss::sstring client_id;
    // Max metadata age
    std::optional<int32_t> metadata_max_age_ms;
    // Default for metadata_max_age_ms (10 seconds)
    static constexpr auto metadata_max_age_ms_default = 10000;
    // Connection timeout
    std::optional<int32_t> connection_timeout_ms;
    // Default for connection_timeout_ms (1 second)
    static constexpr auto connection_timeout_ms_default = 1000;
    // Retry backoff
    std::optional<int32_t> retry_backoff_ms;
    // Default for retry_backoff_ms (100ms)
    static constexpr auto retry_backoff_ms_default = 100;
    // Maximum fetch wait time
    std::optional<int32_t> fetch_wait_max_ms;
    // Default value for fetch_wait_max_ms (100ms)
    static constexpr auto fetch_wait_max_ms_default = 100;
    // Minimum number of bytes to fetch
    std::optional<int32_t> fetch_min_bytes;
    // Default minimum number of bytes to fetch (1B)
    static constexpr auto fetch_min_bytes_default = 1;
    // Maximum number of bytes to fetch
    std::optional<int32_t> fetch_max_bytes;
    // Default maximum number of bytes to fetch (1MiB)
    static constexpr auto fetch_max_bytes_default = 1 * 1024 * 1024;

    // Returns the metadata_max_age_ms value
    int32_t get_metadata_max_age_ms() const {
        return metadata_max_age_ms.value_or(metadata_max_age_ms_default);
    }

    // Returns the connection_timeout_ms value
    int32_t get_connection_timeout_ms() const {
        return connection_timeout_ms.value_or(connection_timeout_ms_default);
    }

    // Returns the retry_backoff_ms value
    int32_t get_retry_backoff_ms() const {
        return retry_backoff_ms.value_or(retry_backoff_ms_default);
    }

    // Returns the fetch_wait_max_ms value
    int32_t get_fetch_wait_max_ms() const {
        return fetch_wait_max_ms.value_or(fetch_wait_max_ms_default);
    }

    // Returns the fetch_min_bytes value
    int32_t get_fetch_min_bytes() const {
        return fetch_min_bytes.value_or(fetch_min_bytes_default);
    }

    // Returns the fetch_max_bytes value
    int32_t get_fetch_max_bytes() const {
        return fetch_max_bytes.value_or(fetch_max_bytes_default);
    }

    friend bool operator==(const connection_config&, const connection_config&)
      = default;

    auto serde_fields() {
        return std::tie(
          bootstrap_servers,
          authn_config,
          cert,
          key,
          ca,
          client_id,
          metadata_max_age_ms,
          connection_timeout_ms,
          retry_backoff_ms,
          fetch_wait_max_ms,
          fetch_min_bytes,
          fetch_max_bytes,
          tls_enabled);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const connection_config& cfg);
};

struct mirror_topic_metadata
  : serde::envelope<
      mirror_topic_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Current mirroring state
    mirror_topic_status status{mirror_topic_status::active};
    /// The topic ID of the source topic
    /// Made optional to allow for cases where we will migrate from clusters
    /// that don't yet support topic ids
    std::optional<::model::topic_id> source_topic_id;
    /// The name of the source topic
    ::model::topic source_topic_name;
    /// The topic ID of the destination topic
    ::model::topic_id destination_topic_id;
    /// The number of partitions on the source topic
    int32_t partition_count;
    /// The replication factor - if not provided, is using
    /// `default_topic_replication` cluster config
    std::optional<int16_t> replication_factor;
    /// The configuration for the topic
    chunked_hash_map<ss::sstring, ss::sstring> topic_configs;

    friend bool
    operator==(const mirror_topic_metadata&, const mirror_topic_metadata&)
      = default;

    auto serde_fields() {
        return std::tie(
          status,
          source_topic_id,
          source_topic_name,
          destination_topic_id,
          partition_count,
          replication_factor,
          topic_configs);
    }

    mirror_topic_metadata copy() const;

    friend std::ostream&
    operator<<(std::ostream& os, const mirror_topic_metadata& md);
};

/// How the patch filters
enum class filter_pattern_type : uint8_t {
    /// Literal name match
    literal,
    /// Match any that is prefixed with the pattern
    prefix
};

static constexpr std::string_view to_string_view(filter_pattern_type f) {
    switch (f) {
    case filter_pattern_type::literal:
        return "literal";
    case filter_pattern_type::prefix:
        return "prefix";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& os, filter_pattern_type f);

/// Whether or not the filter is an inclusive or exclusive filter
enum class filter_type : uint8_t { include, exclude };

static constexpr std::string_view to_string_view(filter_type f) {
    switch (f) {
    case filter_type::include:
        return "include";
    case filter_type::exclude:
        return "exclude";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& os, filter_type f);

struct resource_name_filter_pattern
  : serde::envelope<
      resource_name_filter_pattern,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr const char* wildcard = "*";
    /// The type of the filter pattern
    filter_pattern_type pattern_type;
    filter_type filter;
    /// The pattern to match against
    ss::sstring pattern;

    friend bool operator==(
      const resource_name_filter_pattern&, const resource_name_filter_pattern&)
      = default;

    auto serde_fields() { return std::tie(pattern_type, filter, pattern); }

    friend std::ostream&
    operator<<(std::ostream& os, const resource_name_filter_pattern& p);
};

struct topic_metadata_mirroring_config
  : serde::envelope<
      topic_metadata_mirroring_config,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Flag to indicate if the task is enabled or not
    enabled_t is_enabled{enabled_t::yes};
    /// Interval for the topic creation task
    std::optional<ss::lowres_clock::duration> task_interval;
    // Default interval (30 seconds)
    static constexpr auto task_interval_default = std::chrono::seconds(30);

    /// Filters
    chunked_vector<resource_name_filter_pattern> topic_name_filters;
    using properties_set
      = absl::flat_hash_set<ss::sstring, sstring_hash, sstring_eq>;
    /// List of topic properties to mirror
    properties_set topic_properties_to_mirror;

    ss::lowres_clock::duration get_task_interval() const {
        return task_interval.value_or(task_interval_default);
    }

    friend bool operator==(
      const topic_metadata_mirroring_config&,
      const topic_metadata_mirroring_config&)
      = default;

    auto serde_fields() {
        return std::tie(
          is_enabled,
          task_interval,
          topic_name_filters,
          topic_properties_to_mirror);
    }

    topic_metadata_mirroring_config copy() const;

    friend std::ostream&
    operator<<(std::ostream& os, const topic_metadata_mirroring_config& cfg);
};

struct consumer_groups_mirroring_config
  : serde::envelope<
      consumer_groups_mirroring_config,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Flag to indicate if the task is enabled or not
    enabled_t is_enabled{enabled_t::yes};
    // Task interval for consumer group mirroring
    std::optional<ss::lowres_clock::duration> task_interval;
    static constexpr auto default_task_interval = std::chrono::seconds(30);

    /// Filters
    chunked_vector<resource_name_filter_pattern> filters;

    ss::lowres_clock::duration get_task_interval() const {
        return task_interval.value_or(default_task_interval);
    }

    friend bool operator==(
      const consumer_groups_mirroring_config&,
      const consumer_groups_mirroring_config&)
      = default;

    auto serde_fields() { return std::tie(is_enabled, task_interval, filters); }

    consumer_groups_mirroring_config copy() const;

    friend std::ostream&
    operator<<(std::ostream& os, const consumer_groups_mirroring_config& cfg);
};

enum class acl_resource : uint8_t {
    any,
    cluster,
    group,
    topic,
    txn_id,
    schema_registry_subject,
    schema_registry_global,
    schema_registry_any
};

static constexpr std::string_view to_string_view(acl_resource r) {
    switch (r) {
    case acl_resource::any:
        return "any";
    case acl_resource::cluster:
        return "cluster";
    case acl_resource::group:
        return "group";
    case acl_resource::topic:
        return "topic";
    case acl_resource::txn_id:
        return "txn_id";
    case acl_resource::schema_registry_subject:
        return "schema_registry_subject";
    case acl_resource::schema_registry_global:
        return "schema_registry_global";
    case acl_resource::schema_registry_any:
        return "schema_registry_any";
    }
    return "unknown";
}

enum class acl_pattern : uint8_t { any, literal, prefixed, match };

static constexpr std::string_view to_string_view(acl_pattern p) {
    switch (p) {
    case acl_pattern::any:
        return "any";
    case acl_pattern::literal:
        return "literal";
    case acl_pattern::prefixed:
        return "prefixed";
    case acl_pattern::match:
        return "match";
    }
    return "unknown";
}

enum class acl_operation : uint8_t {
    any,
    all,
    read,
    write,
    create,
    remove,
    alter,
    describe,
    cluster_action,
    describe_configs,
    alter_configs,
    idempotent_write
};

static constexpr std::string_view to_string_view(acl_operation op) {
    switch (op) {
    case acl_operation::any:
        return "any";
    case acl_operation::all:
        return "all";
    case acl_operation::read:
        return "read";
    case acl_operation::write:
        return "write";
    case acl_operation::create:
        return "create";
    case acl_operation::remove:
        return "remove";
    case acl_operation::alter:
        return "alter";
    case acl_operation::describe:
        return "describe";
    case acl_operation::cluster_action:
        return "cluster_action";
    case acl_operation::describe_configs:
        return "describe_configs";
    case acl_operation::alter_configs:
        return "alter_configs";
    case acl_operation::idempotent_write:
        return "idempotent_write";
    }
    return "unknown";
}

enum class acl_permission_type : uint8_t { any, allow, deny };

static constexpr std::string_view to_string_view(acl_permission_type p) {
    switch (p) {
    case acl_permission_type::any:
        return "any";
    case acl_permission_type::allow:
        return "allow";
    case acl_permission_type::deny:
        return "deny";
    }
    return "unknown";
}

struct acl_resource_filter
  : serde::envelope<
      acl_resource_filter,
      serde::version<0>,
      serde::compat_version<0>> {
    acl_resource resource_type;
    acl_pattern pattern_type;
    ss::sstring name;

    friend bool
    operator==(const acl_resource_filter&, const acl_resource_filter&)
      = default;

    auto serde_fields() { return std::tie(resource_type, pattern_type, name); }
};

struct acl_access_filter
  : serde::
      envelope<acl_access_filter, serde::version<0>, serde::compat_version<0>> {
    ss::sstring principal;
    acl_operation operation;
    acl_permission_type permission_type;
    ss::sstring host;

    friend bool operator==(const acl_access_filter&, const acl_access_filter&)
      = default;

    auto serde_fields() {
        return std::tie(principal, operation, permission_type, host);
    }
};

struct acl_filter
  : serde::envelope<acl_filter, serde::version<0>, serde::compat_version<0>> {
    acl_resource_filter resource_filter;
    acl_access_filter access_filter;

    friend bool operator==(const acl_filter&, const acl_filter&) = default;

    auto serde_fields() { return std::tie(resource_filter, access_filter); }
};

/**
 * Configuration for syncing security settings
 *
 */
struct security_settings_sync_config
  : serde::envelope<
      security_settings_sync_config,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Flag to indicate if the task is enabled or not
    enabled_t is_enabled{enabled_t::yes};
    /// Interval for the topic creation task
    std::optional<ss::lowres_clock::duration> task_interval;
    /// Default interval
    static constexpr auto task_interval_default = std::chrono::seconds{30};

    ss::lowres_clock::duration get_task_interval() const {
        return task_interval.value_or(task_interval_default);
    }

    chunked_vector<acl_filter> acl_filters;

    friend bool operator==(
      const security_settings_sync_config&,
      const security_settings_sync_config&)
      = default;

    auto serde_fields() {
        return std::tie(is_enabled, task_interval, acl_filters);
    }

    security_settings_sync_config copy() const;
};

/**
 * Configuration of a cluster link. Configuration changes are driven by the
 * API and are a result of user actions.
 */
struct link_configuration
  : serde::envelope<
      link_configuration,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Configuration for the auto mirror topic creation task
    topic_metadata_mirroring_config topic_metadata_mirroring_cfg;
    /// Configuration for the consumer groups mirroring task
    consumer_groups_mirroring_config consumer_groups_mirroring_cfg;
    security_settings_sync_config security_settings_sync_cfg;

    friend bool operator==(const link_configuration&, const link_configuration&)
      = default;

    auto serde_fields() {
        return std::tie(
          topic_metadata_mirroring_cfg,
          consumer_groups_mirroring_cfg,
          security_settings_sync_cfg);
    }

    link_configuration copy() const;

    friend std::ostream&
    operator<<(std::ostream& os, const link_configuration& lc);
};

enum class link_status : uint8_t {
    // Initial state when the link is created
    active,
    // The link has been paused by the user,
    // pauses all link related tasks including replication and syncing
    // of metadata
    // Note: pausing state has not been implemented yet.
    paused,
};

static constexpr std::string_view to_string_view(link_status s) {
    switch (s) {
    case link_status::active:
        return "active";
    case link_status::paused:
        return "paused";
    }
}
std::ostream& operator<<(std::ostream& os, const link_status& s);

/**
 * Link state. The state is modified by the cluster link tasks and is
 * persisted to the cluster link table.
 */
struct link_state
  : serde::envelope<link_state, serde::version<0>, serde::compat_version<0>> {
    link_state() noexcept = default;
    link_state(link_state&&) noexcept = default;
    link_state(const link_state&) = delete;
    link_state& operator=(link_state&&) noexcept = default;
    link_state& operator=(const link_state&) = delete;
    ~link_state() noexcept = default;

    link_status status{link_status::active};
    /// Map of topics that this link is mirroring and their state
    using mirror_topics_t
      = chunked_hash_map<::model::topic, mirror_topic_metadata>;
    chunked_hash_map<::model::topic, mirror_topic_metadata> mirror_topics;

    void set_mirror_topics(const mirror_topics_t& topics);
    void set_mirror_topics(mirror_topics_t&& topics);

    friend bool operator==(const link_state&, const link_state&) = default;

    auto serde_fields() { return std::tie(status, mirror_topics); }

    link_state copy() const;

    friend std::ostream& operator<<(std::ostream& os, const link_state& ls);
};
struct metadata
  : serde::envelope<metadata, serde::version<0>, serde::compat_version<0>> {
    /// Name of the cluster link
    name_t name;
    /// Unique identifier for the cluster link
    uuid_t uuid;
    /// Connection settings for the cluster link
    connection_config connection;
    /// The state of the link
    link_state state;
    /// Configuration for the cluster link
    link_configuration configuration;

    friend bool operator==(const metadata&, const metadata&) = default;

    auto serde_fields() {
        return std::tie(name, uuid, connection, state, configuration);
    }

    metadata copy() const;

    friend std::ostream& operator<<(std::ostream& os, const metadata& md);
};

/// \brief Command used to add a mirror topic to a cluster link
///
/// This command will be used either via the auto topic creation task via a
/// user action when a new topic is to be created and used as a mirror topic
struct add_mirror_topic_cmd
  : serde::envelope<
      add_mirror_topic_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the topic
    ::model::topic topic;
    /// Initial state of the topic
    mirror_topic_metadata metadata;

    friend bool
    operator==(const add_mirror_topic_cmd&, const add_mirror_topic_cmd&)
      = default;

    auto serde_fields() { return std::tie(topic, metadata); }

    add_mirror_topic_cmd copy() const;
};

/// \brief Command used to update the state of a mirror topic
///
/// Will be used by the cluster linking mirroring task to change the state
/// of mirroring for the topic
struct update_mirror_topic_status_cmd
  : serde::envelope<
      update_mirror_topic_status_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the topic
    ::model::topic topic;
    /// New state of the topic
    mirror_topic_status status{mirror_topic_status::active};

    friend bool operator==(
      const update_mirror_topic_status_cmd&,
      const update_mirror_topic_status_cmd&)
      = default;

    auto serde_fields() { return std::tie(topic, status); }
};

/// \brief Command used to update the properties of a mirror topic
///
/// Will be used by the cluster linking metadata sync test to update
/// the properties of a mirror topic
struct update_mirror_topic_properties_cmd
  : serde::envelope<
      update_mirror_topic_properties_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the topic
    ::model::topic topic;
    int32_t partition_count;
    std::optional<int16_t> replication_factor;
    chunked_hash_map<ss::sstring, ss::sstring> topic_configs;

    friend bool operator==(
      const update_mirror_topic_properties_cmd&,
      const update_mirror_topic_properties_cmd&)
      = default;

    auto serde_fields() {
        return std::tie(
          topic, partition_count, replication_factor, topic_configs);
    }

    update_mirror_topic_properties_cmd copy() const;
};

/// \brief Command used to update the configuration of a cluster link
struct update_cluster_link_configuration_cmd
  : serde::envelope<
      update_cluster_link_configuration_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    connection_config connection;
    link_configuration link_config;

    friend bool operator==(
      const update_cluster_link_configuration_cmd&,
      const update_cluster_link_configuration_cmd&)
      = default;

    auto serde_fields() { return std::tie(connection, link_config); }

    update_cluster_link_configuration_cmd copy() const;
};

/// Status report for a task
struct task_status_report
  : serde::envelope<
      task_status_report,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring task_name;
    task_state task_state;
    ss::sstring task_state_reason;

    friend bool operator==(const task_status_report&, const task_status_report&)
      = default;

    auto serde_fields() {
        return std::tie(task_name, task_state, task_state_reason);
    }
};

/// The status report of a link
struct link_task_status_report
  : serde::envelope<
      link_task_status_report,
      serde::version<0>,
      serde::compat_version<0>> {
    name_t link_name;
    chunked_hash_map<ss::sstring, task_status_report> task_status_reports;

    friend bool
    operator==(const link_task_status_report&, const link_task_status_report&)
      = default;

    auto serde_fields() { return std::tie(link_name, task_status_reports); }
};

/// A map of task status reports per link
struct cluster_link_task_status_report
  : serde::envelope<
      cluster_link_task_status_report,
      serde::version<0>,
      serde::compat_version<0>> {
    chunked_hash_map<name_t, link_task_status_report> link_reports;

    friend bool operator==(
      const cluster_link_task_status_report&,
      const cluster_link_task_status_report&)
      = default;

    auto serde_fields() { return std::tie(link_reports); }
};

struct aggregated_shadow_topic_report {
    struct partition_report {
        ::model::partition_id partition;
    };
    struct broker_report {
        ::model::node_id broker;
        ::model::revision_id link_update_revision;
        chunked_vector<partition_report> leaders;
    };

    chunked_vector<broker_report> brokers;
    int32_t total_partitions{0};

    friend bool operator==(
      const aggregated_shadow_topic_report&,
      const aggregated_shadow_topic_report&)
      = default;
};
using report_result_t = std::expected<aggregated_shadow_topic_report, errc>;
} // namespace cluster_link::model

namespace cluster_link::rpc {
// report request to a single broker
struct shadow_topic_report_request
  : serde::envelope<
      shadow_topic_report_request,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster_link::model::id_t link_id;
    ::model::topic topic_name;

    friend bool operator==(
      const shadow_topic_report_request&, const shadow_topic_report_request&)
      = default;

    fmt::iterator format_to(fmt::iterator) const;

    auto serde_fields() { return std::tie(link_id, topic_name); }
};

struct shadow_topic_partition_leader_report
  : serde::envelope<
      shadow_topic_partition_leader_report,
      serde::version<0>,
      serde::compat_version<0>> {
    ::model::partition_id partition;
    // todo: add offset information for promotion
    // todo: add hwm information for fail over state
    // checkpointing

    friend bool operator==(
      const shadow_topic_partition_leader_report&,
      const shadow_topic_partition_leader_report&)
      = default;

    fmt::iterator format_to(fmt::iterator) const;

    auto serde_fields() { return std::tie(partition); }
};

// aggregated report from a single broker about a shadow topic
// the report is aggregated across all partition leaders and shards
// on the broker.
struct shadow_topic_report_response
  : serde::envelope<
      shadow_topic_report_response,
      serde::version<0>,
      serde::compat_version<0>> {
    ::model::node_id node_id;
    // The smallest revision seen by the link across all shards.
    ::model::revision_id link_update_revision;
    // Report for each partition that is a leader on this broker.
    chunked_vector<shadow_topic_partition_leader_report> leaders;
    errc err_code;

    friend bool operator==(
      const shadow_topic_report_response&, const shadow_topic_report_response&)
      = default;

    fmt::iterator format_to(fmt::iterator) const;

    auto serde_fields() {
        return std::tie(node_id, link_update_revision, leaders, err_code);
    }
};

} // namespace cluster_link::rpc

template<>
struct fmt::formatter<cluster_link::model::mirror_topic_status>
  : fmt::formatter<string_view> {
    auto format(cluster_link::model::mirror_topic_status s, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::task_state>
  : fmt::formatter<string_view> {
    auto format(cluster_link::model::task_state, format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::scram_credentials>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::scram_credentials& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<
  std::optional<cluster_link::model::connection_config::authn_variant>>
  : fmt::formatter<string_view> {
    auto format(
      const std::optional<
        cluster_link::model::connection_config::authn_variant>& m,
      format_context& ctx) -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::tls_file_or_value>
  : fmt::formatter<string_view> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        auto it = ctx.begin();
        auto end = ctx.end();

        /// If formatted with `s`, then the value is sensitive and should not be
        /// displayed to the user
        if (it != end && (*it == 's')) {
            _is_sensitive = true;
            ++it;
        }

        if (it != end && *it != '}') {
            throw fmt::format_error(
              "invalid format specifier for tls_file_or_value");
        }

        return it;
    }
    auto
    format(const cluster_link::model::tls_file_or_value& m, format_context& ctx)
      -> decltype(ctx.out());

private:
    bool _is_sensitive{false};
};

template<>
struct fmt::formatter<std::optional<cluster_link::model::tls_file_or_value>>
  : fmt::formatter<string_view> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        auto it = ctx.begin();
        auto end = ctx.end();

        if (it != end && (*it == 's')) {
            _is_sensitive = true;
            ++it;
        }

        if (it != end && *it != '}') {
            throw fmt::format_error(
              "invalid format specifier for optional<tls_file_or_value>");
        }

        return it;
    }
    auto format(
      const std::optional<cluster_link::model::tls_file_or_value>& m,
      format_context& ctx) -> decltype(ctx.out());

private:
    bool _is_sensitive{false};
};

template<>
struct fmt::formatter<cluster_link::model::connection_config>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::connection_config& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<std::optional<model::topic_id>>
  : fmt::formatter<string_view> {
    auto format(const std::optional<model::topic_id>& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::mirror_topic_metadata>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::mirror_topic_metadata& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::filter_pattern_type>
  : fmt::formatter<string_view> {
    auto format(cluster_link::model::filter_pattern_type s, format_context& ctx)
      const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::filter_type>
  : fmt::formatter<string_view> {
    auto format(cluster_link::model::filter_type s, format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::resource_name_filter_pattern>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::resource_name_filter_pattern& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::topic_metadata_mirroring_config>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::topic_metadata_mirroring_config& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::consumer_groups_mirroring_config>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::consumer_groups_mirroring_config& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<
  decltype(cluster_link::model::link_state::mirror_topics)::value_type>
  : fmt::formatter<string_view> {
    auto format(
      const decltype(cluster_link::model::link_state::mirror_topics)::
        value_type& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::link_state>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::link_state& s, format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::metadata>
  : fmt::formatter<string_view> {
    auto format(const cluster_link::model::metadata& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::add_mirror_topic_cmd>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::add_mirror_topic_cmd& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::update_mirror_topic_status_cmd>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::update_mirror_topic_status_cmd& m,
      format_context& ctx) -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::update_mirror_topic_properties_cmd>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::update_mirror_topic_properties_cmd& m,
      format_context& ctx) -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::task_status_report>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::task_status_report& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<decltype(cluster_link::model::link_task_status_report::
                                 task_status_reports)::value_type>
  : fmt::formatter<string_view> {
    auto format(
      const decltype(cluster_link::model::link_task_status_report::
                       task_status_reports)::value_type& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::link_task_status_report>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::link_task_status_report& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<
  decltype(cluster_link::model::cluster_link_task_status_report::link_reports)::
    value_type> : fmt::formatter<string_view> {
    auto format(
      const decltype(cluster_link::model::cluster_link_task_status_report::
                       link_reports)::value_type& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::cluster_link_task_status_report>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::cluster_link_task_status_report& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::acl_resource_filter>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::acl_resource_filter& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::acl_access_filter>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::acl_access_filter& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::acl_filter>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::acl_filter& m, format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::security_settings_sync_config>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::security_settings_sync_config& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::link_configuration>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::link_configuration& m,
      format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<
  cluster_link::model::update_cluster_link_configuration_cmd>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::update_cluster_link_configuration_cmd& m,
      format_context& ctx) const -> decltype(ctx.out());
};
