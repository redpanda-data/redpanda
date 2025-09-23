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
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"
#include "serde/rw/variant.h"
#include "utils/absl_sstring_hash.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"
#include "utils/uuid.h"

#include <seastar/util/bool_class.hh>

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

enum class mirror_topic_state : uint8_t {
    /// Mirroring is active on the topic
    active,
    /// Mirroring has failed for the topic
    failed,
    /// Mirroring has been paused
    paused,
    /// Mirror topic has been promoted
    promoted
};

static constexpr std::string_view to_string_view(mirror_topic_state s) {
    switch (s) {
    case mirror_topic_state::active:
        return "active";
    case mirror_topic_state::failed:
        return "failed";
    case mirror_topic_state::paused:
        return "paused";
    case mirror_topic_state::promoted:
        return "promoted";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& os, mirror_topic_state s);

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
    return "unknown";
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
          fetch_max_bytes);
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
    mirror_topic_state state{mirror_topic_state::active};
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
          state,
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

    friend bool operator==(const link_configuration&, const link_configuration&)
      = default;

    auto serde_fields() {
        return std::tie(
          topic_metadata_mirroring_cfg, consumer_groups_mirroring_cfg);
    }

    link_configuration copy() const;

    friend std::ostream&
    operator<<(std::ostream& os, const link_configuration& lc);
};
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

    /// Type to indicate if the cluster link is paused
    using paused_t = ss::bool_class<struct paused_tag>;
    /// Flag indicating if the cluster link has been paused
    paused_t paused{paused_t::no};
    using mirror_topics_t
      = chunked_hash_map<::model::topic, mirror_topic_metadata>;
    /// Map of topics that this link is mirroring and their state
    chunked_hash_map<::model::topic, mirror_topic_metadata> mirror_topics;

    void set_mirror_topics(const mirror_topics_t& topics);
    void set_mirror_topics(mirror_topics_t&& topics);

    friend bool operator==(const link_state&, const link_state&) = default;

    auto serde_fields() { return std::tie(paused, mirror_topics); }

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
struct update_mirror_topic_state_cmd
  : serde::envelope<
      update_mirror_topic_state_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the topic
    ::model::topic topic;
    /// New state of the topic
    mirror_topic_state state{mirror_topic_state::active};

    friend bool operator==(
      const update_mirror_topic_state_cmd&,
      const update_mirror_topic_state_cmd&)
      = default;

    auto serde_fields() { return std::tie(topic, state); }
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
} // namespace cluster_link::model

template<>
struct fmt::formatter<cluster_link::model::mirror_topic_state>
  : fmt::formatter<string_view> {
    auto format(cluster_link::model::mirror_topic_state s, format_context& ctx)
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
struct fmt::formatter<cluster_link::model::update_mirror_topic_state_cmd>
  : fmt::formatter<string_view> {
    auto format(
      const cluster_link::model::update_mirror_topic_state_cmd& m,
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
