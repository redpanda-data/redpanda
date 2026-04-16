/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/validators.h"

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "config/configuration.h"
#include "config/sasl_mechanisms.h"
#include "config/types.h"
#include "datalake/partition_spec_parser.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "serde/rw/chrono.h"
#include "ssx/sformat.h"
#include "utils/inet_address_wrapper.h"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <optional>
#include <unordered_map>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
split_string_int_by_colon(const ss::sstring& raw_option) {
    auto del_pos = raw_option.find(":");
    if (del_pos == ss::sstring::npos || del_pos == raw_option.size() - 1) {
        return std::nullopt;
    }

    auto str_val = raw_option.substr(0, del_pos);
    auto int_val = raw_option.substr(del_pos + 1);

    std::pair<ss::sstring, int64_t> ans;
    ans.first = str_val;
    try {
        ans.second = std::stoll(int_val);
    } catch (...) {
        return std::nullopt;
    }
    return ans;
}

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option) {
    return split_string_int_by_colon(raw_option);
}

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit) {
    absl::node_hash_set<net::inet_address_wrapper> ip_set;
    for (const auto& ip_and_limit : ips_with_limit) {
        auto parsing_setting = parse_connection_rate_override(ip_and_limit);
        if (!parsing_setting) {
            return fmt::format(
              "Can not parse connection_rate override {}", ip_and_limit);
        }

        ss::net::inet_address addr;
        try {
            addr = ss::net::inet_address(parsing_setting->first);
        } catch (...) {
            return fmt::format(
              "Looks like {} is not ip", parsing_setting->first);
        }

        if (!ip_set.insert(addr).second) {
            return fmt::format(
              "Duplicate setting for ip: {}", parsing_setting->first);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_sasl_mechanisms(const std::vector<ss::sstring>& mechanisms) {
    // Validate results
    for (const auto& m : mechanisms) {
        if (!std::ranges::contains(supported_sasl_mechanisms, m)) {
            return ssx::sformat("'{}' is not a supported SASL mechanism", m);
        }
    }

    const auto contains = [&mechanisms](const std::string_view& s) {
        return std::ranges::contains(mechanisms, s);
    };

    if (contains(plain) && !contains(scram)) {
        return ssx::sformat(
          "{} mechanism must be enabled if {} is enabled", scram, plain);
    }
    return std::nullopt;
}

std::optional<ss::sstring> validate_sasl_mechanisms_overrides(
  const std::vector<config::sasl_mechanisms_override>& overrides) {
    for (const auto& overide : overrides) {
        const auto error = validate_sasl_mechanisms(overide.sasl_mechanisms);
        if (error.has_value()) {
            return ssx::sformat(
              "Invalid sasl mechanisms override for listener '{}'. Error: {}",
              overide.listener,
              error.value());
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_http_authn_mechanisms(const std::vector<ss::sstring>& mechanisms) {
    constexpr auto supported = std::to_array<std::string_view>(
      {"BASIC", "OIDC"});

    // Validate results
    for (const auto& m : mechanisms) {
        if (std::ranges::none_of(supported, [&m](const auto& s) {
                return s == m;
            })) {
            return ssx::sformat(
              "'{}' is not a supported HTTP authentication mechanism", m);
        }
    }
    return std::nullopt;
}

bool oidc_is_enabled_http() {
    return std::ranges::any_of(
      config::shard_local_cfg().http_authentication(),
      [](const auto& m) { return m == "OIDC"; });
}

bool oidc_is_enabled_kafka() { return has_sasl_mechanism(oauthbearer); }

std::optional<ss::sstring> validate_0_to_1_ratio(const double d) {
    if (d < 0 || d > 1) {
        return fmt::format("Ratio must be in the [0,1] range, got: {}", d);
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_vec(const std::vector<ss::sstring>& vs) {
    for (const auto& s : vs) {
        if (s.empty()) {
            return "Empty strings are not valid in this collection";
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_opt(const std::optional<ss::sstring>& os) {
    if (os.has_value() && os.value().empty()) {
        return "Empty string is not valid";
    } else {
        return std::nullopt;
    }
}

std::optional<ss::sstring>
validate_audit_event_types(const std::vector<ss::sstring>& vs) {
    /// TODO: Should match stringified enums in kafka/types.h
    static const absl::flat_hash_set<ss::sstring> audit_event_types{
      "management",
      "produce",
      "consume",
      "describe",
      "heartbeat",
      "authenticate",
      "admin",
      "schema_registry"};

    for (const auto& e : vs) {
        if (!audit_event_types.contains(e)) {
            return ss::format("Unsupported audit event type passed: {}", e);
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_audit_excluded_topics(const std::vector<ss::sstring>& vs) {
    bool is_kafka_audit_topic = false;
    std::optional<ss::sstring> is_invalid_topic_name = std::nullopt;
    if (
      std::any_of(
        vs.begin(),
        vs.end(),
        [&is_kafka_audit_topic,
         &is_invalid_topic_name](const ss::sstring& topic_name) {
            auto t = model::topic{topic_name};
            if (t == model::kafka_audit_logging_topic) {
                is_kafka_audit_topic = true;
            } else if (model::validate_kafka_topic_name(t)) {
                is_invalid_topic_name = topic_name;
            }
            return is_kafka_audit_topic || is_invalid_topic_name.has_value();
        })) {
        if (is_kafka_audit_topic) {
            return ss::format(
              "Unable to exclude audit log '{}' from auditing",
              model::kafka_audit_logging_topic);
        } else if (is_invalid_topic_name.has_value()) {
            return ss::format(
              "{} is an invalid topic name", *is_invalid_topic_name);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_api_endpoint(const std::optional<ss::sstring>& os) {
    if (
      auto non_empty_string_opt = validate_non_empty_string_opt(os);
      non_empty_string_opt.has_value()) {
        return non_empty_string_opt;
    }

    if (
      os.has_value()
      && (os.value().starts_with("http://") || os.value().starts_with("https://"))) {
        return "String starting with URL protocol is not valid";
    }

    return std::nullopt;
}

std::optional<ss::sstring> validate_tombstone_retention_ms(
  const std::optional<std::chrono::milliseconds>& ms) {
    if (ms.has_value()) {
        if (ms.value() < 1ms || ms.value() > serde::max_serializable_ms) {
            return fmt::format(
              "tombstone_retention_ms should be in range: [1, {}]",
              serde::max_serializable_ms);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_iceberg_partition_spec(const ss::sstring& value) {
    auto parsed = datalake::parse_partition_spec(value);
    if (parsed.has_error()) {
        return fmt::format(
          "couldn't parse iceberg partition spec `{}': {}",
          value,
          parsed.error());
    }
    if (!parsed.value().is_valid_for_default_spec()) {
        return fmt::format(
          "partition spec `{}' can't be used as a default spec", value);
    }
    return std::nullopt;
}

std::optional<ss::sstring> validate_iceberg_topic_name_dot_replacement(
  const std::optional<ss::sstring>& value) {
    if (value.has_value() && value->find('.') != ss::sstring::npos) {
        return "iceberg_topic_name_dot_replacement cannot contain dots";
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_iceberg_default_catalog_namespace(const std::vector<ss::sstring>& ns) {
    if (ns.empty()) {
        return "Iceberg namespace must contain at least one element";
    }
    for (const auto& s : ns) {
        if (s.empty()) {
            return "Iceberg namespace elements cannot be empty strings";
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_iceberg_rest_catalog_auth_mode(const config::configuration& config) {
    auto auth_mode = config.iceberg_rest_catalog_authentication_mode();
    switch (auth_mode) {
    case datalake_catalog_auth_mode::none:
        return std::nullopt;
    case datalake_catalog_auth_mode::bearer: {
        const auto& token = config.iceberg_rest_catalog_token;
        if (!token().has_value()) {
            return fmt::format(
              "Must set {} when iceberg_rest_catalog_authentication_mode is "
              "set to {}.",
              token.name(),
              auth_mode);
        }
        break;
    }
    case datalake_catalog_auth_mode::oauth2: {
        const auto& client_id = config.iceberg_rest_catalog_client_id;
        const auto& client_secret = config.iceberg_rest_catalog_client_secret;
        if (!(client_id().has_value() && client_secret().has_value())) {
            return fmt::format(
              "Must set both of {} and {} when "
              "iceberg_rest_catalog_authentication_mode is "
              "set to {}.",
              client_id.name(),
              client_secret.name(),
              auth_mode);
        }
        break;
    }
    case datalake_catalog_auth_mode::aws_sigv4: {
        // Determine effective credentials source
        auto effective_creds_source
          = config.iceberg_rest_catalog_aws_credentials_source().has_value()
              ? config.iceberg_rest_catalog_aws_credentials_source().value()
              : config.cloud_storage_credentials_source();

        // When using aws_instance_metadata, AWS credentials are not required
        if (
          effective_creds_source
          == model::cloud_credentials_source::aws_instance_metadata) {
            // We still require the region of the Glue endpoint.
            auto effective_region
              = config.iceberg_rest_catalog_aws_region().has_value()
                  ? config.iceberg_rest_catalog_aws_region()
                  : config.cloud_storage_region();
            if (!effective_region.has_value()) {
                return fmt::format(
                  "Must set AWS region when using SigV4 authentication with "
                  "aws_instance_metadata credentials source.");
            }
        } else {
            auto effective_access_key
              = config.iceberg_rest_catalog_aws_access_key().has_value()
                  ? config.iceberg_rest_catalog_aws_access_key()
                  : config.cloud_storage_access_key();
            auto effective_secret_key
              = config.iceberg_rest_catalog_aws_secret_key().has_value()
                  ? config.iceberg_rest_catalog_aws_secret_key()
                  : config.cloud_storage_secret_key();
            auto effective_region
              = config.iceberg_rest_catalog_aws_region().has_value()
                  ? config.iceberg_rest_catalog_aws_region()
                  : config.cloud_storage_region();

            if (!(effective_region.has_value()
                  && effective_access_key.has_value()
                  && effective_secret_key.has_value())) {
                return fmt::format(
                  "Must set AWS region, access key, and secret key when "
                  "iceberg_rest_catalog_authentication_mode is set to {} with "
                  "config_file credentials source. Configure either "
                  "iceberg-specific "
                  "parameters (iceberg_rest_catalog_aws_region, "
                  "iceberg_rest_catalog_aws_access_key, "
                  "iceberg_rest_catalog_aws_secret_key) or cloud storage "
                  "parameters).",
                  auth_mode);
            }
        }
        break;
    }
    case datalake_catalog_auth_mode::gcp: {
        // We implicitly use instance metadata when GCP auth mode is chosen.
        break;
    }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_iceberg_rest_catalog_config(const config::configuration& config) {
    auto catalog_type = config.iceberg_catalog_type();
    if (catalog_type == datalake_catalog_type::rest) {
        const auto& endpoint = config.iceberg_rest_catalog_endpoint;
        if (!endpoint().has_value()) {
            return fmt::format(
              "Must set {} when iceberg_catalog_type is set to 'rest'",
              endpoint.name());
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_consumer_group_metrics(const std::vector<ss::sstring>& metrics) {
    constexpr auto supported = std::to_array<std::string_view>(
      {"group", "partition", "consumer_lag"});

    // Validate results
    for (const auto& m : metrics) {
        if (std::ranges::none_of(supported, [&m](const auto& s) {
                return s == m;
            })) {
            return ssx::sformat("'{}' is not a valid consumer group metric", m);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_cloud_storage_cluster_name(const std::optional<ss::sstring>& input) {
    // Long enough to be useful, short enough not to hit object storage name
    // length limits in most cases.
    constexpr size_t max_cluster_name_length = 64;

    if (!input.has_value()) {
        return std::nullopt;
    }

    if (
      auto non_empty_string_opt = validate_non_empty_string_opt(input);
      non_empty_string_opt.has_value()) {
        return non_empty_string_opt;
    }

    if (input->length() > max_cluster_name_length) {
        return fmt::format(
          "Length must be at most {} characters", max_cluster_name_length);
    }

    for (char c : *input) {
        if (!std::isalnum(c) && !(c == '-' || c == '_')) {
            return "Only alphanumeric characters, hyphens, and underscores are "
                   "allowed";
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_cloud_topics_reconciliation_intervals(const configuration& config) {
    auto min_interval = config.cloud_topics_reconciliation_min_interval();
    auto max_interval = config.cloud_topics_reconciliation_max_interval();

    if (min_interval > max_interval) {
        return fmt::format(
          "cloud_topics_reconciliation_min_interval ({}) must be less than or "
          "equal to cloud_topics_reconciliation_max_interval ({})",
          min_interval.count(),
          max_interval.count());
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_default_redpanda_storage_mode(const configuration& config) {
    auto mode = config.default_redpanda_storage_mode();

    if (
      mode == model::redpanda_storage_mode::tiered
      && !config.cloud_storage_enabled()) {
        return fmt::format(
          "default_redpanda_storage_mode cannot be set to tiered when "
          "cloud_storage_enabled is false");
    }

    if (
      mode == model::redpanda_storage_mode::cloud
      && !config.cloud_topics_enabled()) {
        return fmt::format(
          "default_redpanda_storage_mode cannot be set to cloud when "
          "cloud_topics_enabled is false");
    }

    if (
      mode == model::redpanda_storage_mode::tiered_cloud
      && !config.cloud_topics_enabled()) {
        return fmt::format(
          "default_redpanda_storage_mode cannot be set to tiered_cloud when "
          "cloud_topics_enabled is false");
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_sane_partition_balancer_timeouts(const configuration& config) {
    // how often node status sends an rpc
    auto node_status = config.node_status_interval();
    // in pbp, if 7 consecutive node statuses are missed, the node is considered
    // down for the purposes of determining alive / dead quorums
    auto node_unresponsiveness = 7 * node_status;
    // how often the partition balancer runs
    auto pbp_tick_interval = config.partition_autobalancing_tick_interval_ms();
    // timeout after which the partition balancer will start draining partitions
    // from a node
    auto node_availability
      = config.partition_autobalancing_node_availability_timeout_sec();
    // timeout or nullopt. If timeout, its the timeout after which the partition
    // balancer may consider a node for automatic decommissioning
    auto maybe_auto_decom_timeout
      = config.partition_autobalancing_node_autodecommission_timeout_sec();
    // how often the health report refreshes of its own accord
    auto health_report_tick_time = config.health_monitor_tick_interval();

    // pbp is written under the assumption that node_status << pbp_tick_interval
    if (node_status > pbp_tick_interval) {
        return fmt::format(
          "node_status_interval ({}) must be less than or equal to "
          "partition_autobalancing_tick_interval_ms ({})",
          node_status,
          pbp_tick_interval);
    }

    // node_unresponsiveness is when a node is considered down from the
    // perspective of quorum liveness, aka pbp will consider partitions
    // immutable if their source quorum is down.
    // node_availability is the time at which a node is preemptively drained of
    // replicas. The sane assumption is that node_unresponsiveness <
    // node_availability
    if (node_unresponsiveness > node_availability) {
        return fmt::format(
          "node_status_interval * 7 ({}) should be less than "
          "partition_autobalancing_node_availability_timeout_sec ({})",
          node_unresponsiveness,
          node_availability);
    }

    // pbp relies on health reports for feedback on how its last round of
    // balancing actions worked out. If the health report interval is greater
    // than the pbp_tick interval, pbp may exhibit oscillating behavior where it
    // overshoots balance. It is best to keep the health report interval lower
    // than the pbp tick
    if (pbp_tick_interval < health_report_tick_time) {
        return fmt::format(
          "health_monitor_tick_interval ({}) must be less than "
          "partition_autobalancing_tick_interval_ms ({})",
          health_report_tick_time,
          pbp_tick_interval);
    }

    // sanity check, we should drain but not decommission a node before we
    // attempt to eject it from the cluster
    if (
      maybe_auto_decom_timeout
      && std::chrono::duration_cast<std::chrono::seconds>(node_availability)
           > maybe_auto_decom_timeout) {
        return fmt::format(
          "partition_autobalancing_node_availability_timeout_sec ({}) must be "
          "less than partition_autobalancing_node_autodecommission_timeout_sec "
          "({})",
          node_availability,
          *maybe_auto_decom_timeout);
    }
    return std::nullopt;
}

}; // namespace config
