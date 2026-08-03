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

#include "cluster_link/model/types.h"
#include "kafka/client/cluster.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/types.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <concepts>
#include <expected>
#include <optional>

namespace cluster_link {
kafka::client::connection_configuration
metadata_to_kafka_config(const model::metadata&);

/// Converts a CA certificate given as either a file path or an inline value.
net::certificate to_certificate(const model::tls_file_or_value&);

/// Converts a client key/cert pair given as either file paths or inline
/// values. Both must be given in the same form (asserts otherwise -- the
/// admin converter that produces `model::tls_file_or_value` writes key and
/// cert from the same source, so a mismatch is unreachable in practice).
net::key_store to_key_store(
  const model::tls_file_or_value& key, const model::tls_file_or_value& cert);

/// Returns true if `have` includes every bit set in `required`.
template<typename T>
bool has_required_permissions(T have, T required) {
    return (have & required) == required;
}

/// Cluster-level DESCRIBE permission (0x100), required on the source cluster to
/// read security objects (ACLs, roles) for replication.
inline constexpr auto cluster_describe_permission
  = kafka::cluster_authorized_operations{0x100};

/// A Kafka API whose highest supported wire version can be negotiated: a
/// KafkaApi (providing `key`) that also exposes the `max_valid` version this
/// build supports.
template<typename T>
concept NegotiableKafkaApi = kafka::KafkaApi<T> && requires {
    { T::max_valid } -> std::convertible_to<const kafka::api_version&>;
};

/// Negotiates the wire version to use for the Kafka API `ApiT` against the
/// source `cluster`: the highest version both the source and this build
/// support. Error messages use `ApiT::name` (the wire protocol API name). On
/// success returns the negotiated version; on failure returns an error string
/// suitable for a task state_transition reason. Rethrows shutdown exceptions so
/// callers can propagate aborts.
///
/// Set `broker` to negotiate against that broker alone, for a request
/// dispatched to one broker: a cluster-wide negotiation refuses whenever any
/// broker fails to report, however healthy the one being dispatched to. Set
/// `floor` to the lowest version the caller can use, so a source supporting
/// nothing at or above it is reported unsupported rather than answered at a
/// version whose wire format drops fields the caller relies on.
template<NegotiableKafkaApi ApiT>
ss::future<std::expected<kafka::api_version, ss::sstring>>
negotiate_api_version(
  kafka::client::cluster& cluster,
  ss::abort_source& as,
  std::optional<::model::node_id> broker = std::nullopt,
  kafka::api_version floor = ApiT::min_valid) {
    try {
        auto supported_api_versions = co_await (
          broker.has_value()
            ? cluster.supported_api_versions(*broker, ApiT::key, as)
            : cluster.supported_api_versions(ApiT::key, as));
        if (!supported_api_versions.has_value()) {
            co_return std::unexpected(
              ssx::sformat(
                "Failed to get supported API version for {}", ApiT::name));
        }
        if (supported_api_versions->min > ApiT::max_valid) {
            co_return std::unexpected(
              ssx::sformat(
                "Unsupported API version for {}: {}",
                ApiT::name,
                supported_api_versions->min));
        }
        if (supported_api_versions->max < floor) {
            co_return std::unexpected(
              ssx::sformat(
                "Unsupported API version for {}: source supports at most {}, "
                "below the {} required",
                ApiT::name,
                supported_api_versions->max,
                floor));
        }
        co_return std::min(supported_api_versions->max, ApiT::max_valid);
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            // Propagate shutdown rather than reporting it as a negotiation
            // failure, which would fault the caller's task on the way down.
            std::rethrow_exception(ex);
        }
        co_return std::unexpected(
          ssx::sformat(
            "Failed to get supported API version for {}: {}", ApiT::name, ex));
    }
}

} // namespace cluster_link
