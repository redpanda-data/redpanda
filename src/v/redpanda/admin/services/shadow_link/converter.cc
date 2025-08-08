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

#include "redpanda/admin/services/shadow_link/converter.h"

#include "cluster_link/model/types.h"

#include <stdexcept>

using namespace std::chrono_literals;

namespace admin {
using proto::admin::authentication_configuration;
using proto::admin::create_shadow_link_request;
using proto::admin::name_filter;
using proto::admin::scram_config;
using proto::admin::scram_mechanism;
using proto::admin::tls_file_settings;
using proto::admin::tls_settings;
using proto::admin::tlspem_settings;
using proto::admin::topic_metadata_sync_options;
namespace {

constexpr auto to_filter_pattern_type(proto::admin::pattern_type p) {
    switch (p) {
    case proto::admin::pattern_type::unspecified:
        throw std::invalid_argument("pattern_type is unspecified");
    case proto::admin::pattern_type::literal:
        return cluster_link::model::filter_pattern_type::literal;
    case proto::admin::pattern_type::prefix:
        return cluster_link::model::filter_pattern_type::prefix;
    }
}

constexpr auto to_filter_type(proto::admin::filter_type f) {
    switch (f) {
    case proto::admin::filter_type::unspecified:
        throw std::invalid_argument("filter_type is unspecified");
    case proto::admin::filter_type::include:
        return cluster_link::model::filter_type::include;
    case proto::admin::filter_type::exclude:
        return cluster_link::model::filter_type::exclude;
    }
}

chunked_vector<cluster_link::model::resource_name_filter_pattern>
to_filter_patterns(const chunked_vector<name_filter>& proto_filters) {
    chunked_vector<cluster_link::model::resource_name_filter_pattern> filters;
    filters.reserve(proto_filters.size());
    std::ranges::transform(
      proto_filters, std::back_inserter(filters), [](const name_filter& f) {
          return cluster_link::model::resource_name_filter_pattern{
            .pattern_type = to_filter_pattern_type(f.get_pattern_type()),
            .filter = to_filter_type(f.get_filter_type()),
            .pattern = f.get_name()};
      });

    return filters;
}

cluster_link::model::topic_metadata_mirroring_config
create_topic_metadata_mirroring_config(
  const topic_metadata_sync_options& options) {
    cluster_link::model::topic_metadata_mirroring_config config;

    config.task_interval = options.get_interval() == absl::ZeroDuration()
                             ? 30s
                             : absl::ToChronoNanoseconds(
                                 options.get_interval());

    config.topic_name_filters = to_filter_patterns(options.get_topic_filters());

    std::ranges::copy(
      options.get_shadowed_topic_properties(),
      std::inserter(
        config.topic_properties_to_mirror,
        config.topic_properties_to_mirror.end()));

    return config;
}

cluster_link::model::link_configuration
create_link_configuration(const create_shadow_link_request& req) {
    cluster_link::model::link_configuration config;
    config.topic_metadata_mirroring_cfg
      = create_topic_metadata_mirroring_config(
        req.get_shadow_link()
          .get_configurations()
          .get_topic_metadata_sync_options());

    return config;
}

/// \brief Converts protobuf scram_mechanism to string
/// \throws std::invalid_argument if the mechanism is unspecified
constexpr auto scram_mechanism_to_string(scram_mechanism m)
  -> std::string_view {
    switch (m) {
    case scram_mechanism::scram_sha_256:
        return "SCRAM-SHA-256";
    case scram_mechanism::scram_sha_512:
        return "SCRAM-SHA-512";
    case proto::admin::scram_mechanism::unspecified:
        break;
    }
    throw std::invalid_argument("scram_mechanism is unspecified, must be set "
                                "to either SCRAM-SHA-256 or SCRAM-SHA-512");
}

/// \brief Sets client ID to the format:
/// "cluster-link-{cluster-link-name}-{cluster-link-uuid}"
void set_client_id(cluster_link::model::metadata& md) {
    md.connection.client_id = ssx::sformat(
      "cluster-link-{}-{}", md.name, md.uuid);
}

/// \brief Creates the authentication settings from the create cluster link
/// \brief throws std::invalid_argument if invalid config provided
cluster_link::model::connection_config::authn_variant
create_authn_settings(const authentication_configuration& authn_config) {
    return authn_config.visit_authentication(
      [](const scram_config& scram)
        -> cluster_link::model::connection_config::authn_variant {
          cluster_link::model::scram_credentials creds;
          creds.username = scram.get_username();
          creds.password = scram.get_password();
          creds.mechanism = ss::sstring{
            scram_mechanism_to_string(scram.get_scram_mechanism())};
          return creds;
      },
      [](std::monostate)
        -> cluster_link::model::connection_config::authn_variant {
          throw std::invalid_argument(
            "authentication_configuration is set but not provided");
      });
}
/// \brief Sets TLS settings
/// \throws std::invalid_argument If key and cert are inconsistent
void set_tls_settings(
  cluster_link::model::connection_config& config, const tls_settings& tls) {
    tls.visit_tls_settings(
      [&config](const tls_file_settings& file) {
          if (!file.get_ca_path().empty()) {
              config.ca = cluster_link::model::tls_file_path(
                file.get_ca_path());
          }
          if (!file.get_key_path().empty()) {
              config.key = cluster_link::model::tls_file_path(
                file.get_key_path());
          }
          if (!file.get_cert_path().empty()) {
              config.cert = cluster_link::model::tls_file_path(
                file.get_cert_path());
          }
          if (config.key.has_value() != config.cert.has_value()) {
              throw std::invalid_argument(
                "Must provide both key and cert or neither");
          }
      },
      [&config](const tlspem_settings& pem) {
          if (!pem.get_ca().empty()) {
              config.ca = cluster_link::model::tls_value(pem.get_ca());
          }
          if (!pem.get_key().empty()) {
              config.key = cluster_link::model::tls_value(pem.get_key());
          }
          if (!pem.get_cert().empty()) {
              config.cert = cluster_link::model::tls_value(pem.get_cert());
          }
          if (config.key.has_value() != config.cert.has_value()) {
              throw std::invalid_argument(
                "Must provide both key and cert or neither");
          }
      },
      [](std::monostate) {});
}
/// \brief Creates a connection config from the create cluster link request
/// \throws std::invalid_argument if the bootstrap servers are not valid
cluster_link::model::connection_config
create_connection_config(const create_shadow_link_request& req) {
    cluster_link::model::connection_config config;
    const auto& client_options
      = req.get_shadow_link().get_configurations().get_client_options();
    const auto& bootstrap_servers = client_options.get_bootstrap_servers();
    if (bootstrap_servers.empty()) {
        throw std::invalid_argument(
          "bootstrap_servers must not be empty in the client options");
    }
    std::ranges::transform(
      bootstrap_servers,
      std::back_inserter(config.bootstrap_servers),
      [](const auto& b) { return net::unresolved_address::parse_address(b); });

    if (client_options.has_authentication_configuration()) {
        config.authn_config = create_authn_settings(
          client_options.get_authentication_configuration());
    }

    if (client_options.has_tls_settings()) {
        set_tls_settings(config, client_options.get_tls_settings());
    }

    return config;
}
} // namespace
cluster_link::model::metadata
convert_create_to_metadata(create_shadow_link_request req) {
    cluster_link::model::metadata metadata;

    try {
        auto& cluster_link = req.get_shadow_link();

        metadata.name = cluster_link::model::name_t{
          std::move(cluster_link.get_name())};
        metadata.uuid = cluster_link::model::uuid_t(uuid_t::create());
        metadata.connection = create_connection_config(req);
        metadata.configuration = create_link_configuration(req);

        set_client_id(metadata);
        return metadata;
    } catch (const serde::pb::rpc::base_exception& e) {
        throw;
    } catch (const std::invalid_argument& e) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat("Invalid cluster link configuration: {}", e.what()));
    } catch (const std::exception& e) {
        throw serde::pb::rpc::unknown_exception(
          ssx::sformat("Failed to convert create cluster link request: {}", e));
    }
}
} // namespace admin
