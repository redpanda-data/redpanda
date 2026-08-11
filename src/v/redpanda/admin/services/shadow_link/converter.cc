/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "redpanda/admin/services/shadow_link/converter.h"

#include "bytes/iobuf_parser.h"
#include "cluster_link/model/types.h"
#include "config/configuration.h"
#include "crypto/crypto.h"
#include "serde/protobuf/rpc.h"
#include "utils/base64.h"

#include <seastar/core/memory.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <new>
#include <optional>
#include <stdexcept>
#include <variant>

using namespace std::chrono_literals;

namespace admin {
using proto::admin::acl_access_filter;
using proto::admin::acl_filter;
using proto::admin::acl_resource_filter;
using proto::admin::authentication_configuration;
using proto::admin::consumer_offset_sync_options;
using proto::admin::create_shadow_link_request;
using proto::admin::name_filter;
using proto::admin::plain_config;
using proto::admin::schema_registry_sync_options;
using proto::admin::schema_registry_sync_options_shadow_schema_registry_topic;
using proto::admin::scram_config;
using proto::admin::scram_mechanism;
using proto::admin::security_settings_sync_options;
using proto::admin::shadow_link;
using proto::admin::shadow_link_client_options;
using proto::admin::shadow_link_configurations;
using proto::admin::shadow_link_status;
using proto::admin::shadow_link_task_status;
using proto::admin::shadow_topic;
using proto::admin::shadow_topic_status;
using proto::admin::task_state;
using proto::admin::topic_metadata_sync_options;
using proto::admin::topic_metadata_sync_options_earliest_offset;
using proto::admin::topic_metadata_sync_options_latest_offset;
using proto::admin::topic_partition_information;
using proto::admin::update_shadow_link_request;
using proto::common::acl_operation;
using proto::common::acl_pattern;
using proto::common::acl_permission_type;
using proto::common::acl_resource;
using proto::common::tls_file_settings;
using proto::common::tls_settings;
using proto::common::tlspem_settings;
namespace {

/// Converts an iobuf to ss::sstring for TLS PEM data.
///
/// Note: This creates a contiguous allocation which may exceed 128KiB for large
/// CA bundles. While large allocations are generally discouraged in Seastar,
/// this is acceptable here because the Seastar TLS layer (set_x509_key, etc.)
/// requires linearized certificate data anyway.
ss::sstring iobuf_to_string(const iobuf& buf) {
    auto sz = buf.size_bytes();

    // Temporarily disable crash-on-allocation-failure so we can convert
    // std::bad_alloc to an RPC error instead of crashing the broker.
    try {
        ss::memory::scoped_system_alloc_fallback fb;
        iobuf_const_parser p(buf);
        return p.read_string(sz);
    } catch (const std::bad_alloc&) {
        throw serde::pb::rpc::resource_exhausted_exception(
          ssx::sformat(
            "TLS certificate data too large to linearize ({} bytes)", sz));
    }
}

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

constexpr auto mirror_topic_state_to_shadow_topic_state(
  cluster_link::model::mirror_topic_status s) {
    switch (s) {
    case cluster_link::model::mirror_topic_status::active:
        return proto::admin::shadow_topic_state::active;
    case cluster_link::model::mirror_topic_status::failed:
        return proto::admin::shadow_topic_state::faulted;
    case cluster_link::model::mirror_topic_status::paused:
        return proto::admin::shadow_topic_state::paused;
    case cluster_link::model::mirror_topic_status::promoted:
        return proto::admin::shadow_topic_state::promoted;
    case cluster_link::model::mirror_topic_status::failing_over:
        return proto::admin::shadow_topic_state::failing_over;
    case cluster_link::model::mirror_topic_status::failed_over:
        return proto::admin::shadow_topic_state::failed_over;
    case cluster_link::model::mirror_topic_status::promoting:
        return proto::admin::shadow_topic_state::promoting;
    }
}

constexpr auto convert_link_status(cluster_link::model::link_status s) {
    using proto::admin::shadow_link_state;
    switch (s) {
    case cluster_link::model::link_status::active:
        return shadow_link_state::active;
    case cluster_link::model::link_status::paused:
        return shadow_link_state::paused;
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

    if (options.get_interval() > absl::ZeroDuration()) {
        config.task_interval = absl::ToChronoNanoseconds(
          options.get_interval());
    }

    config.topic_name_filters = to_filter_patterns(
      options.get_auto_create_shadow_topic_filters());

    std::ranges::copy(
      options.get_synced_shadow_topic_properties(),
      std::inserter(
        config.topic_properties_to_mirror,
        config.topic_properties_to_mirror.end()));
    config.exclude_default = options.get_exclude_default();

    options.visit_start_offset(
      [&config](std::monostate) { config.starting_offset = std::nullopt; },
      [&config](const topic_metadata_sync_options_earliest_offset&) {
          config.starting_offset = cluster_link::model::earliest_offset_ts;
      },
      [&config](const topic_metadata_sync_options_latest_offset&) {
          config.starting_offset = cluster_link::model::latest_offset_ts;
      },
      [&config](absl::Time t) {
          config.starting_offset = model::timestamp(absl::ToUnixMillis(t));
      });

    config.is_enabled = cluster_link::model::enabled_t{!options.get_paused()};

    return config;
}

cluster_link::model::schema_registry_sync_config
create_schema_registry_sync_config(
  const schema_registry_sync_options& options) {
    cluster_link::model::schema_registry_sync_config config;

    options.visit_schema_registry_shadowing_mode(
      [&config](
        const schema_registry_sync_options_shadow_schema_registry_topic&) {
          config.sync_schema_registry_topic_mode = cluster_link::model::
            schema_registry_sync_config::shadow_entire_schema_registry{};
      },
      [&config](std::monostate) {
          config.sync_schema_registry_topic_mode = std::nullopt;
      });

    return config;
}

cluster_link::model::consumer_groups_mirroring_config
create_consumer_groups_mirroring_config(
  const proto::admin::consumer_offset_sync_options& options) {
    cluster_link::model::consumer_groups_mirroring_config config;

    if (options.get_interval() > absl::ZeroDuration()) {
        config.task_interval = absl::ToChronoNanoseconds(
          options.get_interval());
    }

    config.filters = to_filter_patterns(options.get_group_filters());

    config.is_enabled = cluster_link::model::enabled_t{!options.get_paused()};

    return config;
}

cluster_link::model::acl_resource to_acl_resource(acl_resource r) {
    switch (r) {
    case proto::common::acl_resource::unspecified:
        throw std::invalid_argument("acl_resource is unspecified");
    case proto::common::acl_resource::any:
        return cluster_link::model::acl_resource::any;
    case proto::common::acl_resource::cluster:
        return cluster_link::model::acl_resource::cluster;
    case proto::common::acl_resource::group:
        return cluster_link::model::acl_resource::group;
    case proto::common::acl_resource::topic:
        return cluster_link::model::acl_resource::topic;
    case proto::common::acl_resource::txn_id:
        return cluster_link::model::acl_resource::txn_id;
    case proto::common::acl_resource::sr_subject:
        return cluster_link::model::acl_resource::schema_registry_subject;
    case proto::common::acl_resource::sr_registry:
        return cluster_link::model::acl_resource::schema_registry_global;
    case proto::common::acl_resource::sr_any:
        return cluster_link::model::acl_resource::schema_registry_any;
    }
}

cluster_link::model::acl_pattern to_acl_pattern(acl_pattern p) {
    switch (p) {
    case proto::common::acl_pattern::unspecified:
        throw std::invalid_argument("acl_pattern is unspecified");
    case proto::common::acl_pattern::any:
        return cluster_link::model::acl_pattern::any;
    case proto::common::acl_pattern::literal:
        return cluster_link::model::acl_pattern::literal;
    case proto::common::acl_pattern::prefixed:
        return cluster_link::model::acl_pattern::prefixed;
    case proto::common::acl_pattern::match:
        return cluster_link::model::acl_pattern::match;
    }
}

cluster_link::model::acl_resource_filter
to_resource_filter(const acl_resource_filter& proto_resource_filter) {
    cluster_link::model::acl_resource_filter filter;

    filter.resource_type = to_acl_resource(
      proto_resource_filter.get_resource_type());
    filter.pattern_type = to_acl_pattern(
      proto_resource_filter.get_pattern_type());

    if (!proto_resource_filter.get_name().empty()) {
        filter.name = proto_resource_filter.get_name();
    }

    return filter;
}

cluster_link::model::acl_operation to_acl_operation(acl_operation op) {
    switch (op) {
    case proto::common::acl_operation::unspecified:
        throw std::invalid_argument("acl_operation is unspecified");
    case proto::common::acl_operation::any:
        return cluster_link::model::acl_operation::any;
    case proto::common::acl_operation::read:
        return cluster_link::model::acl_operation::read;
    case proto::common::acl_operation::write:
        return cluster_link::model::acl_operation::write;
    case proto::common::acl_operation::create:
        return cluster_link::model::acl_operation::create;
    case proto::common::acl_operation::remove:
        return cluster_link::model::acl_operation::remove;
    case proto::common::acl_operation::alter:
        return cluster_link::model::acl_operation::alter;
    case proto::common::acl_operation::describe:
        return cluster_link::model::acl_operation::describe;
    case proto::common::acl_operation::cluster_action:
        return cluster_link::model::acl_operation::cluster_action;
    case proto::common::acl_operation::describe_configs:
        return cluster_link::model::acl_operation::describe_configs;
    case proto::common::acl_operation::alter_configs:
        return cluster_link::model::acl_operation::alter_configs;
    case proto::common::acl_operation::idempotent_write:
        return cluster_link::model::acl_operation::idempotent_write;
    }
}

cluster_link::model::acl_permission_type
to_acl_permission_type(acl_permission_type t) {
    switch (t) {
    case proto::common::acl_permission_type::unspecified:
        throw std::invalid_argument("acl_permission_type unspecified");
    case proto::common::acl_permission_type::any:
        return cluster_link::model::acl_permission_type::any;
    case proto::common::acl_permission_type::allow:
        return cluster_link::model::acl_permission_type::allow;
    case proto::common::acl_permission_type::deny:
        return cluster_link::model::acl_permission_type::deny;
    }
}

cluster_link::model::acl_access_filter
to_access_filter(const acl_access_filter& proto_access_filter) {
    cluster_link::model::acl_access_filter filter;

    if (!proto_access_filter.get_principal().empty()) {
        filter.principal = proto_access_filter.get_principal();
    }

    filter.operation = to_acl_operation(proto_access_filter.get_operation());
    filter.permission_type = to_acl_permission_type(
      proto_access_filter.get_permission_type());

    if (!proto_access_filter.get_host().empty()) {
        filter.host = proto_access_filter.get_host();
    }

    return filter;
}

cluster_link::model::acl_filter to_acl_filter(const acl_filter& proto_filter) {
    return {
      .resource_filter = to_resource_filter(proto_filter.get_resource_filter()),
      .access_filter = to_access_filter(proto_filter.get_access_filter()),
    };
}

chunked_vector<cluster_link::model::acl_filter>
to_acl_filters(const chunked_vector<acl_filter>& proto_filters) {
    chunked_vector<cluster_link::model::acl_filter> filters;
    filters.reserve(proto_filters.size());
    std::ranges::transform(
      proto_filters, std::back_inserter(filters), [](const acl_filter& f) {
          return to_acl_filter(f);
      });

    return filters;
}

cluster_link::model::security_settings_sync_config
create_security_settings_sync_config(
  const security_settings_sync_options& options) {
    cluster_link::model::security_settings_sync_config config;

    if (options.get_interval() > absl::ZeroDuration()) {
        config.task_interval = absl::ToChronoNanoseconds(
          options.get_interval());
    }

    config.acl_filters = to_acl_filters(options.get_acl_filters());

    config.is_enabled = cluster_link::model::enabled_t{!options.get_paused()};

    return config;
}

cluster_link::model::link_configuration
create_link_configuration(const shadow_link& sl) {
    cluster_link::model::link_configuration config;
    config.topic_metadata_mirroring_cfg
      = create_topic_metadata_mirroring_config(
        sl.get_configurations().get_topic_metadata_sync_options());

    config.security_settings_sync_cfg = create_security_settings_sync_config(
      sl.get_configurations().get_security_sync_options());

    config.consumer_groups_mirroring_cfg
      = create_consumer_groups_mirroring_config(
        sl.get_configurations().get_consumer_offset_sync_options());

    config.schema_registry_sync_cfg = create_schema_registry_sync_config(
      sl.get_configurations().get_schema_registry_sync_options());

    return config;
}

/// \brief Converts protobuf scram_mechanism to string
/// \throws std::invalid_argument if the mechanism is unspecified
constexpr auto scram_mechanism_to_string(scram_mechanism m) {
    switch (m) {
    case scram_mechanism::scram_sha_256:
        return "SCRAM-SHA-256";
    case scram_mechanism::scram_sha_512:
        return "SCRAM-SHA-512";
    case proto::admin::scram_mechanism::unspecified:
        break;
    }
    throw std::invalid_argument(
      "scram_mechanism is unspecified, must be set "
      "to either SCRAM-SHA-256 or SCRAM-SHA-512");
}

/// \brief Creates the authentication variant
///
/// \throws std::invalid_argument if invalid config provided
cluster_link::model::connection_config::authn_variant
create_authn_settings(const authentication_configuration& authn_config) {
    return authn_config.visit_authentication(
      [](const scram_config& scram)
        -> cluster_link::model::connection_config::authn_variant {
          cluster_link::model::scram_credentials creds;
          if (
            scram.get_username().empty() || scram.get_password().empty()
            || scram.get_scram_mechanism()
                 == proto::admin::scram_mechanism::unspecified) {
              throw std::invalid_argument(
                "When setting SCRAM configuration, must provide username, "
                "password, and mechanism");
          }
          creds.username = scram.get_username();
          creds.password = scram.get_password();
          creds.mechanism = ss::sstring{
            scram_mechanism_to_string(scram.get_scram_mechanism())};
          return creds;
      },
      [](const plain_config& plain)
        -> cluster_link::model::connection_config::authn_variant {
          if (plain.get_username().empty() || plain.get_password().empty()) {
              throw std::invalid_argument(
                "When setting PLAIN configuration, must provide username and "
                "password");
          }
          cluster_link::model::scram_credentials creds;
          creds.username = plain.get_username();
          creds.password = plain.get_password();
          creds.mechanism = "PLAIN";
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
    config.tls_enabled = cluster_link::model::connection_config::tls_enabled_t{
      tls.get_enabled()};
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
              config.ca = cluster_link::model::tls_value(
                iobuf_to_string(pem.get_ca()));
          }
          if (!pem.get_key().empty()) {
              config.key = cluster_link::model::tls_value(
                iobuf_to_string(pem.get_key()));
          }
          if (!pem.get_cert().empty()) {
              config.cert = cluster_link::model::tls_value(
                iobuf_to_string(pem.get_cert()));
          }
          if (config.key.has_value() != config.cert.has_value()) {
              throw std::invalid_argument(
                "Must provide both key and cert or neither");
          }
      },
      [](std::monostate) {});

    config.tls_provide_sni
      = cluster_link::model::connection_config::tls_provide_sni_t{
        !tls.get_do_not_set_sni_hostname()};
}
/// \brief Creates a connection config from the create cluster link
/// request
/// \throws std::invalid_argument if the bootstrap servers are not valid
cluster_link::model::connection_config
create_connection_config(const shadow_link& sl) {
    cluster_link::model::connection_config config;
    const auto& client_options = sl.get_configurations().get_client_options();
    const auto& bootstrap_servers = client_options.get_bootstrap_servers();
    if (bootstrap_servers.empty()) {
        throw std::invalid_argument(
          "bootstrap_servers must not be empty in the client options");
    }
    std::ranges::transform(
      bootstrap_servers,
      std::back_inserter(config.bootstrap_servers),
      [](const auto& b) { return net::unresolved_address::from_string(b); });

    if (client_options.has_authentication_configuration()) {
        config.authn_config = create_authn_settings(
          client_options.get_authentication_configuration());
    }

    if (client_options.has_tls_settings()) {
        set_tls_settings(config, client_options.get_tls_settings());
    }

    if (client_options.get_metadata_max_age_ms() != 0) {
        config.metadata_max_age_ms = client_options.get_metadata_max_age_ms();
    }

    if (client_options.get_connection_timeout_ms() != 0) {
        config.connection_timeout_ms
          = client_options.get_connection_timeout_ms();
    }

    if (client_options.get_retry_backoff_ms() != 0) {
        config.retry_backoff_ms = client_options.get_retry_backoff_ms();
    }

    if (client_options.get_fetch_wait_max_ms() != 0) {
        config.fetch_wait_max_ms = client_options.get_fetch_wait_max_ms();
    }

    if (client_options.get_fetch_min_bytes() != 0) {
        config.fetch_min_bytes = client_options.get_fetch_min_bytes();
    }

    if (client_options.get_fetch_max_bytes() != 0) {
        config.fetch_max_bytes = client_options.get_fetch_max_bytes();
    }

    if (client_options.get_fetch_partition_max_bytes() != 0) {
        config.fetch_partition_max_bytes
          = client_options.get_fetch_partition_max_bytes();
    }

    return config;
}

authentication_configuration create_authentication_configuration(
  const cluster_link::model::connection_config::authn_variant& authn) {
    return ss::visit(
      authn,
      [](const cluster_link::model::scram_credentials& scram)
        -> authentication_configuration {
          authentication_configuration authn;
          if (scram.mechanism == "PLAIN") {
              plain_config plain_proto;
              plain_proto.set_username(ss::sstring{scram.username});
              plain_proto.set_password_set(true);
              plain_proto.set_password_set_at(
                absl::FromChrono(
                  model::to_time_point(scram.password_last_updated)));
              authn.set_plain_configuration(std::move(plain_proto));
          } else {
              scram_config scram_proto;
              scram_proto.set_username(ss::sstring{scram.username});
              scram_proto.set_password_set(true);
              scram_proto.set_password_set_at(
                absl::FromChrono(
                  model::to_time_point(scram.password_last_updated)));
              scram_proto.set_scram_mechanism(
                proto::admin::scram_mechanism::unspecified);
              if (scram.mechanism == "SCRAM-SHA-256") {
                  scram_proto.set_scram_mechanism(
                    proto::admin::scram_mechanism::scram_sha_256);
              } else if (scram.mechanism == "SCRAM-SHA-512") {
                  scram_proto.set_scram_mechanism(
                    proto::admin::scram_mechanism::scram_sha_512);
              } else {
                  throw std::invalid_argument(
                    ssx::sformat(
                      "Unknown SCRAM mechanism: {}", scram.mechanism));
              }

              authn.set_scram_configuration(std::move(scram_proto));
          }
          return authn;
      });
}

struct tls_visitor {
    explicit tls_visitor(tls_settings* tls_settings)
      : _tls_settings(tls_settings) {}

    void operator()(
      const cluster_link::model::tls_file_path& key,
      const cluster_link::model::tls_file_path& cert) {
        _tls_settings->visit_tls_settings(
          [&key, &cert](tls_file_settings& file_settings) {
              file_settings.set_key_path(ss::sstring{key()});
              file_settings.set_cert_path(ss::sstring{cert()});
          },
          [](tlspem_settings&) {
              throw std::invalid_argument(
                "Cannot set both tls_file_settings and "
                "tls_pem_settings");
          },
          [this, &key, &cert](std::monostate) {
              tls_file_settings file_settings;
              file_settings.set_key_path(ss::sstring{key()});
              file_settings.set_cert_path(ss::sstring{cert()});
              _tls_settings->set_tls_file_settings(std::move(file_settings));
          });
    }

    void operator()(
      const cluster_link::model::tls_value& key,
      const cluster_link::model::tls_value& cert) {
        _tls_settings->visit_tls_settings(
          [](tls_file_settings&) {
              throw std::invalid_argument(
                "Cannot set both tls_file_settings and "
                "tls_pem_settings");
          },
          [&key, &cert](tlspem_settings& pem_settings) {
              auto key_digest = bytes_to_base64(
                crypto::digest(crypto::digest_type::SHA256, key()));
              pem_settings.set_key_fingerprint(std::move(key_digest));
              pem_settings.set_cert(iobuf::from(cert()));
          },
          [this, &key, &cert](std::monostate) {
              tlspem_settings pem_settings;
              auto key_digest = bytes_to_base64(
                crypto::digest(crypto::digest_type::SHA256, key()));
              pem_settings.set_key_fingerprint(std::move(key_digest));
              pem_settings.set_cert(iobuf::from(cert()));
              _tls_settings->set_tls_pem_settings(std::move(pem_settings));
          });
    }

    template<typename T1, typename T2>
    requires(!std::is_same_v<T1, T2>)
    void operator()(const T1&, const T2&) {
        throw std::invalid_argument(
          "TLS key and cert must be of the same type");
    }

    tls_settings* _tls_settings;
};

tls_settings create_tls_settings(const cluster_link::model::metadata& md) {
    tls_settings tls;
    tls.set_enabled(bool(md.connection.tls_enabled));
    if (md.connection.ca.has_value()) {
        ss::visit(
          md.connection.ca.value(),
          [&tls](const cluster_link::model::tls_file_path& path) {
              tls_file_settings file_settings;
              file_settings.set_ca_path(ss::sstring{path});
              tls.set_tls_file_settings(std::move(file_settings));
          },
          [&tls](const cluster_link::model::tls_value& value) {
              tlspem_settings pem_settings;
              pem_settings.set_ca(iobuf::from(value()));
              tls.set_tls_pem_settings(std::move(pem_settings));
          });
    }

    if (md.connection.key.has_value() && md.connection.cert.has_value()) {
        std::visit(
          tls_visitor(&tls),
          md.connection.key.value(),
          md.connection.cert.value());
    }

    tls.set_do_not_set_sni_hostname(!bool(md.connection.tls_provide_sni));

    return tls;
}

shadow_link_client_options
create_shadow_link_client_options(const cluster_link::model::metadata& md) {
    shadow_link_client_options options;

    chunked_vector<ss::sstring> bootstrap_servers;
    bootstrap_servers.reserve(md.connection.bootstrap_servers.size());
    std::ranges::transform(
      md.connection.bootstrap_servers,
      std::back_inserter(bootstrap_servers),
      [](const auto& addr) {
          return ssx::sformat("{}:{}", addr.host(), addr.port());
      });

    options.set_bootstrap_servers(std::move(bootstrap_servers));
    options.set_client_id(ss::sstring{md.connection.client_id});

    if (
      md.connection.tls_enabled || md.connection.ca.has_value()
      || md.connection.cert.has_value() || md.connection.key.has_value()) {
        options.set_tls_settings(create_tls_settings(md));
    }

    if (md.connection.authn_config.has_value()) {
        options.set_authentication_configuration(
          create_authentication_configuration(
            md.connection.authn_config.value()));
    }

    options.set_metadata_max_age_ms(
      md.connection.metadata_max_age_ms.value_or(0));
    options.set_effective_metadata_max_age_ms(
      md.connection.get_metadata_max_age_ms());
    options.set_connection_timeout_ms(
      md.connection.connection_timeout_ms.value_or(0));
    options.set_effective_connection_timeout_ms(
      md.connection.get_connection_timeout_ms());
    options.set_retry_backoff_ms(md.connection.retry_backoff_ms.value_or(0));
    options.set_effective_retry_backoff_ms(
      md.connection.get_retry_backoff_ms());
    options.set_fetch_wait_max_ms(md.connection.fetch_wait_max_ms.value_or(0));
    options.set_effective_fetch_wait_max_ms(
      md.connection.get_fetch_wait_max_ms());
    options.set_fetch_min_bytes(md.connection.fetch_min_bytes.value_or(0));
    options.set_effective_fetch_min_bytes(md.connection.get_fetch_min_bytes());
    options.set_fetch_max_bytes(md.connection.fetch_max_bytes.value_or(0));
    options.set_effective_fetch_max_bytes(md.connection.get_fetch_max_bytes());
    options.set_fetch_partition_max_bytes(
      md.connection.fetch_partition_max_bytes.value_or(0));
    options.set_effective_fetch_partition_max_bytes(
      md.connection.get_fetch_partition_max_bytes());

    return options;
}

constexpr auto
to_proto_filter_pattern(cluster_link::model::filter_pattern_type p) {
    switch (p) {
    case cluster_link::model::filter_pattern_type::literal:
        return proto::admin::pattern_type::literal;
    case cluster_link::model::filter_pattern_type::prefix:
        return proto::admin::pattern_type::prefix;
    }
}

constexpr auto to_proto_filter_type(cluster_link::model::filter_type f) {
    switch (f) {
    case cluster_link::model::filter_type::include:
        return proto::admin::filter_type::include;
    case cluster_link::model::filter_type::exclude:
        return proto::admin::filter_type::exclude;
    }
}

chunked_vector<name_filter> to_name_filters(
  const chunked_vector<cluster_link::model::resource_name_filter_pattern>&
    patterns) {
    chunked_vector<name_filter> filters;
    filters.reserve(patterns.size());

    std::ranges::transform(
      patterns,
      std::back_inserter(filters),
      [](const cluster_link::model::resource_name_filter_pattern& p) {
          name_filter filter;
          filter.set_pattern_type(to_proto_filter_pattern(p.pattern_type));
          filter.set_filter_type(to_proto_filter_type(p.filter));
          filter.set_name(ss::sstring{p.pattern});

          return filter;
      });

    return filters;
}

acl_resource to_acl_resource(cluster_link::model::acl_resource r) {
    switch (r) {
    case cluster_link::model::acl_resource::any:
        return acl_resource::any;
    case cluster_link::model::acl_resource::cluster:
        return acl_resource::cluster;
    case cluster_link::model::acl_resource::group:
        return acl_resource::group;
    case cluster_link::model::acl_resource::topic:
        return acl_resource::topic;
    case cluster_link::model::acl_resource::txn_id:
        return acl_resource::txn_id;
    case cluster_link::model::acl_resource::schema_registry_subject:
        return acl_resource::sr_subject;
    case cluster_link::model::acl_resource::schema_registry_global:
        return acl_resource::sr_registry;
    case cluster_link::model::acl_resource::schema_registry_any:
        return acl_resource::sr_any;
    }
}

acl_pattern to_acl_pattern(cluster_link::model::acl_pattern p) {
    switch (p) {
    case cluster_link::model::acl_pattern::any:
        return acl_pattern::any;
    case cluster_link::model::acl_pattern::literal:
        return acl_pattern::literal;
    case cluster_link::model::acl_pattern::prefixed:
        return acl_pattern::prefixed;
    case cluster_link::model::acl_pattern::match:
        return acl_pattern::match;
    }
}

acl_resource_filter to_acl_resource_filter(
  const cluster_link::model::acl_resource_filter& resource_filter) {
    acl_resource_filter filter;

    filter.set_resource_type(to_acl_resource(resource_filter.resource_type));
    filter.set_pattern_type(to_acl_pattern(resource_filter.pattern_type));
    if (!resource_filter.name.empty()) {
        filter.set_name(ss::sstring{resource_filter.name});
    }

    return filter;
}

acl_operation to_acl_operation(cluster_link::model::acl_operation o) {
    switch (o) {
    case cluster_link::model::acl_operation::any:
        return acl_operation::any;
    case cluster_link::model::acl_operation::all:
        throw std::invalid_argument("No conversion to acl_operation::all");
    case cluster_link::model::acl_operation::read:
        return acl_operation::read;
    case cluster_link::model::acl_operation::write:
        return acl_operation::write;
    case cluster_link::model::acl_operation::create:
        return acl_operation::create;
    case cluster_link::model::acl_operation::remove:
        return acl_operation::remove;
    case cluster_link::model::acl_operation::alter:
        return acl_operation::alter;
    case cluster_link::model::acl_operation::describe:
        return acl_operation::describe;
    case cluster_link::model::acl_operation::cluster_action:
        return acl_operation::cluster_action;
    case cluster_link::model::acl_operation::describe_configs:
        return acl_operation::describe_configs;
    case cluster_link::model::acl_operation::alter_configs:
        return acl_operation::alter_configs;
    case cluster_link::model::acl_operation::idempotent_write:
        return acl_operation::idempotent_write;
    }
}

acl_permission_type
to_acl_permission_type(cluster_link::model::acl_permission_type t) {
    switch (t) {
    case cluster_link::model::acl_permission_type::any:
        return acl_permission_type::any;
    case cluster_link::model::acl_permission_type::allow:
        return acl_permission_type::allow;
    case cluster_link::model::acl_permission_type::deny:
        return acl_permission_type::deny;
    }
}

acl_access_filter to_acl_access_filter(
  const cluster_link::model::acl_access_filter& access_filter) {
    acl_access_filter filter;

    if (!access_filter.host.empty()) {
        filter.set_host(ss::sstring{access_filter.host});
    }

    filter.set_operation(to_acl_operation(access_filter.operation));
    filter.set_permission_type(
      to_acl_permission_type(access_filter.permission_type));

    if (!access_filter.principal.empty()) {
        filter.set_principal(ss::sstring{access_filter.principal});
    }

    return filter;
}

acl_filter to_acl_filter(const cluster_link::model::acl_filter& filter) {
    acl_filter acl;

    acl.set_resource_filter(to_acl_resource_filter(filter.resource_filter));
    acl.set_access_filter(to_acl_access_filter(filter.access_filter));

    return acl;
}

chunked_vector<acl_filter>
to_acl_filters(const chunked_vector<cluster_link::model::acl_filter>& filters) {
    chunked_vector<acl_filter> acl_filters;
    acl_filters.reserve(filters.size());

    std::ranges::transform(
      filters,
      std::back_inserter(acl_filters),
      [](const cluster_link::model::acl_filter& f) {
          return to_acl_filter(f);
      });

    return acl_filters;
}

security_settings_sync_options create_security_settings_sync_options(
  const cluster_link::model::security_settings_sync_config& config) {
    security_settings_sync_options options;

    options.set_interval(
      absl::FromChrono(
        config.task_interval.value_or(ss::lowres_clock::duration::zero())));
    options.set_effective_interval(
      absl::FromChrono(config.get_task_interval()));
    options.set_acl_filters(to_acl_filters(config.acl_filters));
    options.set_paused(!bool(config.is_enabled));

    return options;
}

void starting_offset_to_proto(
  std::optional<model::timestamp> ts, topic_metadata_sync_options& options) {
    if (!ts.has_value()) {
        return;
    }

    if (*ts == cluster_link::model::earliest_offset_ts) {
        options.set_start_at_earliest(
          topic_metadata_sync_options_earliest_offset{});
        return;
    }

    if (*ts == cluster_link::model::latest_offset_ts) {
        options.set_start_at_latest(
          topic_metadata_sync_options_latest_offset{});
        return;
    }

    options.set_start_at_timestamp(absl::FromUnixMillis(ts.value()()));
}

topic_metadata_sync_options create_topic_metadata_sync_options(
  const cluster_link::model::topic_metadata_mirroring_config& cfg) {
    topic_metadata_sync_options options;

    options.set_interval(
      absl::FromChrono(
        cfg.task_interval.value_or(ss::lowres_clock::duration::zero())));
    options.set_effective_interval(absl::FromChrono(cfg.get_task_interval()));
    options.set_auto_create_shadow_topic_filters(
      to_name_filters(cfg.topic_name_filters));

    chunked_vector<ss::sstring> mirrored_properties;
    mirrored_properties.reserve(cfg.topic_properties_to_mirror.size());

    for (const auto& prop : cfg.topic_properties_to_mirror) {
        mirrored_properties.push_back(ss::sstring{prop});
    }

    options.set_synced_shadow_topic_properties(std::move(mirrored_properties));
    options.set_exclude_default(cfg.exclude_default);

    starting_offset_to_proto(cfg.starting_offset, options);

    options.set_paused(!bool(cfg.is_enabled));

    return options;
}

consumer_offset_sync_options create_consumer_offset_sync_options(
  const cluster_link::model::consumer_groups_mirroring_config& cfg) {
    consumer_offset_sync_options options;
    options.set_interval(
      absl::FromChrono(
        cfg.task_interval.value_or(ss::lowres_clock::duration::zero())));
    options.set_effective_interval(absl::FromChrono(cfg.get_task_interval()));
    options.set_group_filters(to_name_filters(cfg.filters));
    options.set_paused(!bool(cfg.is_enabled));

    return options;
}

schema_registry_sync_options create_schema_registry_sync_options(
  const cluster_link::model::schema_registry_sync_config& cfg) {
    schema_registry_sync_options options;
    if (cfg.sync_schema_registry_topic_mode.has_value()) {
        ss::visit(
          *cfg.sync_schema_registry_topic_mode,
          [&options](
            const cluster_link::model::schema_registry_sync_config::
              shadow_entire_schema_registry&) {
              options.set_shadow_schema_registry_topic(
                schema_registry_sync_options_shadow_schema_registry_topic{});
          });
    }

    return options;
}

shadow_link_configurations
create_shadow_link_configuration(const cluster_link::model::metadata& md) {
    shadow_link_configurations configurations;

    configurations.set_client_options(create_shadow_link_client_options(md));
    configurations.set_topic_metadata_sync_options(
      create_topic_metadata_sync_options(
        md.configuration.topic_metadata_mirroring_cfg));
    configurations.set_consumer_offset_sync_options(
      create_consumer_offset_sync_options(
        md.configuration.consumer_groups_mirroring_cfg));
    configurations.set_security_sync_options(
      create_security_settings_sync_options(
        md.configuration.security_settings_sync_cfg));
    configurations.set_schema_registry_sync_options(
      create_schema_registry_sync_options(
        md.configuration.schema_registry_sync_cfg));

    return configurations;
}

chunked_vector<shadow_topic> create_shadow_topics(
  const cluster_link::model::link_state& state,
  const cluster_link::model::shadow_link_status_report& status_report) {
    chunked_vector<shadow_topic> shadow_topics;
    shadow_topics.reserve(state.mirror_topics.size());

    std::ranges::transform(
      state.mirror_topics,
      std::back_inserter(shadow_topics),
      [&status_report](const auto& p) {
          return model_to_shadow_topic(p.first, p.second, status_report);
      });

    std::ranges::sort(
      shadow_topics.begin(),
      shadow_topics.end(),
      [](const shadow_topic& a, const shadow_topic& b) {
          return a.get_name() < b.get_name();
      });

    return shadow_topics;
}

task_state convert_task_state(cluster_link::model::task_state s) {
    switch (s) {
    case cluster_link::model::task_state::active:
        return task_state::active;
    case cluster_link::model::task_state::paused:
        return task_state::paused;
    case cluster_link::model::task_state::link_unavailable:
        return task_state::link_unavailable;
    case cluster_link::model::task_state::stopped:
        return task_state::not_running;
    case cluster_link::model::task_state::faulted:
        return task_state::faulted;
    }
}

chunked_vector<shadow_link_task_status> create_task_status(
  const cluster_link::model::shadow_link_status_report& status_report) {
    chunked_vector<shadow_link_task_status> task_status;
    task_status.reserve(status_report.task_status_reports.size());

    for (const auto& [task_name, statuses] :
         status_report.task_status_reports) {
        std::ranges::transform(
          statuses, std::back_inserter(task_status), [](const auto& status) {
              shadow_link_task_status task_status;
              task_status.set_name(ss::sstring{status.task_name});
              task_status.set_state(convert_task_state(status.task_state));
              task_status.set_reason(ss::sstring{status.task_state_reason});
              task_status.set_broker_id(status.node_id);
              task_status.set_shard(status.shard);

              return task_status;
          });
    }

    std::ranges::sort(task_status, [](const auto& a, const auto& b) {
        if (a.get_name() != b.get_name()) {
            return a.get_name() < b.get_name();
        }
        if (a.get_broker_id() != b.get_broker_id()) {
            return a.get_broker_id() < b.get_broker_id();
        }
        return a.get_shard() < b.get_shard();
    });

    return task_status;
}

shadow_link_status create_shadow_link_status(
  const cluster_link::model::metadata& md,
  const cluster_link::model::shadow_link_status_report& status_report) {
    shadow_link_status status;

    status.set_state(convert_link_status(md.state.status));
    status.set_shadow_topics(create_shadow_topics(md.state, status_report));
    status.set_task_statuses(create_task_status(status_report));

    chunked_vector<ss::sstring> properties_synced;
    auto props = md.configuration.topic_metadata_mirroring_cfg
                   .get_topic_properties_to_mirror();
    properties_synced.reserve(props.size());
    std::ranges::copy(props, std::back_inserter(properties_synced));

    std::ranges::sort(properties_synced);
    status.set_synced_shadow_topic_properties(std::move(properties_synced));
    return status;
}

cluster_link::model::metadata shadow_link_to_metadata(shadow_link sl) {
    cluster_link::model::metadata md;
    md.name = cluster_link::model::name_t{std::move(sl.get_name())};
    md.uuid = cluster_link::model::uuid_t(uuid_t::create());
    md.connection = create_connection_config(sl);
    md.configuration = create_link_configuration(sl);

    set_client_id(md);
    return md;
}

// Merges input only fields from the current metadata model into the shadow link
// update
void merge_input_only_fields(
  const cluster_link::model::metadata& from, shadow_link& to) {
    auto& client_options = to.get_configurations().get_client_options();
    // Check to see if `to` has username set but not password.  If so, then the
    // password was not updated so we will use the current password in from
    if (
      client_options.has_authentication_configuration()
      && client_options.get_authentication_configuration()
           .has_scram_configuration()) {
        auto& to_scram = client_options.get_authentication_configuration()
                           .get_scram_configuration();
        if (
          to_scram.get_password().empty() && !to_scram.get_username().empty()) {
            if (
              from.connection.authn_config.has_value()
              && std::holds_alternative<cluster_link::model::scram_credentials>(
                from.connection.authn_config.value())) {
                const auto& from_scram
                  = std::get<cluster_link::model::scram_credentials>(
                    from.connection.authn_config.value());
                to_scram.set_password(ss::sstring{from_scram.password});
            }
        }
    }

    // Now check to see if the TLS settings are using values and if cert is set.
    // If so, then the key was not updated so we will use the curent key
    if (
      client_options.has_tls_settings()
      && client_options.get_tls_settings().has_tls_pem_settings()) {
        auto& to_pem = client_options.get_tls_settings().get_tls_pem_settings();
        if (to_pem.get_key().empty() && !to_pem.get_cert().empty()) {
            // Key is not set but cert is set, so we need to copy the current
            // key
            if (from.connection.key.has_value()) {
                ss::visit(
                  from.connection.key.value(),
                  [&to_pem](
                    const cluster_link::model::tls_value& value) mutable {
                      to_pem.set_key(iobuf::from(value()));
                  },
                  [](const auto&) {
                      throw std::invalid_argument(
                        "Inconsistent TLS type used in update");
                  });
            }
        }
    }
}

// Used to merge in output only fields that are not set during conversion of
// shadow link message to metadata model
void merge_output_only_fields(
  const cluster_link::model::metadata& from,
  cluster_link::model::metadata& to) {
    to.connection.client_id = from.connection.client_id;
}

// Used to update any "change on" timestamps, e.g. the "password_set_at" field
void update_timestamps(
  const cluster_link::model::metadata& from,
  cluster_link::model::metadata& to) {
    if (from.connection.authn_config != to.connection.authn_config) {
        if (!to.connection.authn_config.has_value()) {
            return;
        }
        ss::visit(
          *to.connection.authn_config,
          [&from](cluster_link::model::scram_credentials& c) {
              // If from does not hold SCRAM credentials, then update the
              // timestamp of when then password was set
              if (
                !from.connection.authn_config.has_value()
                || !std::holds_alternative<
                   cluster_link::model::scram_credentials>(
                  *from.connection.authn_config)) {
                  if (c.password.empty()) {
                      return;
                  }
                  c.password_last_updated = model::timestamp::now();
                  return;
              }
              const auto& from_creds
                = std::get<cluster_link::model::scram_credentials>(
                  *from.connection.authn_config);
              // If the passwords do not match, then update the timestamp of
              // when the password was set
              c.password_last_updated = from_creds.password_last_updated;
              if (from_creds.password != c.password) {
                  c.password_last_updated = model::timestamp::now();
                  return;
              }
          });
    }
}

// Used to update the timestamps for metadata fields
void update_timestamps(cluster_link::model::metadata& to) {
    if (!to.connection.authn_config.has_value()) {
        return;
    }
    ss::visit(
      *to.connection.authn_config,
      [](cluster_link::model::scram_credentials& c) {
          if (c.password.empty()) {
              return;
          }
          c.password_last_updated = model::timestamp::now();
      });
}

chunked_vector<topic_partition_information> status_to_partition_information(
  const cluster_link::rpc::shadow_link_status_topic_response& response) {
    chunked_vector<topic_partition_information> resp;
    resp.reserve(response.partition_reports.size());
    for (const auto& [part_id, report] : response.partition_reports) {
        topic_partition_information info;
        info.set_partition_id(part_id);
        info.set_source_high_watermark(report.source_partition_high_watermark);
        info.set_source_last_stable_offset(
          report.source_partition_last_stable_offset);
        info.set_source_last_updated_timestamp(
          absl::FromUnixMillis(report.last_update_time.count()));
        info.set_high_watermark(report.shadow_partition_high_watermark);
        resp.emplace_back(std::move(info));
    }
    std::ranges::sort(
      resp,
      std::ranges::less{},
      &topic_partition_information::get_partition_id);
    return resp;
}
} // namespace

void set_client_id(cluster_link::model::metadata& md) {
    md.connection.client_id = ssx::sformat(
      "shadow-link-{}-{}", md.name, md.uuid);
}

cluster_link::model::metadata
convert_create_to_metadata(create_shadow_link_request req) {
    try {
        auto metadata = shadow_link_to_metadata(
          std::move(req.get_shadow_link()));
        metadata.state.status = cluster_link::model::link_status::active;
        update_timestamps(metadata);
        return metadata;
    } catch (const std::invalid_argument& e) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat("Invalid cluster link configuration: {}", e.what()));
    }
}

shadow_link metadata_to_shadow_link(
  cluster_link::model::metadata_ptr md,
  cluster_link::model::shadow_link_status_report status_report) {
    shadow_link sl;

    sl.set_name(ss::sstring{md->name()});
    sl.set_uid(ssx::sformat("{}", md->uuid));
    sl.set_configurations(create_shadow_link_configuration(*md));
    sl.set_status(create_shadow_link_status(*md, status_report));

    return sl;
}

cluster_link::model::update_cluster_link_configuration_cmd
create_update_cluster_link_config_cmd(
  update_shadow_link_request req,
  cluster_link::model::metadata_ptr current_metadata) {
    if (!req.get_update_mask().is_valid_for_message<shadow_link>()) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat(
            "Invalid update mask for shadow_link: {}", req.get_update_mask()));
    }
    auto current_md_copy = ss::make_lw_shared<cluster_link::model::metadata>({
      .name = current_metadata->name,
      .uuid = current_metadata->uuid,
      .connection = current_metadata->connection,
      .configuration = current_metadata->configuration.copy(),
    });
    // Save off client ID to reuse later
    // Client ID is an output only field so when the shadow link value is
    // converted back to metadata, the client ID is not set
    auto current_sl = metadata_to_shadow_link(std::move(current_md_copy), {});
    req.get_update_mask().merge_into(
      std::move(req.get_shadow_link()), &current_sl);
    merge_input_only_fields(*current_metadata, current_sl);
    try {
        auto updated_md = shadow_link_to_metadata(std::move(current_sl));

        merge_output_only_fields(*current_metadata, updated_md);
        update_timestamps(*current_metadata, updated_md);

        return cluster_link::model::update_cluster_link_configuration_cmd{
          .connection = std::move(updated_md.connection),
          .link_config = std::move(updated_md.configuration)};
    } catch (const std::invalid_argument& e) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat(
            "Invalid shadow link update configuration: {}", e.what()));
    }
}

shadow_topic model_to_shadow_topic(
  ::model::topic_view topic,
  const cluster_link::model::mirror_topic_metadata& metadata,
  const cluster_link::model::shadow_link_status_report& status_report) {
    shadow_topic st;
    st.set_name(ss::sstring{topic});
    if (metadata.destination_topic_id != model::topic_id{}) {
        st.set_topic_id(ssx::sformat("{}", metadata.destination_topic_id));
    }
    st.set_source_topic_name(ss::sstring{metadata.source_topic_name});
    if (metadata.source_topic_id.has_value()) {
        st.set_source_topic_id(
          ssx::sformat("{}", metadata.source_topic_id.value()));
    }
    shadow_topic_status status;
    status.set_state(mirror_topic_state_to_shadow_topic_state(metadata.status));
    auto it = status_report.topic_responses.find(topic);
    if (it != status_report.topic_responses.end()) {
        status.set_partition_information(
          status_to_partition_information(it->second));
    }
    st.set_status(std::move(status));

    return st;
}
} // namespace admin
