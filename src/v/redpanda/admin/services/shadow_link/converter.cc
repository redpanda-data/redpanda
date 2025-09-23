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

#include "cluster_link/model/types.h"

#include <stdexcept>

using namespace std::chrono_literals;

namespace admin {
using proto::admin::authentication_configuration;
using proto::admin::create_shadow_link_request;
using proto::admin::name_filter;
using proto::admin::scram_config;
using proto::admin::scram_mechanism;
using proto::admin::shadow_link;
using proto::admin::shadow_link_client_options;
using proto::admin::shadow_link_configurations;
using proto::admin::shadow_link_status;
using proto::admin::shadow_topic_status;
using proto::admin::tls_file_settings;
using proto::admin::tls_settings;
using proto::admin::tlspem_settings;
using proto::admin::topic_metadata_sync_options;
using proto::admin::update_shadow_link_request;
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

constexpr auto mirror_topic_state_to_shadow_topic_state(
  cluster_link::model::mirror_topic_state s) {
    switch (s) {
    case cluster_link::model::mirror_topic_state::active:
        return proto::admin::shadow_topic_state::active;
    case cluster_link::model::mirror_topic_state::failed:
        return proto::admin::shadow_topic_state::faulted;
    case cluster_link::model::mirror_topic_state::paused:
        return proto::admin::shadow_topic_state::paused;
    case cluster_link::model::mirror_topic_state::promoted:
        return proto::admin::shadow_topic_state::promoted;
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
      options.get_shadowed_topic_properties(),
      std::inserter(
        config.topic_properties_to_mirror,
        config.topic_properties_to_mirror.end()));

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

    return config;
}
cluster_link::model::link_configuration
create_link_configuration(const shadow_link& sl) {
    cluster_link::model::link_configuration config;
    config.topic_metadata_mirroring_cfg
      = create_topic_metadata_mirroring_config(
        sl.get_configurations().get_topic_metadata_sync_options());

    config.consumer_groups_mirroring_cfg
      = create_consumer_groups_mirroring_config(
        sl.get_configurations().get_consumer_offset_sync_options());

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

/// \brief Creates the authentication settings from the create cluster link
/// \brief throws std::invalid_argument if invalid config provided
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

    return config;
}

authentication_configuration create_authentication_configuration(
  const cluster_link::model::connection_config::authn_variant& authn) {
    return ss::visit(
      authn,
      [](const cluster_link::model::scram_credentials& scram)
        -> authentication_configuration {
          scram_config scram_proto;
          scram_proto.set_username(ss::sstring{scram.username});
          scram_proto.set_password_set(true);
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
                ssx::sformat("Unknown SCRAM mechanism: {}", scram.mechanism));
          }
          authentication_configuration authn;
          authn.set_scram_configuration(std::move(scram_proto));
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
                "Cannot set both tls_file_settings and tls_pem_settings");
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
                "Cannot set both tls_file_settings and tls_pem_settings");
          },
          [&key, &cert](tlspem_settings& pem_settings) {
              pem_settings.set_key(ss::sstring{key()});
              pem_settings.set_cert(ss::sstring{cert()});
          },
          [this, &key, &cert](std::monostate) {
              tlspem_settings pem_settings;
              pem_settings.set_key(ss::sstring{key()});
              pem_settings.set_cert(ss::sstring{cert()});
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
              pem_settings.set_ca(ss::sstring{value});
              tls.set_tls_pem_settings(std::move(pem_settings));
          });
    }

    if (md.connection.key.has_value() && md.connection.cert.has_value()) {
        std::visit(
          tls_visitor(&tls),
          md.connection.key.value(),
          md.connection.cert.value());
    }

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
      md.connection.ca.has_value() || md.connection.cert.has_value()
      || md.connection.key.has_value()) {
        options.set_tls_settings(create_tls_settings(md));
    }

    if (md.connection.authn_config.has_value()) {
        options.set_authentication_configuration(
          create_authentication_configuration(
            md.connection.authn_config.value()));
    }

    options.set_metadata_max_age_ms(
      md.connection.metadata_max_age_ms.value_or(0));
    options.set_connection_timeout_ms(
      md.connection.connection_timeout_ms.value_or(0));
    options.set_retry_backoff_ms(md.connection.retry_backoff_ms.value_or(0));
    options.set_fetch_wait_max_ms(md.connection.fetch_wait_max_ms.value_or(0));
    options.set_fetch_min_bytes(md.connection.fetch_min_bytes.value_or(0));
    options.set_fetch_max_bytes(md.connection.fetch_max_bytes.value_or(0));

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

topic_metadata_sync_options create_topic_metadata_sync_options(
  const cluster_link::model::topic_metadata_mirroring_config& cfg) {
    topic_metadata_sync_options options;

    options.set_interval(
      absl::FromChrono(
        cfg.task_interval.value_or(ss::lowres_clock::duration::zero())));
    options.set_auto_create_shadow_topic_filters(
      to_name_filters(cfg.topic_name_filters));

    chunked_vector<ss::sstring> mirrored_properties;
    mirrored_properties.reserve(cfg.topic_properties_to_mirror.size());

    for (const auto& prop : cfg.topic_properties_to_mirror) {
        mirrored_properties.push_back(ss::sstring{prop});
    }

    options.set_shadowed_topic_properties(std::move(mirrored_properties));

    return options;
}

shadow_link_configurations
create_shadow_link_configuration(const cluster_link::model::metadata& md) {
    shadow_link_configurations configurations;

    configurations.set_client_options(create_shadow_link_client_options(md));
    configurations.set_topic_metadata_sync_options(
      create_topic_metadata_sync_options(
        md.configuration.topic_metadata_mirroring_cfg));

    return configurations;
}

chunked_vector<shadow_topic_status>
create_shadow_topic_statuses(const cluster_link::model::link_state& state) {
    chunked_vector<shadow_topic_status> statuses;
    statuses.reserve(state.mirror_topics.size());

    for (const auto& [topic, metadata] : state.mirror_topics) {
        shadow_topic_status status;
        status.set_name(ss::sstring{topic});
        status.set_state(
          mirror_topic_state_to_shadow_topic_state(metadata.state));
        statuses.emplace_back(std::move(status));
    }

    return statuses;
}

shadow_link_status
create_shadow_link_status(const cluster_link::model::metadata& md) {
    shadow_link_status status;

    status.set_state(proto::admin::shadow_link_state::active);
    status.set_shadow_topic_statuses(create_shadow_topic_statuses(md.state));

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
                      to_pem.set_key(ss::sstring{value()});
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
} // namespace

void set_client_id(cluster_link::model::metadata& md) {
    md.connection.client_id = ssx::sformat(
      "cluster-link-{}-{}", md.name, md.uuid);
}

cluster_link::model::metadata
convert_create_to_metadata(create_shadow_link_request req) {
    cluster_link::model::metadata metadata;

    try {
        return shadow_link_to_metadata(std::move(req.get_shadow_link()));
    } catch (const std::invalid_argument& e) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat("Invalid cluster link configuration: {}", e.what()));
    }
}

shadow_link metadata_to_shadow_link(cluster_link::model::metadata md) {
    shadow_link sl;

    sl.set_name(std::move(md.name));
    sl.set_uid(ssx::sformat("{}", md.uuid));
    sl.set_configurations(create_shadow_link_configuration(md));
    sl.set_status(create_shadow_link_status(md));

    return sl;
}

cluster_link::model::update_cluster_link_configuration_cmd
create_update_cluster_link_config_cmd(
  update_shadow_link_request req,
  cluster_link::model::metadata current_metadata) {
    if (!req.get_update_mask().is_valid_for_message<shadow_link>()) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat(
            "Invalid update mask for shadow_link: {}", req.get_update_mask()));
    }
    // Save off client ID to reuse later
    // Client ID is an output only field so when the shadow link value is
    // converted back to metadata, the client ID is not set
    auto current_sl = metadata_to_shadow_link(current_metadata.copy());
    req.get_update_mask().merge_into(
      std::move(req.get_shadow_link()), &current_sl);
    merge_input_only_fields(current_metadata, current_sl);
    try {
        auto updated_md = shadow_link_to_metadata(std::move(current_sl));

        merge_output_only_fields(current_metadata, updated_md);

        return cluster_link::model::update_cluster_link_configuration_cmd{
          .connection = std::move(updated_md.connection),
          .link_config = std::move(updated_md.configuration)};
    } catch (const std::invalid_argument& e) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat(
            "Invalid shadow link update configuration: {}", e.what()));
    }
}
} // namespace admin
