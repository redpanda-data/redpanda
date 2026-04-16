/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/auth_refresh_bg_op.h"
#include "cloud_storage_clients/client_probe.h"
#include "cloud_storage_clients/types.h"
#include "model/metadata.h"
#include "net/types.h"

namespace cloud_storage_clients {

/// List of default overrides that can be used to workaround issues
/// that can arise when we want to deal with different S3 API implementations
/// and different OS issues (like different truststore locations on different
/// Linux distributions).
struct default_overrides {
    std::optional<endpoint_url> endpoint = std::nullopt;
    std::optional<uint16_t> port = std::nullopt;
    std::optional<ca_trust_file> trust_file = std::nullopt;
    std::optional<ss::lowres_clock::duration> max_idle_time = std::nullopt;
    bool disable_tls = false;
};

/// Configuration options common across cloud storage clients
/// Primitive, copyable across shards.
struct common_configuration {
    /// URI of the access point
    access_point_uri uri;
    /// Max time that connection can spend idle
    ss::lowres_clock::duration max_idle_time;

    bool requires_self_configuration{false};

    model::cloud_credentials_source cloud_credentials_source{
      model::cloud_credentials_source::config_file};

    net::metrics_disabled disable_metrics{net::metrics_disabled::no};
    net::public_metrics_disabled disable_public_metrics{
      net::public_metrics_disabled::no};

    /// \defgroup Fields for constructing net::base_transport::configuration
    /// @{
    net::unresolved_address server_addr;

    bool disable_tls = false;
    std::optional<ca_trust_file> tls_truststore_path;

    /// Optional server name indication (SNI) for TLS connection
    std::optional<ss::sstring> tls_sni_hostname;

    /// Potentially skip wait for EOF after BYE message on TLS session end
    bool wait_for_tls_server_eof = true;
    /// @}
};

struct s3_configuration : common_configuration {
    /// AWS region
    cloud_roles::aws_region_name region;
    /// AWS service (e.g., "s3")
    cloud_roles::aws_service_name service;
    /// AWS access key, optional if configuration uses temporary credentials
    std::optional<cloud_roles::public_key_str> access_key;
    /// AWS secret key, optional if configuration uses temporary credentials
    std::optional<cloud_roles::private_key_str> secret_key;
    /// AWS URL style, either virtual-hosted-style or path-style. Nullopt means
    /// that the style needs to be determined with self-configuration.
    std::optional<s3_url_style> url_style = std::nullopt;
    /// Whether the s3-compatible backend is GCS. Used in the client pool to
    /// select between s3_client and gcs_client at client creation time.
    bool is_gcs{false};

    /// \brief opinionated configuration initialization
    /// Generates uri field from region, initializes credentials for the
    /// transport, resolves the uri to get the server_addr.
    ///
    /// \param pkey is an AWS access key
    /// \param skey is an AWS secret key
    /// \param region is an AWS region code
    /// \param bucket is an AWS bucket name. it's needed to form the endpoints
    /// in fips mode
    /// \param overrides contains a bunch of property overrides like
    ///        non-standard SSL port and alternative location of the
    ///        truststore
    /// \return future that returns initialized configuration
    static ss::future<s3_configuration> make_configuration(
      model::cloud_credentials_source cloud_credentials_source,
      const std::optional<cloud_roles::public_key_str>& pkey,
      const std::optional<cloud_roles::private_key_str>& skey,
      const cloud_roles::aws_region_name& region,
      const bucket_name& bucket,
      std::optional<cloud_storage_clients::s3_url_style> url_style,
      bool node_is_in_fips_mode,
      const default_overrides& overrides = {},
      net::metrics_disabled disable_metrics = net::metrics_disabled::yes,
      net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::yes);

    ss::shared_ptr<client_probe> make_probe() const;

    friend std::ostream& operator<<(std::ostream& o, const s3_configuration& c);
};

struct abs_configuration : common_configuration {
    cloud_roles::storage_account storage_account_name;
    std::optional<cloud_roles::private_key_str> shared_key;
    bool is_hns_enabled{false};

    static ss::future<abs_configuration> make_configuration(
      model::cloud_credentials_source cloud_credentials_source,
      const std::optional<cloud_roles::private_key_str>& shared_key,
      const cloud_roles::storage_account& storage_account_name,
      const default_overrides& overrides = {},
      net::metrics_disabled disable_metrics = net::metrics_disabled::yes,
      net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::yes);

    ss::shared_ptr<client_probe> make_probe() const;

    friend std::ostream&
    operator<<(std::ostream& o, const abs_configuration& c);
};

template<typename T>
concept storage_client_configuration
  = std::is_base_of_v<common_configuration, T>;

template<storage_client_configuration... Ts>
using client_configuration_variant = std::variant<Ts...>;

using client_configuration
  = client_configuration_variant<abs_configuration, s3_configuration>;

std::ostream& operator<<(std::ostream&, const client_configuration&);

struct abs_self_configuration_result {
    bool is_hns_enabled;
};

struct s3_self_configuration_result {
    s3_url_style url_style;
};

using client_self_configuration_output
  = std::variant<abs_self_configuration_result, s3_self_configuration_result>;

void apply_self_configuration_result(
  client_configuration&, const client_self_configuration_output&);

std::ostream& operator<<(std::ostream&, const abs_self_configuration_result&);
std::ostream& operator<<(std::ostream&, const s3_self_configuration_result&);
std::ostream&
operator<<(std::ostream&, const client_self_configuration_output&);

// In the case of S3-compatible providers, all that is needed to infer the
// backend is the access point/uri.
model::cloud_storage_backend
infer_backend_from_uri(const access_point_uri& uri);

model::cloud_storage_backend infer_backend_from_configuration(
  const client_configuration& client_config,
  model::cloud_credentials_source cloud_storage_credentials_source);

cloud_roles::auth_refresh_bg_op::credentials_source_config
build_refresh_credentials_source(
  const client_configuration& config,
  model::cloud_credentials_source cloud_credentials_source);

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_tls_credentials(const client_configuration& config);

net::base_transport::configuration build_transport_configuration(
  const client_configuration&,
  ss::shared_ptr<ss::tls::certificate_credentials>);

} // namespace cloud_storage_clients
