// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "configuration.h"

#include "base/units.h"
#include "config/configuration.h"
#include "kafka/client/logger.h"
#include "security/oidc_authenticator.h"
#include "security/scram_authenticator.h"

namespace kafka::client {
using namespace std::chrono_literals;

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    read_yaml(cfg);
}

configuration::configuration()
  : brokers(
      *this,
      "brokers",
      "Network addresses of the Kafka API servers to which the HTTP Proxy "
      "client should connect.",
      {},
      std::vector<net::unresolved_address>({{"127.0.0.1", 9092}}))
  , broker_tls(
      *this,
      "broker_tls",
      "TLS configuration for the Kafka API servers to which the HTTP Proxy "
      "client should connect.",
      {},
      config::tls_config(),
      config::tls_config::validate)
  , retries(
      *this,
      "retries",
      "Number of times to retry a request to a broker.",
      {},
      5)
  , retry_base_backoff(
      *this,
      "retry_base_backoff_ms",
      "Delay (in milliseconds) for initial retry backoff.",
      {},
      100ms)
  , produce_batch_record_count(
      *this,
      "produce_batch_record_count",
      "Number of records to batch before sending to broker.",
      {},
      1000)
  , produce_batch_size_bytes(
      *this,
      "produce_batch_size_bytes",
      "Number of bytes to batch before sending to broker.",
      {},
      1048576)
  , produce_batch_delay(
      *this,
      "produce_batch_delay_ms",
      "Delay (in milliseconds) to wait before sending batch.",
      {},
      100ms)
  , produce_compression_type(
      *this,
      "produce_compression_type",
      "Enable or disable compression by the Kafka client. Specify `none` to "
      "disable compression or one of the supported types [gzip, snappy, lz4, "
      "zstd].",
      {},
      "none",
      [](const ss::sstring& v) -> std::optional<ss::sstring> {
          constexpr auto supported_types = std::to_array<std::string_view>(
            {"none", "gzip", "snappy", "lz4", "zstd"});
          const auto found = std::find(
            supported_types.cbegin(), supported_types.cend(), v);
          if (found == supported_types.end()) {
              return ss::format(
                "{} is not a supported client compression type", v);
          }
          return std::nullopt;
      })
  , produce_shutdown_delay(
      *this,
      "produce_shutdown_delay_ms",
      "Delay (in milliseconds) to allow for final flush of buffers before "
      "shutting down.",
      {},
      0ms)
  , produce_ack_level(
      *this,
      "produce_ack_level",
      "Number of acknowledgments the producer requires the leader to have "
      "received before considering a request complete.",
      {},
      -1,
      [](int16_t acks) -> std::optional<ss::sstring> {
          if (acks < -1 || acks > 1) {
              return ss::format("Validation failed for acks: {}", acks);
          }
          return std::nullopt;
      })
  , consumer_request_timeout(
      *this,
      "consumer_request_timeout_ms",
      "Interval (in milliseconds) for consumer request timeout.",
      {},
      100ms)
  , consumer_request_min_bytes(
      *this,
      "consumer_request_min_bytes",
      "Minimum bytes to fetch per request.",
      {},
      1,
      {.min = 0})
  , consumer_request_max_bytes(
      *this,
      "consumer_request_max_bytes",
      "Maximum bytes to fetch per request.",
      {},
      1_MiB,
      {.min = 0})
  , consumer_session_timeout(
      *this,
      "consumer_session_timeout_ms",
      "Timeout (in milliseconds) for consumer session.",
      {},
      10s)
  , consumer_rebalance_timeout(
      *this,
      "consumer_rebalance_timeout_ms",
      "Timeout (in milliseconds) for consumer rebalance.",
      {},
      2s)
  , consumer_heartbeat_interval(
      *this,
      "consumer_heartbeat_interval_ms",
      "Interval (in milliseconds) for consumer heartbeats.",
      {},
      500ms)
  , sasl_mechanism(
      *this,
      "sasl_mechanism",
      "The SASL mechanism to use when the HTTP Proxy client connects to the "
      "Kafka API. These credentials are used when the HTTP Proxy API "
      "listener has `authentication_method: none` but the cluster requires "
      "authenticated access to the Kafka API. Starting in Redpanda 25.2, "
      "ephemeral credentials for HTTP Proxy are removed. If your HTTP Proxy "
      "listeners use `authentication_method: none`, you must configure these "
      "SASL properties for HTTP Proxy to authenticate with the Kafka API. "
      "For more information, see "
      "https://docs.redpanda.com/current/manage/security/authentication/",
      {},
      "")
  , scram_username(
      *this,
      "scram_username",
      "Username to use for SCRAM authentication mechanisms when the HTTP "
      "Proxy client connects to the Kafka API. This property is required "
      "when the HTTP Proxy API listener has `authentication_method: none` "
      "but the cluster requires authenticated access to the Kafka API. "
      "Starting in Redpanda 25.2, ephemeral credentials for HTTP Proxy are "
      "removed. You must configure this property if your HTTP Proxy "
      "listeners use `authentication_method: none`.",
      {},
      "")
  , scram_password(
      *this,
      "scram_password",
      "Password to use for SCRAM authentication mechanisms when the HTTP "
      "Proxy client connects to the Kafka API. This property is required "
      "when the HTTP Proxy API listener has `authentication_method: none` "
      "but the cluster requires authenticated access to the Kafka API. "
      "Starting in Redpanda 25.2, ephemeral credentials for HTTP Proxy are "
      "removed. You must configure this property if your HTTP Proxy "
      "listeners use `authentication_method: none`.",
      {.secret = config::is_secret::yes},
      "")
  , client_identifier(
      *this,
      "client_identifier",
      "Custom identifier to include in the Kafka request header for the HTTP "
      "Proxy client. This identifier can help debug or monitor client "
      "activities.",
      {},
      "test_client") {}

namespace {
model::compression compression_from_str(std::string_view v) {
    if (v == "none") {
        return model::compression::none;
    } else if (v == "gzip") {
        return model::compression::gzip;
    } else if (v == "snappy") {
        return model::compression::snappy;
    } else if (v == "lz4") {
        return model::compression::lz4;
    } else if (v == "zstd") {
        return model::compression::zstd;
    }
    throw std::invalid_argument(
      ss::format("Unsupported compression type: {}", v));
}

void validate_sasl_properties(
  std::string_view mechanism,
  std::string_view username,
  std::string_view password) {
    if (
      mechanism != security::scram_sha256_authenticator::name
      && mechanism != security::scram_sha512_authenticator::name
      && mechanism != security::oidc::sasl_authenticator::name) [[unlikely]] {
        throw std::invalid_argument(
          ss::format(
            "Unknown SASL mechanism: {}, currently Redpanda client only "
            "supports "
            "SCRAM-256, SCRAM-512 and OAUTHBEARER",
            mechanism));
    }

    if (username.empty()) [[unlikely]] {
        throw std::invalid_argument("Scram username can not be empty");
    }

    if (password.empty()) [[unlikely]] {
        throw std::invalid_argument("Scram password can not be empty");
    }
}

} // namespace

producer_configuration
producer_configuration::from_config_store(const configuration& cfg) {
    auto compression_type = compression_from_str(
      cfg.produce_compression_type.value());
    return producer_configuration{
      .batch_record_count = cfg.produce_batch_record_count,
      .batch_size_bytes = cfg.produce_batch_size_bytes,
      .batch_delay = cfg.produce_batch_delay,
      .compression_type = compression_type,
      .shutdown_delay = cfg.produce_shutdown_delay.value(),
      .ack_level = acks(cfg.produce_ack_level.value()),
    };
}

consumer_configuration
consumer_configuration::from_config_store(const configuration& cfg) {
    return consumer_configuration{
      .request_timeout = cfg.consumer_request_timeout.value(),
      .fetch_min_bytes = cfg.consumer_request_min_bytes.value(),
      .fetch_max_bytes = cfg.consumer_request_max_bytes.value(),
      .session_timeout = cfg.consumer_session_timeout.value(),
      .rebalance_timeout = cfg.consumer_rebalance_timeout.value(),
      .heartbeat_interval = cfg.consumer_heartbeat_interval.value(),
    };
}

connection_configuration
connection_configuration::from_config_store(const configuration& cfg) {
    connection_configuration ret{
      .initial_brokers = cfg.brokers.value(),
      .client_id = cfg.client_identifier.value(),
    };

    if (!cfg.sasl_mechanism().empty()) {
        validate_sasl_properties(
          cfg.sasl_mechanism(), cfg.scram_username(), cfg.scram_password());
        ret.sasl_cfg = sasl_configuration{
          .mechanism = cfg.sasl_mechanism(),
          .username = cfg.scram_username(),
          .password = cfg.scram_password(),
        };
    }
    ret.broker_tls = tls_configuration::from_tls_config(cfg.broker_tls.value());
    ret.include_authorized_operations
      = connection_configuration::include_authorized_ops_t::no;

    return ret;
}

fmt::iterator connection_configuration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{initial_brokers: {}, broker_tls: {}, sasl_cfg: {}, client_id: {}, "
      "max_metadata_age: {}, connection_timeout: {}}}",
      initial_brokers,
      broker_tls,
      sasl_cfg,
      client_id,
      max_metadata_age,
      connection_timeout);
}

client_configuration
client_configuration::from_config_store(const configuration& cfg) {
    return client_configuration{
      .connection_cfg = connection_configuration::from_config_store(cfg),
      .producer_cfg = producer_configuration::from_config_store(cfg),
      .consumer_cfg = consumer_configuration::from_config_store(cfg),
      .retries_cfg = retries_configuration{
          .max_retries = cfg.retries.value(),
          .retry_base_backoff = cfg.retry_base_backoff.value(),
      },
    };
}
namespace {
net::key_store create_key_store(const config::key_cert_container& container) {
    return ss::visit(
      container,
      [](const config::key_cert& kc) {
          return net::key_store{net::key_cert_path{
            .key = std::filesystem::path(kc.key_file),
            .cert = std::filesystem::path(kc.cert_file),
          }};
      },
      [](const config::p12_container& pkcs) {
          return net::key_store{net::pkcs12{
            .cert = std::filesystem::path(pkcs.p12_path),
            .password = pkcs.p12_password,
          }};
      });
}
} // namespace

std::optional<tls_configuration>
tls_configuration::from_tls_config(const config::tls_config& cfg) {
    if (!cfg.is_enabled()) {
        return std::nullopt;
    }

    tls_configuration ret;
    auto& cert_files = cfg.get_key_cert_files();

    if (cert_files.has_value()) {
        ret.k_store = create_key_store(cert_files.value());
    }
    if (cfg.get_truststore_file().has_value()) {
        ret.truststore = std::filesystem::path(
          cfg.get_truststore_file().value());
    }

    return ret;
}

ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
tls_configuration::build_credentials() const {
    auto builder = co_await net::get_credentials_builder({
      .truststore = truststore,
      .k_store = k_store,
      .crl = std::nullopt,
      .min_tls_version = from_config(
        config::shard_local_cfg().tls_min_version()),
      .enable_renegotiation
      = config::shard_local_cfg().tls_enable_renegotiation(),
      .require_client_auth = false,
    });

    co_return co_await builder.build_reloadable_certificate_credentials(
      [](
        const std::unordered_set<ss::sstring>& updated,
        const std::exception_ptr& eptr) {
          if (eptr) {
              try {
                  std::rethrow_exception(eptr);
              } catch (...) {
                  vlog(
                    kclog.error,
                    "kafka-client credentials reload error {}",
                    std::current_exception());
              }
          } else {
              for (const auto& name : updated) {
                  vlog(
                    kclog.info,
                    "kafka-client key or certificate file updated - {}",
                    name);
              }
          }
      });
}

fmt::iterator tls_configuration::format_to(fmt::iterator it) const {
    fmt::format_to(it, "{{truststore: ");
    if (truststore) {
        net::format_cert(it, *truststore);
    } else {
        fmt::format_to(it, "null");
    }
    fmt::format_to(it, ", k_store: ");
    if (k_store) {
        net::format_keystore(it, *k_store);
    } else {
        fmt::format_to(it, "null");
    }
    return fmt::format_to(
      it, ", provide_sni_hostname: {}}}", provide_sni_hostname);
}

fmt::iterator sasl_configuration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{mechanism: {}, username: {}, password: <redacted>}}",
      mechanism,
      username);
}
} // namespace kafka::client
