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
      {.required = config::required::yes},
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
      "The SASL mechanism to use when connecting.",
      {},
      "")
  , scram_username(
      *this,
      "scram_username",
      "Username to use for SCRAM authentication mechanisms.",
      {},
      "")
  , scram_password(
      *this,
      "scram_password",
      "Password to use for SCRAM authentication mechanisms.",
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

} // namespace kafka::client
