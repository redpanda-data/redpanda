// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "configuration.h"

#include "config/configuration.h"
#include "units.h"

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
    "List of address and port of the brokers",
    {.required = config::required::yes},
    std::vector<net::unresolved_address>({{"127.0.0.1", 9092}}))
  , broker_tls(
      *this,
      "broker_tls",
      "TLS configuration for the brokers",
      {},
      config::tls_config(),
      config::tls_config::validate)
  , retries(
      *this, "retries", "Number of times to retry a request to a broker", {}, 5)
  , retry_base_backoff(
      *this,
      "retry_base_backoff_ms",
      "Delay (in milliseconds) for initial retry backoff",
      {},
      100ms)
  , produce_batch_record_count(
      *this,
      "produce_batch_record_count",
      "Number of records to batch before sending to broker",
      {},
      1000)
  , produce_batch_size_bytes(
      *this,
      "produce_batch_size_bytes",
      "Number of bytes to batch before sending to broker",
      {},
      1048576)
  , produce_batch_delay(
      *this,
      "produce_batch_delay_ms",
      "Delay (in milliseconds) to wait before sending batch",
      {},
      100ms)
  , consumer_request_timeout(
      *this,
      "consumer_request_timeout_ms",
      "Interval (in milliseconds) for consumer request timeout",
      {},
      100ms)
  , consumer_request_max_bytes(
      *this,
      "consumer_request_max_bytes",
      "Max bytes to fetch per request",
      {},
      1_MiB)
  , consumer_session_timeout(
      *this,
      "consumer_session_timeout_ms",
      "Timeout (in milliseconds) for consumer session",
      {},
      10s)
  , consumer_rebalance_timeout(
      *this,
      "consumer_rebalance_timeout_ms",
      "Timeout (in milliseconds) for consumer rebalance",
      {},
      2s)
  , consumer_heartbeat_interval(
      *this,
      "consumer_heartbeat_interval_ms",
      "Interval (in milliseconds) for consumer heartbeats",
      {},
      500ms)
  , sasl_mechanism(
      *this,
      "sasl_mechanism",
      "The SASL mechanism to use when connecting",
      {},
      "")
  , scram_username(
      *this,
      "scram_username",
      "Username to use for SCRAM authentication mechanisms",
      {},
      "")
  , scram_password(
      *this,
      "scram_password",
      "Password to use for SCRAM authentication mechanisms",
      {.secret = config::is_secret::yes},
      "")
  , client_identifier(
      *this,
      "client_identifier",
      "Identifier to use within the kafka request header",
      {},
      "test_client") {}

} // namespace kafka::client
