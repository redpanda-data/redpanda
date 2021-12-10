// Copyright 2020 Vectorized, Inc.
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

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

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
    config::required::yes,
    std::vector<net::unresolved_address>({{"127.0.0.1", 9092}}))
  , broker_tls(
      *this,
      "broker_tls",
      "TLS configuration for the brokers",
      config::required::no,
      config::tls_config(),
      config::tls_config::validate)
  , retries(
      *this,
      "retries",
      "Number of times to retry a request to a broker",
      config::required::no,
      5)
  , retry_base_backoff(
      *this,
      "retry_base_backoff_ms",
      "Delay (in milliseconds) for initial retry backoff",
      config::required::no,
      100ms)
  , produce_batch_record_count(
      *this,
      "produce_batch_record_count",
      "Number of records to batch before sending to broker",
      config::required::no,
      1000)
  , produce_batch_size_bytes(
      *this,
      "produce_batch_size_bytes",
      "Number of bytes to batch before sending to broker",
      config::required::no,
      1048576)
  , produce_batch_delay(
      *this,
      "produce_batch_delay_ms",
      "Delay (in milliseconds) to wait before sending batch",
      config::required::no,
      100ms)
  , consumer_request_timeout(
      *this,
      "consumer_request_timeout_ms",
      "Interval (in milliseconds) for consumer request timeout",
      config::required::no,
      100ms)
  , consumer_request_max_bytes(
      *this,
      "consumer_request_max_bytes",
      "Max bytes to fetch per request",
      config::required::no,
      1_MiB)
  , consumer_session_timeout(
      *this,
      "consumer_session_timeout_ms",
      "Timeout (in milliseconds) for consumer session",
      config::required::no,
      10s)
  , consumer_rebalance_timeout(
      *this,
      "consumer_rebalance_timeout_ms",
      "Timeout (in milliseconds) for consumer rebalance",
      config::required::no,
      2s)
  , consumer_heartbeat_interval(
      *this,
      "consumer_heartbeat_interval_ms",
      "Interval (in milliseconds) for consumer heartbeats",
      config::required::no,
      500ms)
  , sasl_mechanism(
      *this,
      "sasl_mechanism",
      "The SASL mechanism to use when connecting",
      config::required::no,
      "")
  , scram_username(
      *this,
      "scram_username",
      "Username to use for SCRAM authentication mechanisms",
      config::required::no,
      "")
  , scram_password(
      *this,
      "scram_password",
      "Password to use for SCRAM authentication mechanisms",
      {.secret = config::is_secret::yes},
      "") {}

} // namespace kafka::client
