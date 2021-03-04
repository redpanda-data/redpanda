/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/fetcher.h"

#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "seastar/core/gate.hh"

namespace kafka::client {

fetch_request make_fetch_request(
  const model::topic_partition& tp,
  model::offset offset,
  int32_t max_bytes,
  std::chrono::milliseconds timeout) {
    std::vector<fetch_request::partition> partitions;
    partitions.push_back(fetch_request::partition{
      .id{tp.partition},
      .current_leader_epoch = 0,
      .fetch_offset{offset},
      .log_start_offset{model::offset{-1}},
      .partition_max_bytes = max_bytes});
    std::vector<fetch_request::topic> topics;
    topics.push_back(fetch_request::topic{
      .name{tp.topic}, .partitions{std::move(partitions)}});

    return fetch_request{
      .replica_id{consumer_replica_id},
      .max_wait_time{timeout},
      .min_bytes = 0,
      .max_bytes = max_bytes,
      .isolation_level = 0,
      .topics{std::move(topics)}};
}

fetch_response
make_fetch_response(const model::topic_partition& tp, std::exception_ptr ex) {
    error_code error;
    try {
        std::rethrow_exception(std::move(ex));
    } catch (const partition_error& ex) {
        vlog(kclog.debug, "handling partition_error {}", ex);
        error = ex.error;
    } catch (const broker_error& ex) {
        vlog(kclog.debug, "handling broker_error {}", ex);
        error = ex.error;
    } catch (const ss::gate_closed_exception&) {
        vlog(kclog.debug, "gate_closed_exception");
        error = error_code::operation_not_attempted;
    } catch (const std::exception& ex) {
        vlog(kclog.warn, "std::exception {}", ex);
        error = error_code::unknown_server_error;
    } catch (const std::exception_ptr&) {
        vlog(kclog.error, "std::exception_ptr");
        error = error_code::unknown_server_error;
    }
    fetch_response::partition_response pr{
      .id{tp.partition},
      .error = error,
      .high_watermark{model::offset{-1}},
      .last_stable_offset{model::offset{-1}},
      .log_start_offset{model::offset{-1}},
      .aborted_transactions{},
      .record_set{}};

    std::vector<fetch_response::partition_response> responses;
    responses.push_back(std::move(pr));
    auto response = fetch_response::partition(tp.topic);
    response.responses = std::move(responses);
    std::vector<fetch_response::partition> parts;
    parts.push_back(std::move(response));
    return fetch_response{
      .error = error,
      .partitions = std::move(parts),
    };
}

} // namespace kafka::client