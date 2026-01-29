/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/read_request.h"

#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/logger.h"
#include "utils/retry_chain_node.h"

#include <chrono>

namespace cloud_topics::l0 {

template<class Clock>
read_request<Clock>::read_request(
  model::ntp ntp,
  dataplane_query query,
  timestamp_t timeout,
  basic_retry_chain_node<Clock>* root_rtc,
  pipeline_stage stage)
  : ntp(std::move(ntp))
  , query(std::move(query))
  , ingestion_time(Clock::now())
  , expiration_time(timeout)
  , stage(stage)
  , rtc(
      expiration_time,
      std::chrono::milliseconds(100),
      retry_strategy::backoff,
      root_rtc)
  , rtc_logger(
      cd_log, rtc, ssx::sformat("ct:read_request[{}]", this->ntp.path())) {
    vlog(
      rtc_logger.debug,
      "read_request created, timeout: {}",
      std::chrono::duration_cast<std::chrono::milliseconds>(rtc.get_timeout()));
}

template<class Clock>
void read_request<Clock>::set_value(errc e) noexcept {
    try {
        vlog(rtc_logger.debug, "Setting request error to: {}", e);
        response.set_value(std::unexpected(e));
    } catch (const ss::broken_promise&) {
        vlog(
          cd_log.error,
          "Can't fail request for {}, error {} will be lost",
          ntp,
          e);
    }
}

template<class Clock>
void read_request<Clock>::set_value(dataplane_query_result result) noexcept {
    try {
        response.set_value(std::move(result));
    } catch (const ss::broken_promise&) {
        vlog(cd_log.error, "Can't acknowledge request for {}", ntp);
    }
}

template<class Clock>
bool read_request<Clock>::has_expired() const noexcept {
    return Clock::now() > expiration_time;
}

template struct read_request<ss::lowres_clock>;
template struct read_request<ss::manual_clock>;
} // namespace cloud_topics::l0
