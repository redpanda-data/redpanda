/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/write_request.h"

#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/logger.h"

namespace cloud_topics::l0 {

template<class Clock>
write_request<Clock>::write_request(
  model::ntp ntp,
  cluster_epoch topic_start_epoch,
  serialized_chunk chunk,
  timestamp_t timeout,
  pipeline_stage stage)
  : ntp(std::move(ntp))
  , topic_start_epoch(topic_start_epoch)
  , data_chunk(std::move(chunk))
  , ingestion_time(Clock::now())
  , expiration_time(timeout)
  , stage(stage) {}

template<class Clock>
void write_request<Clock>::set_value(errc e) noexcept {
    try {
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
size_t write_request<Clock>::size_bytes() const noexcept {
    return data_chunk.payload.size_bytes();
}

template<class Clock>
void write_request<Clock>::set_value(upload_meta meta) noexcept {
    try {
        response.set_value(std::move(meta));
    } catch (const ss::broken_promise&) {
        vlog(cd_log.error, "Can't acknowledge request for {}", ntp);
    }
}

template<class Clock>
bool write_request<Clock>::has_expired() const noexcept {
    return Clock::now() > expiration_time;
}

template struct write_request<ss::lowres_clock>;
template struct write_request<ss::manual_clock>;
} // namespace cloud_topics::l0
