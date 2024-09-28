/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/write_request.h"

#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/logger.h"

namespace experimental::cloud_topics::details {

template<class Clock>
write_request<Clock>::write_request(
  model::ntp ntp,
  batcher_req_index index,
  serialized_chunk chunk,
  std::chrono::milliseconds timeout)
  : ntp(std::move(ntp))
  , index(index)
  , data_chunk(std::move(chunk))
  , ingestion_time(Clock::now())
  , expiration_time(Clock::now() + timeout) {}

template<class Clock>
void write_request<Clock>::set_value(errc e) noexcept {
    try {
        response.set_value(e);
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
void write_request<Clock>::set_value(
  ss::circular_buffer<model::record_batch> placeholders) noexcept {
    try {
        response.set_value(std::move(placeholders));
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
} // namespace experimental::cloud_topics::details
