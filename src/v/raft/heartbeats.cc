/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/heartbeats.h"

#include "raft/types.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <cstdint>

namespace raft {
void heartbeat_request_v2::add(const group_heartbeat& hb) {
    if (hb.data) {
        _full_heartbeats.push_back(
          full_heartbeat{.group = hb.group, .data = *hb.data});
    } else {
        _lw_heartbeats.add(hb.group);
    }
}

heartbeat_request_v2 heartbeat_request_v2::copy() const {
    heartbeat_request_v2 ret;
    ret._source_node = _source_node;
    ret._target_node = _target_node;
    ret._lw_heartbeats = _lw_heartbeats.copy();
    ret._full_heartbeats.reserve(_full_heartbeats.size());

    std::copy(
      _full_heartbeats.begin(),
      _full_heartbeats.end(),
      std::back_inserter(ret._full_heartbeats));
    ret._lw_cnt = _lw_cnt;

    return ret;
}

ss::future<> heartbeat_request_v2::serde_async_write(iobuf& out) {
    using serde::write;
    using serde::write_async;
    write(out, _source_node);
    write(out, _target_node);
    write(out, _lw_cnt);

    co_await write_async(out, std::move(_lw_heartbeats));
    co_await ss::coroutine::maybe_yield();
    co_await write_async(out, std::move(_full_heartbeats));
}

ss::future<> heartbeat_request_v2::serde_async_read(
  iobuf_parser& in, const serde::header hdr) {
    using serde::read_async_nested;
    using serde::read_nested;
    _source_node = serde::read_nested<model::node_id>(
      in, hdr._bytes_left_limit);
    _target_node = serde::read_nested<model::node_id>(
      in, hdr._bytes_left_limit);
    _lw_cnt = serde::read_nested<uint64_t>(in, hdr._bytes_left_limit);

    _lw_heartbeats = co_await read_async_nested<lw_column_t>(
      in, hdr._bytes_left_limit);

    co_await ss::coroutine::maybe_yield();

    _full_heartbeats = co_await read_async_nested<full_heartbeats_t>(
      in, hdr._bytes_left_limit);
}

heartbeat_reply_v2 heartbeat_reply_v2::copy() const {
    heartbeat_reply_v2 ret;
    ret._source_node = _source_node;
    ret._target_node = _target_node;
    ret._lw_replies = _lw_replies.copy();
    ret._results = _results.copy();

    std::copy(
      _full_replies.begin(),
      _full_replies.end(),
      std::back_inserter(ret._full_replies));

    return ret;
}

void heartbeat_reply_v2::add(group_id group, reply_result status) {
    _lw_replies.add(group);
    _results.add(static_cast<uint8_t>(status));
}

void heartbeat_reply_v2::add(
  group_id id, reply_result status, const heartbeat_reply_data& data) {
    _full_replies.push_back(full_heartbeat_reply{
      .group = id,
      .result = status,
      .data = data,
    });
}

ss::future<> heartbeat_reply_v2::serde_async_write(iobuf& out) {
    using serde::write;
    using serde::write_async;
    write(out, _source_node);
    write(out, _target_node);

    co_await write_async(out, std::move(_lw_replies));
    co_await ss::coroutine::maybe_yield();
    co_await write_async(out, std::move(_results));
    co_await ss::coroutine::maybe_yield();
    co_await write_async(out, std::move(_full_replies));
}

ss::future<> heartbeat_reply_v2::serde_async_read(
  iobuf_parser& in, const serde::header hdr) {
    using serde::read_async_nested;
    using serde::read_nested;
    _source_node = read_nested<model::node_id>(in, hdr._bytes_left_limit);
    _target_node = read_nested<model::node_id>(in, hdr._bytes_left_limit);

    _lw_replies = co_await read_async_nested<lw_column_t>(
      in, hdr._bytes_left_limit);

    co_await ss::coroutine::maybe_yield();

    _results = co_await read_async_nested<result_column_t>(
      in, hdr._bytes_left_limit);

    co_await ss::coroutine::maybe_yield();

    _full_replies
      = co_await read_async_nested<ss::chunked_fifo<full_heartbeat_reply>>(
        in, hdr._bytes_left_limit);
}
} // namespace raft
