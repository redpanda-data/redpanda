// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/types.h"

#include "base/vassert.h"
#include "model/async_adl_serde.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/transfer_leadership.h"
#include "reflection/adl.h"
#include "reflection/async_adl.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <chrono>
namespace {
template<typename T>
T decode_signed(T value) {
    return value < T(0) ? T{} : value;
}

template<typename T>
T varlong_reader(iobuf_parser& in) {
    auto [val, len] = in.read_varlong();
    return T(val);
}

struct hbeat_soa {
    explicit hbeat_soa(size_t n)
      : groups(n)
      , commit_indices(n)
      , terms(n)
      , prev_log_indices(n)
      , prev_log_terms(n)
      , last_visible_indices(n)
      , revisions(n)
      , target_revisions(n) {}

    ~hbeat_soa() noexcept = default;
    hbeat_soa(const hbeat_soa&) = delete;
    hbeat_soa& operator=(const hbeat_soa&) = delete;
    hbeat_soa(hbeat_soa&&) noexcept = default;
    hbeat_soa& operator=(hbeat_soa&&) noexcept = default;

    std::vector<raft::group_id> groups;
    std::vector<model::offset> commit_indices;
    std::vector<model::term_id> terms;
    std::vector<model::offset> prev_log_indices;
    std::vector<model::term_id> prev_log_terms;
    std::vector<model::offset> last_visible_indices;
    std::vector<model::revision_id> revisions;
    std::vector<model::revision_id> target_revisions;
};

struct hbeat_response_array {
    explicit hbeat_response_array(size_t n)
      : groups(n)
      , terms(n)
      , last_flushed_log_index(n)
      , last_dirty_log_index(n)
      , last_term_base_offset(n)
      , revisions(n)
      , target_revisions(n) {}

    std::vector<raft::group_id> groups;
    std::vector<model::term_id> terms;
    std::vector<model::offset> last_flushed_log_index;
    std::vector<model::offset> last_dirty_log_index;
    std::vector<model::offset> last_term_base_offset;
    std::vector<model::revision_id> revisions;
    std::vector<model::revision_id> target_revisions;
};
template<typename T>
void encode_one_vint(iobuf& out, const T& t) {
    auto b = vint::to_bytes(t);
    // NOLINTNEXTLINE
    out.append(reinterpret_cast<const char*>(b.data()), b.size());
}

template<typename T>
void encode_varint_delta(iobuf& out, const T& prev, const T& current) {
    // TODO: use delta-delta:
    // https://github.com/facebookarchive/beringei/blob/92784ec6e2/beringei/lib/BitUtil.cpp
    auto delta = current - prev;
    encode_one_vint(out, delta);
}

template<typename T>
void encode_one_delta_array(iobuf& o, const std::vector<T>& v) {
    if (v.empty()) {
        return;
    }
    const size_t max = v.size();
    encode_one_vint(o, v[0]);
    for (size_t i = 1; i < max; ++i) {
        encode_varint_delta(o, v[i - 1], v[i]);
    }
}
template<typename T>
T read_one_varint_delta(iobuf_parser& in, const T& prev) {
    auto dst = varlong_reader<T>(in);
    return prev + dst;
}
} // namespace

namespace raft {

replicate_stages::replicate_stages(
  ss::future<> enq, ss::future<result<replicate_result>> offset_future)
  : request_enqueued(std::move(enq))
  , replicate_finished(std::move(offset_future)) {}

replicate_stages::replicate_stages(raft::errc ec)
  : request_enqueued(ss::now())
  , replicate_finished(
      ss::make_ready_future<result<replicate_result>>(make_error_code(ec))) {};

std::ostream& operator<<(std::ostream& o, const vnode& id) {
    fmt::print(o, "{{id: {}, revision: {}}}", id.id(), id.revision());
    return o;
}

std::ostream& operator<<(std::ostream& o, const append_entries_reply& r) {
    fmt::print(
      o,
      "{{node_id: {}, target_node_id: {}, group: {}, term: {}, "
      "last_dirty_log_index: {}, last_flushed_log_index: {}, "
      "last_term_base_offset: {}, result: {}, may_recover: {}}}",
      r.node_id,
      r.target_node_id,
      r.group,
      r.term,
      r.last_dirty_log_index,
      r.last_flushed_log_index,
      r.last_term_base_offset,
      r.result,
      r.may_recover);
    return o;
}

std::ostream& operator<<(std::ostream& o, const vote_request& r) {
    fmt::print(
      o,
      "{{node_id: {}, target_node_id: {}, group: {}, term: {}, prev_log_index: "
      "{}, prev_log_term: {}, leadership_xfer: {}}}",
      r.node_id,
      r.target_node_id,
      r.group,
      r.term,
      r.prev_log_index,
      r.prev_log_term,
      r.leadership_transfer);
    return o;
}
std::ostream& operator<<(std::ostream& o, const follower_metrics& i) {
    fmt::print(
      o,
      "{{node_id: {}, is_learner: {}, committed_log_index: {}, "
      "dirty_log_index: {}, match_index: {}, is_live: {}, "
      "under_replicated: {}}}",
      i.id,
      i.is_learner,
      i.committed_log_index,
      i.dirty_log_index,
      i.match_index,
      i.is_live,
      i.under_replicated);
    return o;
}
std::ostream& operator<<(std::ostream& o, const heartbeat_metadata& hm) {
    fmt::print(
      o,
      "{{node_id: {}, target_node_id: {}, protocol_metadata: {}}}",
      hm.node_id,
      hm.target_node_id,
      hm.meta);
    return o;
}

std::ostream& operator<<(std::ostream& o, const consistency_level& l) {
    switch (l) {
    case consistency_level::quorum_ack:
        o << "consistency_level::quorum_ack";
        break;
    case consistency_level::leader_ack:
        o << "consistency_level::leader_ack";
        break;
    case consistency_level::no_ack:
        o << "consistency_level::no_ack";
        break;
    default:
        o << "unknown consistency_level";
    }
    return o;
}
std::ostream& operator<<(std::ostream& o, const protocol_metadata& m) {
    fmt::print(
      o,
      "{{group: {}, commit_index: {}, term: {}, prev_log_index: {}, "
      "prev_log_term: {}, last_visible_index: {}, dirty_offset: {}, "
      "prev_log_delta: {}}}",
      m.group,
      m.commit_index,
      m.term,
      m.prev_log_index,
      m.prev_log_term,
      m.last_visible_index,
      m.dirty_offset,
      m.prev_log_delta);
    return o;
}

std::ostream& operator<<(std::ostream& o, const vote_reply& r) {
    fmt::print(
      o,
      "{{term: {}, target_node: {}, vote_granted: {}, log_ok: {}}}",
      r.term,
      r.target_node_id,
      r.granted,
      r.log_ok);
    return o;
}
std::ostream& operator<<(std::ostream& o, const reply_result& r) {
    switch (r) {
    case reply_result::success:
        o << "success";
        return o;
    case reply_result::failure:
        o << "failure";
        return o;
    case reply_result::group_unavailable:
        o << "group_unavailable";
        return o;
    case reply_result::follower_busy:
        o << "follower_busy";
        return o;
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const install_snapshot_request& r) {
    fmt::print(
      o,
      "{{term: {}, group: {}, target_node_id: {}, node_id: {}, "
      "last_included_index: {}, "
      "file_offset: {}, chunk_size: {}, done: {}, dirty_offset: {}}}",
      r.term,
      r.group,
      r.target_node_id,
      r.node_id,
      r.last_included_index,
      r.file_offset,
      r.chunk.size_bytes(),
      r.done,
      r.dirty_offset);
    return o;
}

std::ostream& operator<<(std::ostream& o, const install_snapshot_reply& r) {
    fmt::print(
      o,
      "{{term: {}, target_node_id: {}, bytes_stored: {}, success: {}}}",
      r.term,
      r.target_node_id,
      r.bytes_stored,
      r.success);
    return o;
}

append_entries_request::append_entries_request(
  vnode src,
  protocol_metadata m,
  chunked_vector<model::record_batch> r,
  size_t size,
  flush_after_append f) noexcept
  : append_entries_request(src, vnode{}, m, std::move(r), size, f) {}

append_entries_request::append_entries_request(
  vnode src,
  vnode target,
  protocol_metadata m,
  chunked_vector<model::record_batch> r,
  size_t size,
  flush_after_append f) noexcept
  : _source_node(src)
  , _target_node_id(target)
  , _meta(m)
  , _flush(f)
  , _batches(std::move(r))
  , _total_size(size + sizeof(append_entries_request)) {}

size_t append_entries_request::batches_size() const {
    return total_size() - sizeof(append_entries_request);
}

ss::future<> append_entries_request::serde_async_write(iobuf& dst) {
    using serde::write;
    using serde::write_async;

    iobuf out;
    write(out, static_cast<uint32_t>(_batches.size()));
    for (auto& batch : _batches) {
        co_await reflection::async_adl<model::record_batch>{}.to(
          out, std::move(batch));
    }
    write(out, _source_node);
    write(out, _target_node_id);
    write(out, _meta);
    write(out, _flush);

    write(dst, std::move(out));
}

ss::future<append_entries_request>
append_entries_request::serde_async_direct_read(
  iobuf_parser& src, serde::header h) {
    using serde::read_async_nested;
    using serde::read_nested;

    auto tmp = co_await read_async_nested<iobuf>(src, h._bytes_left_limit);
    iobuf_parser in(std::move(tmp));

    auto batch_count = read_nested<uint32_t>(in, 0U);
    // use chunked fifo as usually batches size is small
    chunked_vector<model::record_batch> batches{};
    size_t batches_size{0};
    for (uint32_t i = 0; i < batch_count; ++i) {
        auto b = co_await reflection::async_adl<model::record_batch>{}.from(in);
        batches_size += b.size_bytes();
        batches.push_back(std::move(b));
        co_await ss::coroutine::maybe_yield();
    }

    auto node_id = read_nested<raft::vnode>(in, 0U);
    auto target_node_id = read_nested<raft::vnode>(in, 0U);
    auto meta = read_nested<raft::protocol_metadata>(in, 0U);
    auto flush = read_nested<raft::flush_after_append>(in, 0U);

    co_return append_entries_request(
      node_id, target_node_id, meta, std::move(batches), batches_size, flush);
}

ss::future<>
append_entries_request_serde_wrapper::serde_async_write(iobuf& dst) {
    using serde::write;

    write(dst, _request.source_node());
    write(dst, _request.target_node());
    write(dst, _request.metadata());
    write(dst, _request.is_flush_required());
    co_await serde::write_async(dst, std::move(_request).release_batches());
}

ss::future<append_entries_request_serde_wrapper>
append_entries_request_serde_wrapper::serde_async_direct_read(
  iobuf_parser& src, serde::header h) {
    using serde::read_async_nested;
    using serde::read_nested;

    auto node_id = read_nested<raft::vnode>(src, 0U);
    auto target_node_id = read_nested<raft::vnode>(src, 0U);
    auto meta = read_nested<raft::protocol_metadata>(src, 0U);
    auto flush = read_nested<raft::flush_after_append>(src, 0U);
    auto batch_count = read_nested<uint32_t>(src, 0U);

    chunked_vector<model::record_batch> batches{};
    batches.reserve(batch_count);
    size_t batches_size{0};
    for (uint32_t i = 0; i < batch_count; ++i) {
        auto b = co_await serde::read_async_nested<model::record_batch>(
          src, h._bytes_left_limit);
        batches_size += b.size_bytes();
        batches.push_back(std::move(b));
        co_await ss::coroutine::maybe_yield();
    }

    co_return append_entries_request(
      node_id, target_node_id, meta, std::move(batches), batches_size, flush);
}

std::ostream& operator<<(std::ostream& o, const append_entries_request& r) {
    if (r._batches.empty()) {
        fmt::print(
          o,
          "node_id: {}, target_node_id: {}, protocol metadata: {}, batches: "
          "{{}}",
          r._source_node,
          r._target_node_id,
          r._meta);
    } else {
        fmt::print(
          o,
          "node_id: {}, target_node_id: {}, protocol metadata: {}, batch "
          "count: {}, offset range: [{},{}]",
          r._source_node,
          r._target_node_id,
          r._meta,
          r._batches.size(),
          r._batches.front().base_offset(),
          r._batches.back().last_offset());
    }
    return o;
}

std::ostream&
operator<<(std::ostream& o, const transfer_leadership_request& r) {
    fmt::print(
      o, "group {} target {} timeout {}", r.group, r.target, r.timeout);
    return o;
}

} // namespace raft

namespace reflection {

raft::snapshot_metadata adl<raft::snapshot_metadata>::from(iobuf_parser& in) {
    auto last_included_index = adl<model::offset>{}.from(in);
    auto last_included_term = adl<model::term_id>{}.from(in);
    raft::offset_translator_delta log_start_delta;

    auto version = adl<int8_t>{}.from(in.peek(sizeof(int8_t)));

    // if peeked buffer contains version greater than initial version we deal
    // with new snapshot metadata
    if (version >= raft::snapshot_metadata::initial_version) {
        in.skip(sizeof(int8_t));
    }

    auto cfg = raft::details::deserialize_nested_configuration(in);
    ss::lowres_clock::time_point cluster_time{
      adl<std::chrono::milliseconds>{}.from(in)};

    if (version >= raft::snapshot_metadata::initial_version) {
        log_start_delta = adl<raft::offset_translator_delta>{}.from(in);
    }

    return raft::snapshot_metadata{
      .last_included_index = last_included_index,
      .last_included_term = last_included_term,
      .latest_configuration = std::move(cfg),
      .cluster_time = cluster_time,
      .log_start_delta = log_start_delta};
}

void adl<raft::snapshot_metadata>::to(
  iobuf& out, raft::snapshot_metadata&& md) {
    reflection::serialize(
      out, md.last_included_index, md.last_included_term, md.version);

    auto cfg_buffer = raft::details::serialize_configuration(
      std::move(md.latest_configuration));
    out.append_fragments(std::move(cfg_buffer));

    reflection::serialize(
      out,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        md.cluster_time.time_since_epoch()),
      md.log_start_delta);
}
} // namespace reflection
