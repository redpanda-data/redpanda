// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/types.h"

#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "reflection/adl.h"
#include "utils/to_string.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <type_traits>

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

namespace internal {
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
} // namespace internal
} // namespace

namespace raft {

replicate_stages::replicate_stages(
  ss::future<> enq, ss::future<result<replicate_result>> offset_future)
  : request_enqueued(std::move(enq))
  , replicate_finished(std::move(offset_future)) {}

replicate_stages::replicate_stages(raft::errc ec)
  : request_enqueued(ss::now())
  , replicate_finished(
      ss::make_ready_future<result<replicate_result>>(make_error_code(ec))){};

void follower_index_metadata::reset() {
    last_dirty_log_index = model::offset{};
    last_flushed_log_index = model::offset{};
    last_sent_offset = model::offset{};
    match_index = model::offset{};
    next_index = model::offset{};
    heartbeats_failed = 0;
    last_sent_seq = follower_req_seq{0};
    last_received_seq = follower_req_seq{0};
    last_successful_received_seq = follower_req_seq{0};
    last_suppress_heartbeats_seq = follower_req_seq{0};
    suppress_heartbeats = heartbeats_suppressed::no;
}

std::ostream& operator<<(std::ostream& o, const vnode& id) {
    return o << "{id: " << id.id() << ", revision: " << id.revision() << "}";
}

std::ostream& operator<<(std::ostream& o, const append_entries_reply& r) {
    return o << "{node_id: " << r.node_id << ", target_node_id"
             << r.target_node_id << ", group: " << r.group
             << ", term:" << r.term
             << ", last_dirty_log_index:" << r.last_dirty_log_index
             << ", last_flushed_log_index:" << r.last_flushed_log_index
             << ", last_term_base_offset:" << r.last_term_base_offset
             << ", result: " << r.result << "}";
}

std::ostream& operator<<(std::ostream& o, const vote_request& r) {
    return o << "{node_id: " << r.node_id << ", target_node_id"
             << r.target_node_id << ", group: " << r.group
             << ", term:" << r.term << ", prev_log_index:" << r.prev_log_index
             << ", prev_log_term: " << r.prev_log_term
             << ", leadership_xfer: " << r.leadership_transfer << "}";
}
std::ostream& operator<<(std::ostream& o, const follower_index_metadata& i) {
    return o << "{node_id: " << i.node_id
             << ", last_committed_log_idx: " << i.last_flushed_log_index
             << ", last_dirty_log_idx: " << i.last_dirty_log_index
             << ", match_index: " << i.match_index
             << ", next_index: " << i.next_index
             << ", is_learner: " << i.is_learner
             << ", is_recovering: " << i.is_recovering << "}";
}

std::ostream& operator<<(std::ostream& o, const heartbeat_request& r) {
    o << "{meta:(" << r.heartbeats.size() << ") [";
    for (auto& m : r.heartbeats) {
        o << "meta: " << m.meta << ","
          << "node_id: " << m.node_id << ","
          << "target_node_id: " << m.target_node_id << ",";
    }
    return o << "]}";
}
std::ostream& operator<<(std::ostream& o, const heartbeat_reply& r) {
    o << "{meta:[";
    for (auto& m : r.meta) {
        o << m << ",";
    }
    return o << "]}";
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
    return o << "{raft_group:" << m.group << ", commit_index:" << m.commit_index
             << ", term:" << m.term << ", prev_log_index:" << m.prev_log_index
             << ", prev_log_term:" << m.prev_log_term
             << ", last_visible_index:" << m.last_visible_index << "}";
}
std::ostream& operator<<(std::ostream& o, const vote_reply& r) {
    return o << "{term:" << r.term << ", target_node_id" << r.target_node_id
             << ", vote_granted: " << r.granted << ", log_ok:" << r.log_ok
             << "}";
}
std::ostream&
operator<<(std::ostream& o, const append_entries_reply::status& r) {
    switch (r) {
    case append_entries_reply::status::success:
        o << "success";
        return o;
    case append_entries_reply::status::failure:
        o << "failure";
        return o;
    case append_entries_reply::status::group_unavailable:
        o << "group_unavailable";
        return o;
    case append_entries_reply::status::timeout:
        o << "timeout";
        return o;
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const install_snapshot_request& r) {
    fmt::print(
      o,
      "{{term: {}, group: {}, target_node_id: {}, node_id: {}, "
      "last_included_index: {}, "
      "file_offset: {}, chunk_size: {}, done: {}}}",
      r.term,
      r.group,
      r.target_node_id,
      r.node_id,
      r.last_included_index,
      r.file_offset,
      r.chunk.size_bytes(),
      r.done);
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

ss::future<> heartbeat_request::serde_async_write(iobuf& dst) {
    vassert(!heartbeats.empty(), "cannot serialize empty heartbeats request");

    struct sorter_fn {
        constexpr bool operator()(
          const raft::heartbeat_metadata& lhs,
          const raft::heartbeat_metadata& rhs) const {
            return lhs.meta.commit_index < rhs.meta.commit_index;
        }
    };

    iobuf out;
    auto& request = *this;

    std::sort(
      request.heartbeats.begin(), request.heartbeats.end(), sorter_fn{});

    co_await ss::coroutine::maybe_yield();

    internal::hbeat_soa encodee(request.heartbeats.size());
    // target physical node id is always the same it differs only by
    // revision

    const size_t size = request.heartbeats.size();
    for (size_t i = 0; i < size; ++i) {
        const auto& m = request.heartbeats[i].meta;
        const raft::vnode node = request.heartbeats[i].node_id;
        const raft::vnode target_node = request.heartbeats[i].target_node_id;
        vassert(m.group() >= 0, "Negative raft group detected. {}", m.group);
        encodee.groups[i] = m.group;
        encodee.commit_indices[i] = std::max(model::offset(-1), m.commit_index);
        encodee.terms[i] = std::max(model::term_id(-1), m.term);
        encodee.prev_log_indices[i] = std::max(
          model::offset(-1), m.prev_log_index);
        encodee.prev_log_terms[i] = std::max(
          model::term_id(-1), m.prev_log_term);
        encodee.last_visible_indices[i] = std::max(
          model::offset(-1), m.last_visible_index);
        encodee.revisions[i] = std::max(
          model::revision_id(-1), node.revision());
        encodee.target_revisions[i] = std::max(
          model::revision_id(-1), target_node.revision());

        co_await ss::coroutine::maybe_yield();
    }
    // important to release this memory after this function
    // request.meta = {}; // release memory

    using serde::write;

    // physical node ids are the same for all requests
    write(out, request.heartbeats.front().node_id.id());
    write(out, request.heartbeats.front().target_node_id.id());
    write(out, static_cast<uint32_t>(size));

    internal::encode_one_delta_array<raft::group_id>(out, encodee.groups);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.commit_indices);
    internal::encode_one_delta_array<model::term_id>(out, encodee.terms);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.prev_log_indices);
    internal::encode_one_delta_array<model::term_id>(
      out, encodee.prev_log_terms);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_visible_indices);
    internal::encode_one_delta_array<model::revision_id>(
      out, encodee.revisions);
    internal::encode_one_delta_array<model::revision_id>(
      out, encodee.target_revisions);

    write(dst, std::move(out));
}

void heartbeat_request::serde_read(
  iobuf_parser& src, const serde::header& hdr) {
    using serde::read_nested;
    auto tmp = read_nested<iobuf>(src, hdr._bytes_left_limit);
    iobuf_parser in(std::move(tmp));

    auto& req = *this;
    auto node_id = read_nested<model::node_id>(in, 0U);
    auto target_node = read_nested<model::node_id>(in, 0U);
    req.heartbeats = std::vector<raft::heartbeat_metadata>(
      read_nested<uint32_t>(in, 0U));
    if (req.heartbeats.empty()) {
        return;
    }
    const size_t max = req.heartbeats.size();
    req.heartbeats[0].meta.group = varlong_reader<raft::group_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.group
          = internal::read_one_varint_delta<raft::group_id>(
            in, req.heartbeats[i - 1].meta.group);
    }
    req.heartbeats[0].meta.commit_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.commit_index
          = internal::read_one_varint_delta<model::offset>(
            in, req.heartbeats[i - 1].meta.commit_index);
    }
    req.heartbeats[0].meta.term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.term
          = internal::read_one_varint_delta<model::term_id>(
            in, req.heartbeats[i - 1].meta.term);
    }
    req.heartbeats[0].meta.prev_log_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.prev_log_index
          = internal::read_one_varint_delta<model::offset>(
            in, req.heartbeats[i - 1].meta.prev_log_index);
    }
    req.heartbeats[0].meta.prev_log_term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.prev_log_term
          = internal::read_one_varint_delta<model::term_id>(
            in, req.heartbeats[i - 1].meta.prev_log_term);
    }
    req.heartbeats[0].meta.last_visible_index = varlong_reader<model::offset>(
      in);
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].meta.last_visible_index
          = internal::read_one_varint_delta<model::offset>(
            in, req.heartbeats[i - 1].meta.last_visible_index);
    }

    req.heartbeats[0].node_id = raft::vnode(
      node_id, varlong_reader<model::revision_id>(in));
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].node_id = raft::vnode(
          node_id,
          internal::read_one_varint_delta<model::revision_id>(
            in, req.heartbeats[i - 1].node_id.revision()));
    }

    req.heartbeats[0].target_node_id = raft::vnode(
      target_node, varlong_reader<model::revision_id>(in));
    for (size_t i = 1; i < max; ++i) {
        req.heartbeats[i].target_node_id = raft::vnode(
          target_node,
          internal::read_one_varint_delta<model::revision_id>(
            in, req.heartbeats[i - 1].target_node_id.revision()));
    }

    for (auto& hb : req.heartbeats) {
        hb.meta.prev_log_index = decode_signed(hb.meta.prev_log_index);
        hb.meta.commit_index = decode_signed(hb.meta.commit_index);
        hb.meta.prev_log_term = decode_signed(hb.meta.prev_log_term);
        hb.meta.last_visible_index = decode_signed(hb.meta.last_visible_index);
        hb.node_id = raft::vnode(
          hb.node_id.id(), decode_signed(hb.node_id.revision()));
        hb.target_node_id = raft::vnode(
          hb.target_node_id.id(), decode_signed(hb.target_node_id.revision()));
    }
}

void heartbeat_reply::serde_write(iobuf& dst) {
    using serde::write;

    auto& reply = *this;
    iobuf out;

    struct sorter_fn {
        constexpr bool operator()(
          const raft::append_entries_reply& lhs,
          const raft::append_entries_reply& rhs) const {
            return lhs.last_flushed_log_index < rhs.last_flushed_log_index;
        }
    };

    write(out, static_cast<uint32_t>(reply.meta.size()));
    // no requests
    if (reply.meta.empty()) {
        return;
    }

    std::sort(reply.meta.begin(), reply.meta.end(), sorter_fn{});
    /**
     * We use a target/source node_id from the last available append_entries
     * response as all of the failed responses (timeouts and responses for which
     * group couldn't be find) are present at the beginning after the array is
     * sorted since last_flushed_log_index for those replies is an uninitialized
     * i.e. model::offset::min().
     */
    auto it = reply.meta.rbegin();
    for (; it != reply.meta.rend(); ++it) {
        if (likely(it->target_node_id.id() != model::node_id{})) {
            // replies are coming from the same physical node
            write(out, it->node_id.id());
            // replies are addressed to the same physical node
            write(out, it->target_node_id.id());
            break;
        }
    }
    /**
     * There are no successful heartbeat replies, fill in with information from
     * first reply
     */
    if (unlikely(it == reply.meta.rend())) {
        write(out, reply.meta.front().node_id.id());
        write(out, reply.meta.front().target_node_id.id());
    }

    internal::hbeat_response_array encodee(reply.meta.size());

    for (size_t i = 0; i < reply.meta.size(); ++i) {
        encodee.groups[i] = reply.meta[i].group;
        encodee.terms[i] = std::max(model::term_id(-1), reply.meta[i].term);

        encodee.last_flushed_log_index[i] = std::max(
          model::offset(-1), reply.meta[i].last_flushed_log_index);
        encodee.last_dirty_log_index[i] = std::max(
          model::offset(-1), reply.meta[i].last_dirty_log_index);
        encodee.last_term_base_offset[i] = std::max(
          model::offset(-1), reply.meta[i].last_term_base_offset);
        encodee.revisions[i] = std::max(
          model::revision_id(-1), reply.meta[i].node_id.revision());
        encodee.target_revisions[i] = std::max(
          model::revision_id(-1), reply.meta[i].target_node_id.revision());
    }
    internal::encode_one_delta_array<raft::group_id>(out, encodee.groups);
    internal::encode_one_delta_array<model::term_id>(out, encodee.terms);

    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_flushed_log_index);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_dirty_log_index);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_term_base_offset);
    internal::encode_one_delta_array<model::revision_id>(
      out, encodee.revisions);
    internal::encode_one_delta_array<model::revision_id>(
      out, encodee.target_revisions);
    for (auto& m : reply.meta) {
        write(out, m.result);
    }

    write(dst, std::move(out));
}

void heartbeat_reply::serde_read(iobuf_parser& src, const serde::header& hdr) {
    using serde::read_nested;
    auto tmp = read_nested<iobuf>(src, hdr._bytes_left_limit);
    iobuf_parser in(std::move(tmp));

    auto& reply = *this;
    reply.meta = std::vector<raft::append_entries_reply>(
      read_nested<uint32_t>(in, 0U));

    // empty reply
    if (reply.meta.empty()) {
        return;
    }

    auto node_id = read_nested<model::node_id>(in, 0U);
    auto target_node_id = read_nested<model::node_id>(in, 0U);

    size_t size = reply.meta.size();
    reply.meta[0].group = varlong_reader<raft::group_id>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].group = internal::read_one_varint_delta<raft::group_id>(
          in, reply.meta[i - 1].group);
    }
    reply.meta[0].term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].term = internal::read_one_varint_delta<model::term_id>(
          in, reply.meta[i - 1].term);
    }

    reply.meta[0].last_flushed_log_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].last_flushed_log_index
          = internal::read_one_varint_delta<model::offset>(
            in, reply.meta[i - 1].last_flushed_log_index);
    }

    reply.meta[0].last_dirty_log_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].last_dirty_log_index
          = internal::read_one_varint_delta<model::offset>(
            in, reply.meta[i - 1].last_dirty_log_index);
    }

    reply.meta[0].last_term_base_offset = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].last_term_base_offset
          = internal::read_one_varint_delta<model::offset>(
            in, reply.meta[i - 1].last_term_base_offset);
    }

    reply.meta[0].node_id = raft::vnode(
      node_id, varlong_reader<model::revision_id>(in));
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].node_id = raft::vnode(
          node_id,
          internal::read_one_varint_delta<model::revision_id>(
            in, reply.meta[i - 1].node_id.revision()));
    }

    reply.meta[0].target_node_id = raft::vnode(
      target_node_id, varlong_reader<model::revision_id>(in));
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].target_node_id = raft::vnode(
          target_node_id,
          internal::read_one_varint_delta<model::revision_id>(
            in, reply.meta[i - 1].target_node_id.revision()));
    }

    for (size_t i = 0; i < size; ++i) {
        reply.meta[i].result = read_nested<raft::append_entries_reply::status>(
          in, 0U);
    }

    for (auto& m : reply.meta) {
        m.last_flushed_log_index = decode_signed(m.last_flushed_log_index);
        m.last_dirty_log_index = decode_signed(m.last_dirty_log_index);
        m.last_term_base_offset = decode_signed(m.last_term_base_offset);
        m.node_id = raft::vnode(
          m.node_id.id(), decode_signed(m.node_id.revision()));
        m.target_node_id = raft::vnode(
          m.target_node_id.id(), decode_signed(m.target_node_id.revision()));
    }
}

ss::future<> append_entries_request::serde_async_write(iobuf& dst) {
    auto mem_batches = co_await model::consume_reader_to_memory(
      std::move(batches()), model::no_timeout);

    iobuf out;
    using serde::write;

    write(out, static_cast<uint32_t>(mem_batches.size()));
    for (auto& batch : mem_batches) {
        // intentionally using reflection here for batches which are not yet
        // supported with serde, but also have largely solidified.
        reflection::serialize(out, std::move(batch));
        co_await ss::coroutine::maybe_yield();
    }

    write(out, node_id);
    write(out, target_node_id);
    write(out, meta);
    write(out, flush);

    write(dst, std::move(out));
}

ss::future<> append_entries_request::serde_async_read(
  iobuf_parser& src, const serde::header hdr) {
    using serde::read_nested;
    auto tmp = read_nested<iobuf>(src, hdr._bytes_left_limit);
    iobuf_parser in(std::move(tmp));

    auto batch_count = read_nested<uint32_t>(in, 0U);
    auto batches = ss::circular_buffer<model::record_batch>{};
    batches.reserve(batch_count);
    for (uint32_t i = 0; i < batch_count; ++i) {
        batches.push_back(reflection::adl<model::record_batch>{}.from(in));
        co_await ss::coroutine::maybe_yield();
    }

    _batches = model::make_memory_record_batch_reader(std::move(batches));
    node_id = read_nested<raft::vnode>(in, 0U);
    target_node_id = read_nested<raft::vnode>(in, 0U);
    meta = read_nested<raft::protocol_metadata>(in, 0U);
    flush = read_nested<raft::append_entries_request::flush_after_append>(
      in, 0U);
}

} // namespace raft

namespace reflection {

void adl<raft::protocol_metadata>::to(
  iobuf& out, raft::protocol_metadata request) {
    std::array<bytes::value_type, 6 * vint::max_length> staging{};
    auto idx = vint::serialize(request.group(), staging.data());
    idx += vint::serialize(request.commit_index(), staging.data() + idx);
    idx += vint::serialize(request.term(), staging.data() + idx);

    // varint the delta-encoded value
    idx += vint::serialize(request.prev_log_index(), staging.data() + idx);
    idx += vint::serialize(request.prev_log_term(), staging.data() + idx);
    idx += vint::serialize(request.last_visible_index(), staging.data() + idx);

    out.append(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(staging.data()),
      idx);
}

raft::protocol_metadata adl<raft::protocol_metadata>::from(iobuf_parser& in) {
    raft::protocol_metadata ret;
    ret.group = varlong_reader<raft::group_id>(in);
    ret.commit_index = varlong_reader<model::offset>(in);
    ret.term = varlong_reader<model::term_id>(in);
    ret.prev_log_index = varlong_reader<model::offset>(in);
    ret.prev_log_term = varlong_reader<model::term_id>(in);
    ret.last_visible_index = varlong_reader<model::offset>(in);
    return ret;
}

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

    auto cfg = adl<raft::group_configuration>{}.from(in);
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
      out,
      md.last_included_index,
      md.last_included_term,
      md.version,
      md.latest_configuration,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        md.cluster_time.time_since_epoch()),
      md.log_start_delta);
}
} // namespace reflection
