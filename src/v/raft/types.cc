// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/types.h"

#include "model/fundamental.h"
#include "raft/consensus_utils.h"
#include "reflection/adl.h"
#include "utils/to_string.h"
#include "vassert.h"
#include "vlog.h"

#include <fmt/ostream.h>

#include <type_traits>

namespace raft {

std::ostream& operator<<(std::ostream& o, const append_entries_reply& r) {
    return o << "{node_id: " << r.node_id << ", group: " << r.group
             << ", term:" << r.term
             << ", last_dirty_log_index:" << r.last_dirty_log_index
             << ", last_committed_log_index:" << r.last_committed_log_index
             << ", last_term_base_offset:" << r.last_term_base_offset
             << ", result: " << r.result << "}";
}

std::ostream& operator<<(std::ostream& o, const vote_request& r) {
    return o << "{node_id: " << r.node_id << ", group: " << r.group
             << ", term:" << r.term << ", prev_log_index:" << r.prev_log_index
             << ", prev_log_term: " << r.prev_log_term
             << ", leadership_xfer: " << r.leadership_transfer << "}";
}
std::ostream& operator<<(std::ostream& o, const follower_index_metadata& i) {
    return o << "{node_id: " << i.node_id
             << ", last_committed_log_idx: " << i.last_committed_log_index
             << ", last_dirty_log_idx: " << i.last_dirty_log_index
             << ", match_index: " << i.match_index
             << ", next_index: " << i.next_index
             << ", is_learner: " << i.is_learner
             << ", is_recovering: " << i.is_recovering << "}";
}

std::ostream& operator<<(std::ostream& o, const heartbeat_request& r) {
    o << "{node: " << r.node_id << ", meta:(" << r.meta.size() << ") [";
    for (auto& m : r.meta) {
        o << m << ",";
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
             << ", prev_log_term:" << m.prev_log_term << "}";
}
std::ostream& operator<<(std::ostream& o, const vote_reply& r) {
    return o << "{term:" << r.term << ", vote_granted: " << r.granted
             << ", log_ok:" << r.log_ok << "}";
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
    default:
        return o << "uknown append_entries_reply::status";
    }
}

std::ostream& operator<<(std::ostream& o, const install_snapshot_request& r) {
    fmt::print(
      o,
      "{{term: {}, group: {}, node_id: {}, last_included_index: {}, "
      "file_offset: {}, chunk_size: {}, done: {}}}",
      r.term,
      r.group,
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
      "{{term: {}, bytes_stored: {}, success: {}}}",
      r.term,
      r.bytes_stored,
      r.success);
    return o;
}

} // namespace raft

namespace reflection {

struct rpc_model_reader_consumer {
    explicit rpc_model_reader_consumer(iobuf& oref)
      : ref(oref) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        reflection::serialize(ref, batch.header());
        if (!batch.compressed()) {
            reflection::serialize<int8_t>(ref, 0);
            batch.for_each_record([this](model::record r) {
                reflection::serialize(ref, std::move(r));
            });
        } else {
            reflection::serialize<int8_t>(ref, 1);
            reflection::serialize(ref, std::move(batch).release_data());
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    void end_of_stream(){};
    iobuf& ref;
};

ss::future<> async_adl<raft::append_entries_request>::to(
  iobuf& out, raft::append_entries_request&& request) {
    return model::consume_reader_to_memory(
             std::move(request.batches), model::no_timeout)
      .then([&out, request = std::move(request)](
              ss::circular_buffer<model::record_batch> batches) {
          reflection::adl<uint32_t>{}.to(out, batches.size());
          for (auto& batch : batches) {
              reflection::serialize(out, std::move(batch));
          }
          reflection::serialize(
            out, request.meta, request.node_id, request.flush);
      });
}

ss::future<raft::append_entries_request>
async_adl<raft::append_entries_request>::from(iobuf_parser& in) {
    auto batchCount = reflection::adl<uint32_t>{}.from(in);
    auto batches = ss::circular_buffer<model::record_batch>{};
    batches.reserve(batchCount);
    for (uint32_t i = 0; i < batchCount; ++i) {
        batches.push_back(adl<model::record_batch>{}.from(in));
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    auto meta = reflection::adl<raft::protocol_metadata>{}.from(in);
    auto n = reflection::adl<model::node_id>{}.from(in);
    auto flush
      = reflection::adl<raft::append_entries_request::flush_after_append>{}
          .from(in);
    raft::append_entries_request ret(n, meta, std::move(reader), flush);
    return ss::make_ready_future<raft::append_entries_request>(std::move(ret));
}

void adl<raft::protocol_metadata>::to(
  iobuf& out, raft::protocol_metadata request) {
    std::array<bytes::value_type, 5 * vint::max_length> staging{};
    auto idx = vint::serialize(request.group(), staging.data());
    idx += vint::serialize(request.commit_index(), staging.data() + idx);
    idx += vint::serialize(request.term(), staging.data() + idx);

    // varint the delta-encoded value
    idx += vint::serialize(request.prev_log_index(), staging.data() + idx);
    idx += vint::serialize(request.prev_log_term(), staging.data() + idx);

    out.append(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(staging.data()),
      idx);
}

template<typename T>
T varlong_reader(iobuf_parser& in) {
    auto [val, len] = in.read_varlong();
    return T(val);
}

raft::protocol_metadata adl<raft::protocol_metadata>::from(iobuf_parser& in) {
    raft::protocol_metadata ret;
    ret.group = varlong_reader<raft::group_id>(in);
    ret.commit_index = varlong_reader<model::offset>(in);
    ret.term = varlong_reader<model::term_id>(in);
    ret.prev_log_index = varlong_reader<model::offset>(in);
    ret.prev_log_term = varlong_reader<model::term_id>(in);
    return ret;
}
namespace internal {
struct hbeat_soa {
    explicit hbeat_soa(size_t n)
      : groups(n)
      , commit_indices(n)
      , terms(n)
      , prev_log_indices(n)
      , prev_log_terms(n) {}
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
};

struct hbeat_response_array {
    explicit hbeat_response_array(size_t n)
      : groups(n)
      , terms(n)
      , last_committed_log_index(n)
      , last_dirty_log_index(n)
      , last_term_base_offset(n) {}

    std::vector<raft::group_id> groups;
    std::vector<model::term_id> terms;
    std::vector<model::offset> last_committed_log_index;
    std::vector<model::offset> last_dirty_log_index;
    std::vector<model::offset> last_term_base_offset;
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

ss::future<> async_adl<raft::heartbeat_request>::to(
  iobuf& out, raft::heartbeat_request&& request) {
    struct sorter_fn {
        constexpr bool operator()(
          const raft::protocol_metadata& lhs,
          const raft::protocol_metadata& rhs) const {
            return lhs.commit_index < rhs.commit_index;
        }
    };
    std::sort(request.meta.begin(), request.meta.end(), sorter_fn{});
    return ss::make_ready_future<>()
      .then([&out, request = std::move(request)] {
          internal::hbeat_soa encodee(request.meta.size());
          const size_t size = request.meta.size();
          for (size_t i = 0; i < size; ++i) {
              const auto& m = request.meta[i];
              vassert(
                m.group() >= 0, "Negative raft group detected. {}", m.group);
              encodee.groups[i] = m.group;
              encodee.commit_indices[i] = std::max(
                model::offset(-1), m.commit_index);
              encodee.terms[i] = std::max(model::term_id(-1), m.term);
              encodee.prev_log_indices[i] = std::max(
                model::offset(-1), m.prev_log_index);
              encodee.prev_log_terms[i] = std::max(
                model::term_id(-1), m.prev_log_term);
          }
          // important to release this memory after this function
          // request.meta = {}; // release memory
          adl<model::node_id>{}.to(out, request.node_id);
          adl<uint32_t>{}.to(out, size);
          return encodee;
      })
      .then([&out](internal::hbeat_soa encodee) {
          internal::encode_one_delta_array<raft::group_id>(out, encodee.groups);
          internal::encode_one_delta_array<model::offset>(
            out, encodee.commit_indices);
          internal::encode_one_delta_array<model::term_id>(out, encodee.terms);
          internal::encode_one_delta_array<model::offset>(
            out, encodee.prev_log_indices);
          internal::encode_one_delta_array<model::term_id>(
            out, encodee.prev_log_terms);
      });
}

template<typename T>
T decode_signed(T value) {
    return value < T(0) ? T{} : value;
}

ss::future<raft::heartbeat_request>
async_adl<raft::heartbeat_request>::from(iobuf_parser& in) {
    raft::heartbeat_request req;
    req.node_id = adl<model::node_id>{}.from(in);
    req.meta = std::vector<raft::protocol_metadata>(adl<uint32_t>{}.from(in));
    if (req.meta.empty()) {
        return ss::make_ready_future<raft::heartbeat_request>(std::move(req));
    }
    const size_t max = req.meta.size();
    req.meta[0].group = varlong_reader<raft::group_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.meta[i].group = internal::read_one_varint_delta<raft::group_id>(
          in, req.meta[i - 1].group);
    }
    req.meta[0].commit_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < max; ++i) {
        req.meta[i].commit_index
          = internal::read_one_varint_delta<model::offset>(
            in, req.meta[i - 1].commit_index);
    }
    req.meta[0].term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.meta[i].term = internal::read_one_varint_delta<model::term_id>(
          in, req.meta[i - 1].term);
    }
    req.meta[0].prev_log_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < max; ++i) {
        req.meta[i].prev_log_index
          = internal::read_one_varint_delta<model::offset>(
            in, req.meta[i - 1].prev_log_index);
    }
    req.meta[0].prev_log_term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < max; ++i) {
        req.meta[i].prev_log_term
          = internal::read_one_varint_delta<model::term_id>(
            in, req.meta[i - 1].prev_log_term);
    }

    for (auto& m : req.meta) {
        m.prev_log_index = decode_signed(m.prev_log_index);
        m.commit_index = decode_signed(m.commit_index);
        m.prev_log_term = decode_signed(m.prev_log_term);
    }
    return ss::make_ready_future<raft::heartbeat_request>(std::move(req));
}

ss::future<> async_adl<raft::heartbeat_reply>::to(
  iobuf& out, raft::heartbeat_reply&& reply) {
    struct sorter_fn {
        constexpr bool operator()(
          const raft::append_entries_reply& lhs,
          const raft::append_entries_reply& rhs) const {
            return lhs.last_committed_log_index < rhs.last_committed_log_index;
        }
    };
    adl<uint32_t>{}.to(out, reply.meta.size());
    // no requests
    if (reply.meta.empty()) {
        return ss::make_ready_future<>();
    }

    // replies are comming from the same node
    adl<model::node_id>{}.to(out, reply.meta.front().node_id);

    std::sort(reply.meta.begin(), reply.meta.end(), sorter_fn{});
    internal::hbeat_response_array encodee(reply.meta.size());

    for (size_t i = 0; i < reply.meta.size(); ++i) {
        encodee.groups[i] = reply.meta[i].group;
        encodee.terms[i] = std::max(model::term_id(-1), reply.meta[i].term);

        encodee.last_committed_log_index[i] = std::max(
          model::offset(-1), reply.meta[i].last_committed_log_index);
        encodee.last_dirty_log_index[i] = std::max(
          model::offset(-1), reply.meta[i].last_dirty_log_index);
        encodee.last_term_base_offset[i] = std::max(
          model::offset(-1), reply.meta[i].last_term_base_offset);
    }
    internal::encode_one_delta_array<raft::group_id>(out, encodee.groups);
    internal::encode_one_delta_array<model::term_id>(out, encodee.terms);

    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_committed_log_index);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_dirty_log_index);
    internal::encode_one_delta_array<model::offset>(
      out, encodee.last_term_base_offset);
    for (auto& m : reply.meta) {
        adl<raft::append_entries_reply::status>{}.to(out, m.result);
    }
    return ss::make_ready_future<>();
}

ss::future<raft::heartbeat_reply>
async_adl<raft::heartbeat_reply>::from(iobuf_parser& in) {
    raft::heartbeat_reply reply;
    reply.meta = std::vector<raft::append_entries_reply>(
      adl<uint32_t>{}.from(in));

    // empty reply
    if (reply.meta.empty()) {
        return ss::make_ready_future<raft::heartbeat_reply>(std::move(reply));
    }

    auto node_id = adl<model::node_id>{}.from(in);

    size_t size = reply.meta.size();
    reply.meta[0].node_id = node_id;
    reply.meta[0].group = varlong_reader<raft::group_id>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].node_id = node_id;
        reply.meta[i].group = internal::read_one_varint_delta<raft::group_id>(
          in, reply.meta[i - 1].group);
    }
    reply.meta[0].term = varlong_reader<model::term_id>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].term = internal::read_one_varint_delta<model::term_id>(
          in, reply.meta[i - 1].term);
    }

    reply.meta[0].last_committed_log_index = varlong_reader<model::offset>(in);
    for (size_t i = 1; i < size; ++i) {
        reply.meta[i].last_committed_log_index
          = internal::read_one_varint_delta<model::offset>(
            in, reply.meta[i - 1].last_committed_log_index);
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

    for (size_t i = 0; i < size; ++i) {
        reply.meta[i].result = adl<raft::append_entries_reply::status>{}.from(
          in);
    }

    for (auto& m : reply.meta) {
        m.last_committed_log_index = decode_signed(m.last_committed_log_index);
        m.last_dirty_log_index = decode_signed(m.last_dirty_log_index);
        m.last_term_base_offset = decode_signed(m.last_term_base_offset);
    }

    return ss::make_ready_future<raft::heartbeat_reply>(std::move(reply));
}

raft::snapshot_metadata adl<raft::snapshot_metadata>::from(iobuf_parser& in) {
    auto last_included_index = adl<model::offset>{}.from(in);
    auto last_included_term = adl<model::term_id>{}.from(in);
    auto cfg = adl<raft::group_configuration>{}.from(in);
    ss::lowres_clock::time_point cluster_time{
      adl<ss::lowres_clock::duration>{}.from(in)};
    return raft::snapshot_metadata{
      .last_included_index = last_included_index,
      .last_included_term = last_included_term,
      .latest_configuration = std::move(cfg),
      .cluster_time = cluster_time};
}

void adl<raft::snapshot_metadata>::to(
  iobuf& out, raft::snapshot_metadata&& request) {
    reflection::serialize(
      out,
      request.last_included_index,
      request.last_included_term,
      request.latest_configuration,
      request.cluster_time.time_since_epoch());
}
} // namespace reflection
