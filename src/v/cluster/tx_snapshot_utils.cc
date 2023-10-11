// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_snapshot_utils.h"

namespace cluster {

bool deprecated_seq_entry::operator==(const deprecated_seq_entry& other) const {
    if (this == &other) {
        return true;
    }
    return pid == other.pid && seq == other.seq
           && last_offset == other.last_offset
           && last_write_timestamp == other.last_write_timestamp
           && std::equal(
             seq_cache.begin(),
             seq_cache.end(),
             other.seq_cache.begin(),
             other.seq_cache.end());
}

deprecated_seq_entry deprecated_seq_entry::from_producer_state_snapshot(
  producer_state_snapshot& state) {
    deprecated_seq_entry entry;
    entry.pid = state._id;
    if (!state._finished_requests.empty()) {
        const auto& last = state._finished_requests.back();
        entry.seq = last._last_sequence;
        entry.last_offset = last._last_offset;
        for (const auto& req : state._finished_requests) {
            entry.seq_cache.emplace_back(req._last_sequence, req._last_offset);
        }
        entry.last_write_timestamp = model::timestamp::now().value();
    }
    return entry;
}

tx_snapshot::tx_snapshot(tx_snapshot_v4 snap_v4, raft::group_id group)
  : offset(snap_v4.offset)
  , fenced(std::move(snap_v4.fenced))
  , ongoing(std::move(snap_v4.ongoing))
  , prepared(std::move(snap_v4.prepared))
  , aborted(std::move(snap_v4.aborted))
  , abort_indexes(std::move(snap_v4.abort_indexes))
  , tx_data(std::move(snap_v4.tx_data))
  , expiration(std::move(snap_v4.expiration)) {
    for (auto& entry : snap_v4.seqs) {
        cluster::producer_state_snapshot snapshot;
        snapshot._id = entry.pid;
        snapshot._group = group;
        auto duration = model::timestamp_clock::duration{
          (model::timestamp::now()
           - model::timestamp{entry.last_write_timestamp})
            .value()};
        snapshot._ms_since_last_update
          = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        // there is an incompatibility with old version of snapshot here.
        // older version only saved last_seq for each sequence range, but
        // the new format saves [first last] pairs. For the first sequence
        // of first pair, we just assume it is 0. This is not a correctness
        // problem and should not even be a problem once the cluster fully
        // upgrades.
        auto prev_last = -1;
        for (auto& req : entry.seq_cache) {
            cluster::producer_state_snapshot::finished_request request;
            request._first_sequence = prev_last + 1;
            request._last_sequence = req.seq;
            request._last_offset = req.offset;
            snapshot._finished_requests.push_back(std::move(request));
            prev_last = req.seq;
        }
        producers.push_back(std::move(snapshot));
    }
}
} // namespace cluster

namespace reflection {

template<class T>
using fvec = fragmented_vector<T>;

ss::future<> async_adl<tx_snapshot_v3>::to(iobuf& out, tx_snapshot_v3 snap) {
    co_await detail::async_adl_list<
      fragmented_vector<model::producer_identity>>{}
      .to(out, std::move(snap.fenced));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.ongoing));
    co_await detail::async_adl_list<fvec<prepare_marker>>{}.to(
      out, std::move(snap.prepared));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.aborted));
    co_await detail::async_adl_list<fvec<abort_index>>{}.to(
      out, std::move(snap.abort_indexes));
    reflection::serialize(out, snap.offset);
    co_await detail::async_adl_list<fvec<cluster::deprecated_seq_entry>>{}.to(
      out, std::move(snap.seqs));
    co_await detail::async_adl_list<fvec<tx_snapshot_v3::tx_seqs_snapshot>>{}
      .to(out, std::move(snap.tx_seqs));
    co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}.to(
      out, std::move(snap.expiration));
}

ss::future<tx_snapshot_v3> async_adl<tx_snapshot_v3>::from(iobuf_parser& in) {
    auto fenced
      = co_await detail::async_adl_list<fvec<model::producer_identity>>{}.from(
        in);
    auto ongoing = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    auto prepared
      = co_await detail::async_adl_list<fvec<prepare_marker>>{}.from(in);
    auto aborted = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    auto abort_indexes
      = co_await detail::async_adl_list<fvec<abort_index>>{}.from(in);
    auto offset = reflection::adl<model::offset>{}.from(in);
    auto seqs
      = co_await detail::async_adl_list<fvec<cluster::deprecated_seq_entry>>{}
          .from(in);
    auto tx_seqs = co_await detail::async_adl_list<
                     fvec<tx_snapshot_v3::tx_seqs_snapshot>>{}
                     .from(in);
    auto expiration
      = co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}
          .from(in);

    co_return tx_snapshot_v3{
      .fenced = std::move(fenced),
      .ongoing = std::move(ongoing),
      .prepared = std::move(prepared),
      .aborted = std::move(aborted),
      .abort_indexes = std::move(abort_indexes),
      .offset = offset,
      .seqs = std::move(seqs),
      .tx_seqs = std::move(tx_seqs),
      .expiration = std::move(expiration)};
}

ss::future<> async_adl<tx_snapshot_v4>::to(iobuf& out, tx_snapshot_v4 snap) {
    co_await detail::async_adl_list<
      fragmented_vector<model::producer_identity>>{}
      .to(out, std::move(snap.fenced));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.ongoing));
    co_await detail::async_adl_list<fvec<prepare_marker>>{}.to(
      out, std::move(snap.prepared));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.aborted));
    co_await detail::async_adl_list<fvec<abort_index>>{}.to(
      out, std::move(snap.abort_indexes));
    reflection::serialize(out, snap.offset);
    co_await detail::async_adl_list<fvec<cluster::deprecated_seq_entry>>{}.to(
      out, std::move(snap.seqs));
    co_await detail::async_adl_list<fvec<cluster::tx_data_snapshot>>{}.to(
      out, std::move(snap.tx_data));
    co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}.to(
      out, std::move(snap.expiration));
}

ss::future<tx_snapshot_v4> async_adl<tx_snapshot_v4>::from(iobuf_parser& in) {
    auto fenced
      = co_await detail::async_adl_list<fvec<model::producer_identity>>{}.from(
        in);
    auto ongoing = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    auto prepared
      = co_await detail::async_adl_list<fvec<prepare_marker>>{}.from(in);
    auto aborted = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    auto abort_indexes
      = co_await detail::async_adl_list<fvec<abort_index>>{}.from(in);
    auto offset = reflection::adl<model::offset>{}.from(in);
    auto seqs
      = co_await detail::async_adl_list<fvec<cluster::deprecated_seq_entry>>{}
          .from(in);
    auto tx_data
      = co_await detail::async_adl_list<fvec<cluster::tx_data_snapshot>>{}.from(
        in);
    auto expiration
      = co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}
          .from(in);

    co_return tx_snapshot_v4{
      .fenced = std::move(fenced),
      .ongoing = std::move(ongoing),
      .prepared = std::move(prepared),
      .aborted = std::move(aborted),
      .abort_indexes = std::move(abort_indexes),
      .offset = offset,
      .seqs = std::move(seqs),
      .tx_data = std::move(tx_data),
      .expiration = std::move(expiration)};
}

ss::future<> async_adl<tx_snapshot>::to(iobuf& out, tx_snapshot snap) {
    reflection::serialize(out, snap.offset);
    co_await detail::async_adl_list<fvec<cluster::producer_state_snapshot>>{}
      .to(out, std::move(snap.producers));
    co_await detail::async_adl_list<
      fragmented_vector<model::producer_identity>>{}
      .to(out, std::move(snap.fenced));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.ongoing));
    co_await detail::async_adl_list<fvec<prepare_marker>>{}.to(
      out, std::move(snap.prepared));
    co_await detail::async_adl_list<fvec<tx_range>>{}.to(
      out, std::move(snap.aborted));
    co_await detail::async_adl_list<fvec<abort_index>>{}.to(
      out, std::move(snap.abort_indexes));
    co_await detail::async_adl_list<fvec<cluster::tx_data_snapshot>>{}.to(
      out, std::move(snap.tx_data));
    co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}.to(
      out, std::move(snap.expiration));
}

ss::future<tx_snapshot> async_adl<tx_snapshot>::from(iobuf_parser& in) {
    tx_snapshot result;
    result.offset = reflection::adl<model::offset>{}.from(in);
    result.producers = co_await detail::async_adl_list<
                         fvec<cluster::producer_state_snapshot>>{}
                         .from(in);
    result.fenced
      = co_await detail::async_adl_list<fvec<model::producer_identity>>{}.from(
        in);
    result.ongoing = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    result.prepared
      = co_await detail::async_adl_list<fvec<prepare_marker>>{}.from(in);
    result.aborted = co_await detail::async_adl_list<fvec<tx_range>>{}.from(in);
    result.abort_indexes
      = co_await detail::async_adl_list<fvec<abort_index>>{}.from(in);
    result.tx_data
      = co_await detail::async_adl_list<fvec<cluster::tx_data_snapshot>>{}.from(
        in);
    result.expiration
      = co_await detail::async_adl_list<fvec<cluster::expiration_snapshot>>{}
          .from(in);
    co_return result;
}

}; // namespace reflection
