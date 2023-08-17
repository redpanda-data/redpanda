// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"
#include "reflection/async_adl.h"

namespace reflection {

template<class T>
using fvec = fragmented_vector<T>;

using tx_snapshot = cluster::rm_stm::tx_snapshot;
using tx_snapshot_v3 = cluster::rm_stm::tx_snapshot_v3;
// note: tx_snapshot[v0-v2] cleaned up in 23.3.x

using tx_range = cluster::rm_stm::tx_range;
using prepare_marker = cluster::rm_stm::prepare_marker;
using abort_index = cluster::rm_stm::abort_index;
using seq_entry = cluster::rm_stm::seq_entry;

template<>
struct async_adl<tx_snapshot> {
    ss::future<> to(iobuf& out, tx_snapshot snap) {
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
        co_await detail::async_adl_list<fvec<seq_entry>>{}.to(
          out, std::move(snap.seqs));
        co_await detail::async_adl_list<fvec<tx_snapshot::tx_data_snapshot>>{}
          .to(out, std::move(snap.tx_data));
        co_await detail::async_adl_list<
          fvec<tx_snapshot::expiration_snapshot>>{}
          .to(out, std::move(snap.expiration));
    }

    ss::future<tx_snapshot> from(iobuf_parser& in) {
        auto fenced
          = co_await detail::async_adl_list<fvec<model::producer_identity>>{}
              .from(in);
        auto ongoing = co_await detail::async_adl_list<fvec<tx_range>>{}.from(
          in);
        auto prepared
          = co_await detail::async_adl_list<fvec<prepare_marker>>{}.from(in);
        auto aborted = co_await detail::async_adl_list<fvec<tx_range>>{}.from(
          in);
        auto abort_indexes
          = co_await detail::async_adl_list<fvec<abort_index>>{}.from(in);
        auto offset = reflection::adl<model::offset>{}.from(in);
        auto seqs = co_await detail::async_adl_list<fvec<seq_entry>>{}.from(in);
        auto tx_data = co_await detail::async_adl_list<
                         fvec<tx_snapshot::tx_data_snapshot>>{}
                         .from(in);
        auto expiration = co_await detail::async_adl_list<
                            fvec<tx_snapshot::expiration_snapshot>>{}
                            .from(in);

        co_return tx_snapshot{
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
};

template<>
struct async_adl<tx_snapshot_v3> {
    ss::future<> to(iobuf& out, tx_snapshot_v3 snap) {
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
        co_await detail::async_adl_list<fvec<seq_entry>>{}.to(
          out, std::move(snap.seqs));
        co_await detail::async_adl_list<
          fvec<tx_snapshot_v3::tx_seqs_snapshot>>{}
          .to(out, std::move(snap.tx_seqs));
        co_await detail::async_adl_list<
          fvec<tx_snapshot::expiration_snapshot>>{}
          .to(out, std::move(snap.expiration));
    }

    ss::future<tx_snapshot_v3> from(iobuf_parser& in) {
        auto fenced
          = co_await detail::async_adl_list<fvec<model::producer_identity>>{}
              .from(in);
        auto ongoing = co_await detail::async_adl_list<fvec<tx_range>>{}.from(
          in);
        auto prepared
          = co_await detail::async_adl_list<fvec<prepare_marker>>{}.from(in);
        auto aborted = co_await detail::async_adl_list<fvec<tx_range>>{}.from(
          in);
        auto abort_indexes
          = co_await detail::async_adl_list<fvec<abort_index>>{}.from(in);
        auto offset = reflection::adl<model::offset>{}.from(in);
        auto seqs = co_await detail::async_adl_list<fvec<seq_entry>>{}.from(in);
        auto tx_seqs = co_await detail::async_adl_list<
                         fvec<tx_snapshot_v3::tx_seqs_snapshot>>{}
                         .from(in);
        auto expiration = co_await detail::async_adl_list<
                            fvec<tx_snapshot::expiration_snapshot>>{}
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
};

}; // namespace reflection
