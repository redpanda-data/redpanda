// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm_types.h"

#include "storage/record_batch_builder.h"

namespace cluster::tx {

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
        producer_state_snapshot snapshot;
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
            producer_state_snapshot::finished_request request;
            request._first_sequence = prev_last + 1;
            request._last_sequence = req.seq;
            request._last_offset = req.offset;
            snapshot._finished_requests.push_back(std::move(request));
            prev_last = req.seq;
        }
        producers.push_back(std::move(snapshot));
    }
}

bool producer_partition_transaction_state::is_in_progress() const {
    return status == partition_transaction_status::ongoing
           || status == partition_transaction_status::initialized;
}

std::ostream&
operator<<(std::ostream& o, const partition_transaction_status& status) {
    switch (status) {
    case partition_transaction_status::ongoing:
        o << "ongoing";
        break;
    case partition_transaction_status::initialized:
        o << "initialized";
        break;
    case partition_transaction_status::committed:
        o << "committed";
        break;
    case partition_transaction_status::aborted:
        o << "aborted";
        break;
    }
    return o;
}

ss::sstring partition_transaction_info::get_status() const {
    return fmt::format("{}", status);
}

bool partition_transaction_info::is_expired() const {
    return !info.has_value() || info.value().deadline() <= clock_type::now();
}

std::optional<duration_type> partition_transaction_info::get_staleness() const {
    if (is_expired()) {
        return std::nullopt;
    }

    auto now = ss::lowres_clock::now();
    return now - info->last_update;
}

std::optional<duration_type> partition_transaction_info::get_timeout() const {
    if (is_expired()) {
        return std::nullopt;
    }

    return info->timeout;
}

std::ostream& operator<<(std::ostream& o, const abort_snapshot& as) {
    fmt::print(
      o,
      "{{first: {}, last: {}, aborted tx count: {}}}",
      as.first,
      as.last,
      as.aborted.size());
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const producer_partition_transaction_state& tx_state) {
    fmt::print(
      o,
      "{{first: {}, last: {}, sequence: {}, timeout: {}, coordinator "
      "partition: {}, status: {} }}",
      tx_state.first,
      tx_state.last,
      tx_state.sequence,
      tx_state.timeout,
      tx_state.coordinator_partition,
      tx_state.status);
    return o;
}

model::record_batch make_fence_batch(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms,
  model::partition_id tm) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    // the key byte representation must not change because it's used in
    // compaction
    reflection::serialize(
      value, fence_control_record_version, tx_seq, transaction_timeout_ms, tm);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

fence_batch_data read_fence_batch(model::record_batch&& b) {
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);

    vassert(
      b.record_count() == 1,
      "model::record_batch_type::tx_fence batch must contain a single record");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto version = reflection::adl<int8_t>{}.from(val_reader);
    vassert(
      version <= fence_control_record_version,
      "unknown fence record version: {} expected: {}",
      version,
      fence_control_record_version);

    std::optional<model::tx_seq> tx_seq{};
    std::optional<std::chrono::milliseconds> transaction_timeout_ms;
    if (version >= fence_control_record_v1_version) {
        tx_seq = reflection::adl<model::tx_seq>{}.from(val_reader);
        transaction_timeout_ms
          = reflection::adl<std::chrono::milliseconds>{}.from(val_reader);
    }
    model::partition_id tm{model::legacy_tm_ntp.tp.partition};
    if (version >= fence_control_record_version) {
        tm = reflection::adl<model::partition_id>{}.from(val_reader);
    }

    auto key_buf = record.release_key();
    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    vassert(
      hdr.type == batch_type,
      "broken model::record_batch_type::tx_fence batch. expected batch type {} "
      "got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    vassert(
      p_id == bid.pid.id,
      "broken model::record_batch_type::tx_fence batch. expected pid {} got: "
      "{}",
      bid.pid.id,
      p_id);
    return fence_batch_data{bid, tx_seq, transaction_timeout_ms, tm};
}

model::control_record_type parse_control_batch(const model::record_batch& b) {
    const auto& hdr = b.header();
    vassert(
      hdr.type == model::record_batch_type::raft_data,
      "expect data batch type got {}",
      hdr.type);
    vassert(hdr.attrs.is_control(), "expect control attrs got {}", hdr.attrs);
    vassert(
      b.record_count() == 1, "control batch must contain a single record");

    auto r = b.copy_records();
    auto& record = *r.begin();
    auto key = record.release_key();
    kafka::protocol::decoder key_reader(std::move(key));
    auto version = model::control_record_version(key_reader.read_int16());
    vassert(
      version == model::current_control_record_version,
      "unknown control record version");
    return model::control_record_type(key_reader.read_int16());
}

model::record_batch make_tx_control_batch(
  model::producer_identity pid, model::control_record_type crt) {
    iobuf key;
    kafka::protocol::encoder kw(key);
    kw.write(model::current_control_record_version());
    kw.write(static_cast<int16_t>(crt));

    iobuf value;
    kafka::protocol::encoder vw(value);
    vw.write(static_cast<int16_t>(0));
    vw.write(static_cast<int32_t>(0));

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.set_transactional_type();
    builder.add_raw_kw(
      std::move(key), std::move(value), std::vector<model::record_header>());

    return std::move(builder).build();
}

}; // namespace cluster::tx

namespace reflection {

using namespace cluster::tx;

template<class T>
using fvec = fragmented_vector<T>;

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
    co_await detail::async_adl_list<fvec<deprecated_seq_entry>>{}.to(
      out, std::move(snap.seqs));
    co_await detail::async_adl_list<fvec<tx_data_snapshot>>{}.to(
      out, std::move(snap.tx_data));
    co_await detail::async_adl_list<fvec<expiration_snapshot>>{}.to(
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
      = co_await detail::async_adl_list<fvec<deprecated_seq_entry>>{}.from(in);
    auto tx_data
      = co_await detail::async_adl_list<fvec<tx_data_snapshot>>{}.from(in);
    auto expiration
      = co_await detail::async_adl_list<fvec<expiration_snapshot>>{}.from(in);

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
    co_await detail::async_adl_list<fvec<producer_state_snapshot>>{}.to(
      out, std::move(snap.producers));
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
    co_await detail::async_adl_list<fvec<tx_data_snapshot>>{}.to(
      out, std::move(snap.tx_data));
    co_await detail::async_adl_list<fvec<expiration_snapshot>>{}.to(
      out, std::move(snap.expiration));
    reflection::serialize(out, snap.highest_producer_id);
}

ss::future<tx_snapshot> async_adl<tx_snapshot>::from(iobuf_parser& in) {
    tx_snapshot result;
    result.offset = reflection::adl<model::offset>{}.from(in);
    result.producers
      = co_await detail::async_adl_list<fvec<producer_state_snapshot>>{}.from(
        in);
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
      = co_await detail::async_adl_list<fvec<tx_data_snapshot>>{}.from(in);
    result.expiration
      = co_await detail::async_adl_list<fvec<expiration_snapshot>>{}.from(in);
    result.highest_producer_id = reflection::adl<model::producer_id>{}.from(in);
    co_return result;
}

}; // namespace reflection
