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

std::ostream& operator<<(std::ostream& o, const tx_snapshot_v6& snapshot) {
    fmt::print(
      o,
      "{{ version: {}, producers: {}, aborted transactions: {}, abort indexes: "
      "{} }}",
      tx_snapshot_v6::version,
      snapshot.producers.size(),
      snapshot.aborted.size(),
      snapshot.abort_indexes.size());
    return o;
}

}; // namespace cluster::tx

namespace reflection {

using namespace cluster::tx;

ss::future<> async_adl<abort_index>::to(iobuf& out, abort_index t) {
    reflection::serialize(out, t.first, t.last);
    co_return;
}

ss::future<abort_index> async_adl<abort_index>::from(iobuf_parser& in) {
    abort_index result;
    result.first = adl<model::offset>{}.from(in);
    result.last = adl<model::offset>{}.from(in);
    co_return result;
}

void adl<model::tx_range>::to(iobuf& out, model::tx_range t) {
    reflection::serialize(out, t.pid, t.first, t.last);
}

model::tx_range adl<model::tx_range>::from(iobuf_parser& in) {
    model::tx_range result;
    result.pid = adl<model::producer_identity>{}.from(in);
    result.first = adl<model::offset>{}.from(in);
    result.last = adl<model::offset>{}.from(in);
    return result;
}

ss::future<> async_adl<model::tx_range>::to(iobuf& out, model::tx_range t) {
    adl<model::tx_range>{}.to(out, t);
    co_return;
}

ss::future<model::tx_range> async_adl<model::tx_range>::from(iobuf_parser& in) {
    co_return adl<model::tx_range>{}.from(in);
}

} // namespace reflection
