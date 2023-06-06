// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/adl_serde.h"

#include "model/record.h"

#include <seastar/core/smp.hh>

namespace reflection {

void adl<model::topic>::to(iobuf& out, model::topic&& t) {
    auto str = ss::sstring(t);
    reflection::serialize(out, str);
}

model::topic adl<model::topic>::from(iobuf_parser& in) {
    auto str = adl<ss::sstring>{}.from(in);
    return model::topic(std::move(str));
}

void adl<model::ns>::to(iobuf& out, model::ns&& t) {
    auto str = ss::sstring(t);
    reflection::serialize(out, str);
}

model::ns adl<model::ns>::from(iobuf_parser& in) {
    auto str = adl<ss::sstring>{}.from(in);
    return model::ns(std::move(str));
}

void adl<model::topic_partition>::to(iobuf& out, model::topic_partition&& t) {
    auto str = ss::sstring(t.topic);
    reflection::serialize(out, std::move(str), t.partition);
}

model::topic_partition adl<model::topic_partition>::from(iobuf_parser& in) {
    auto topic = model::topic(adl<ss::sstring>{}.from(in));
    auto id = adl<model::partition_id>{}.from(in);
    return model::topic_partition{std::move(topic), id};
}

void adl<model::ntp>::to(iobuf& out, model::ntp&& ntp) {
    reflection::serialize(out, ntp.ns, std::move(ntp.tp));
}

model::ntp adl<model::ntp>::from(iobuf_parser& in) {
    auto ns = adl<model::ns>{}.from(in);
    auto partition = adl<model::topic_partition>{}.from(in);
    return model::ntp(std::move(ns), std::move(partition));
}

void adl<net::unresolved_address>::to(
  iobuf& out, const net::unresolved_address& address) {
    adl<ss::sstring>{}.to(out, address.host());
    adl<uint16_t>{}.to(out, address.port());
}

net::unresolved_address adl<net::unresolved_address>::from(iobuf_parser& in) {
    auto host = adl<ss::sstring>{}.from(in);
    auto port = adl<uint16_t>{}.from(in);
    return net::unresolved_address(std::move(host), port);
}

void adl<model::broker_properties>::to(
  iobuf& out, const model::broker_properties& prop) {
    adl<uint32_t>{}.to(out, prop.cores);
    adl<uint32_t>{}.to(out, prop.available_memory_gb);
    adl<uint32_t>{}.to(out, prop.available_disk_gb);
    adl<std::vector<ss::sstring>>{}.to(out, prop.mount_paths);
    type vec;
    vec.reserve(prop.etc_props.size());
    std::move(
      std::begin(prop.etc_props),
      std::end(prop.etc_props),
      std::back_inserter(vec));
    serialize(out, std::move(vec));
}

model::broker_properties adl<model::broker_properties>::from(iobuf_parser& in) {
    auto cores = adl<uint32_t>{}.from(in);
    auto mem = adl<uint32_t>{}.from(in);
    auto disk = adl<uint32_t>{}.from(in);
    auto paths = adl<std::vector<ss::sstring>>{}.from(in);
    auto vec = adl<type>{}.from(in);
    auto props = std::unordered_map<ss::sstring, ss::sstring>(
      vec.begin(), vec.end());
    return model::broker_properties{
      .cores = cores,
      .available_memory_gb = mem,
      .available_disk_gb = disk,
      .mount_paths = std::move(paths),
      .etc_props = std::move(props)};
}

model::broker_endpoint adl<model::broker_endpoint>::from(iobuf_parser& in) {
    auto name = adl<ss::sstring>{}.from(in);
    auto address = adl<net::unresolved_address>{}.from(in);
    return {std::move(name), address};
}

void adl<model::broker_endpoint>::to(iobuf& out, model::broker_endpoint&& ep) {
    adl<ss::sstring>{}.to(out, ep.name);
    adl<net::unresolved_address>{}.to(out, ep.address);
}

void adl<model::broker>::to(iobuf& out, model::broker&& r) {
    adl<model::node_id>{}.to(out, r.id());
    adl<std::vector<model::broker_endpoint>>{}.to(
      out, r.kafka_advertised_listeners());
    adl<net::unresolved_address>{}.to(out, r.rpc_address());
    adl<std::optional<ss::sstring>>{}.to(out, r.rack());
    adl<model::broker_properties>{}.to(out, r.properties());
}

model::broker adl<model::broker>::from(iobuf_parser& in) {
    auto id = adl<model::node_id>{}.from(in);
    auto kafka_adrs = adl<std::vector<model::broker_endpoint>>{}.from(in);
    auto rpc_adrs = adl<net::unresolved_address>{}.from(in);
    auto rack = adl<std::optional<model::rack_id>>{}.from(in);
    auto etc_props = adl<model::broker_properties>{}.from(in);
    return model::broker{id, std::move(kafka_adrs), rpc_adrs, rack, etc_props};
}

void adl<model::internal::broker_v0>::to(
  iobuf& out, model::internal::broker_v0&& r) {
    adl<model::node_id>{}.to(out, r.id);
    adl<net::unresolved_address>{}.to(out, r.kafka_address);
    adl<net::unresolved_address>{}.to(out, r.rpc_address);
    adl<std::optional<model::rack_id>>{}.to(out, r.rack);
    adl<model::broker_properties>{}.to(out, r.properties);
}

model::internal::broker_v0
adl<model::internal::broker_v0>::from(iobuf_parser& in) {
    auto id = adl<model::node_id>{}.from(in);
    auto kafka_adrs = adl<net::unresolved_address>{}.from(in);
    auto rpc_adrs = adl<net::unresolved_address>{}.from(in);
    auto rack = adl<std::optional<model::rack_id>>{}.from(in);
    auto etc_props = adl<model::broker_properties>{}.from(in);
    return model::internal::broker_v0{
      id, kafka_adrs, rpc_adrs, rack, etc_props};
}

void adl<model::record>::to(iobuf& ref, model::record&& record) {
    reflection::serialize(
      ref,
      record.size_bytes(),
      record.attributes().value(),
      record.timestamp_delta(),
      record.offset_delta(),
      record.key_size(),
      record.share_key(),
      record.value_size(),
      record.share_value(),
      int32_t(record.headers().size()));
    for (auto& h : record.headers()) {
        reflection::serialize(
          ref, h.key_size(), h.share_key(), h.value_size(), h.share_value());
    }
}

model::record adl<model::record>::from(iobuf_parser& in) {
    auto sz_bytes = adl<uint32_t>{}.from(in);
    using attr_t = model::record_attributes::type;
    auto attributes = model::record_attributes(adl<attr_t>{}.from(in));
    auto timestamp = adl<int64_t>{}.from(in);
    auto offset_data = adl<int32_t>{}.from(in);
    auto key_len = adl<int32_t>{}.from(in);
    auto key = adl<iobuf>{}.from(in);
    auto value_len = adl<int32_t>{}.from(in);
    auto value = adl<iobuf>{}.from(in);
    auto hdr_size = adl<int32_t>{}.from(in);
    std::vector<model::record_header> headers;
    headers.reserve(hdr_size);
    for (int i = 0; i < hdr_size; ++i) {
        auto hkey_len = adl<int32_t>{}.from(in);
        auto hkey = adl<iobuf>{}.from(in);
        auto hvalue_len = adl<int32_t>{}.from(in);
        auto hvalue = adl<iobuf>{}.from(in);
        headers.emplace_back(model::record_header(
          hkey_len, std::move(hkey), hvalue_len, std::move(hvalue)));
    }
    return model::record(
      sz_bytes,
      attributes,
      timestamp,
      offset_data,
      key_len,
      std::move(key),
      value_len,
      std::move(value),
      std::move(headers));
}

void adl<model::record_batch_header>::to(
  iobuf& out, model::record_batch_header&& r) {
    reflection::serialize(
      out,
      r.header_crc,
      r.size_bytes,
      r.base_offset,
      r.type,
      r.crc,
      r.attrs.value(),
      r.last_offset_delta,
      r.first_timestamp.value(),
      r.max_timestamp.value(),
      r.producer_id,
      r.producer_epoch,
      r.base_sequence,
      r.record_count,
      r.ctx.term);
}

void adl<batch_header>::to(iobuf& out, batch_header&& header) {
    reflection::serialize(out, header.bhdr, header.is_compressed);
}

batch_header adl<batch_header>::from(iobuf_parser& in) {
    auto record = adl<model::record_batch_header>{}.from(in);
    auto is_compressed = adl<int8_t>{}.from(in);
    return batch_header{record, is_compressed};
}

void adl<model::record_batch>::to(iobuf& out, model::record_batch&& batch) {
    batch_header hdr{
      .bhdr = batch.header(),
      .is_compressed = static_cast<int8_t>(batch.compressed() ? 1 : 0)};
    reflection::serialize(out, hdr);
    if (batch.compressed()) {
        reflection::serialize(out, std::move(batch).release_data());
        return;
    }
    batch.for_each_record(
      [&out](model::record r) { reflection::serialize(out, std::move(r)); });
}

model::record_batch adl<model::record_batch>::from(iobuf_parser& in) {
    auto hdr = reflection::adl<batch_header>{}.from(in);
    if (hdr.is_compressed == 1) {
        auto io = reflection::adl<iobuf>{}.from(in);
        return model::record_batch(hdr.bhdr, std::move(io));
    }
    auto recs = std::vector<model::record>{};
    recs.reserve(hdr.bhdr.record_count);
    for (int i = 0; i < hdr.bhdr.record_count; ++i) {
        recs.push_back(adl<model::record>{}.from(in));
    }
    return model::record_batch(hdr.bhdr, std::move(recs));
}

void adl<model::partition_metadata>::to(
  iobuf& out, model::partition_metadata&& md) {
    reflection::serialize(out, md.id, std::move(md.replicas), md.leader_node);
}

model::partition_metadata
adl<model::partition_metadata>::from(iobuf_parser& in) {
    auto md = model::partition_metadata(
      reflection::adl<model::partition_id>{}.from(in));
    md.replicas = reflection::adl<std::vector<model::broker_shard>>{}.from(in);
    md.leader_node = reflection::adl<std::optional<model::node_id>>{}.from(in);
    return md;
}

void adl<model::topic_namespace>::to(
  iobuf& out, model::topic_namespace&& tp_ns) {
    reflection::serialize(out, std::move(tp_ns.ns), std::move(tp_ns.tp));
}

model::topic_namespace adl<model::topic_namespace>::from(iobuf_parser& in) {
    auto ns = reflection::adl<model::ns>{}.from(in);
    auto tp = reflection::adl<model::topic>{}.from(in);
    return model::topic_namespace(ns, tp);
}

void adl<model::topic_metadata>::to(iobuf& out, model::topic_metadata&& md) {
    reflection::serialize(out, md.tp_ns, std::move(md.partitions));
}

model::topic_metadata adl<model::topic_metadata>::from(iobuf_parser& in) {
    auto md = model::topic_metadata(
      reflection::adl<model::topic_namespace>{}.from(in));
    md.partitions
      = reflection::adl<std::vector<model::partition_metadata>>{}.from(in);
    return md;
}

model::producer_identity adl<model::producer_identity>::from(iobuf_parser& in) {
    auto id = reflection::adl<int64_t>{}.from(in);
    auto epoch = reflection::adl<int16_t>{}.from(in);
    return {id, epoch};
}

void adl<model::producer_identity>::to(
  iobuf& out, model::producer_identity&& p) {
    reflection::serialize(out, p.id, p.epoch);
}
} // namespace reflection
