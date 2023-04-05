// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "model/validation.h"
#include "serde/serde.h"
#include "utils/string_switch.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <fmt/ostream.h>

#include <iostream>
#include <type_traits>

namespace model {

std::ostream& operator<<(std::ostream& os, timestamp ts) {
    if (ts != timestamp::missing()) {
        fmt::print(os, "{{timestamp: {}}}", ts.value());
        return os;
    }
    return os << "{timestamp: missing}";
}

void read_nested(
  iobuf_parser& in, timestamp& ts, size_t const bytes_left_limit) {
    serde::read_nested(in, ts._v, bytes_left_limit);
}

void write(iobuf& out, timestamp ts) { serde::write(out, ts._v); }

std::ostream& operator<<(std::ostream& os, const topic_partition& tp) {
    fmt::print(os, "{{topic_partition: {}:{}}}", tp.topic, tp.partition);
    return os;
}

std::ostream& operator<<(std::ostream& os, const ntp& n) {
    fmt::print(os, "{{{}/{}/{}}}", n.ns(), n.tp.topic(), n.tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& o, const model::topic_namespace& tp_ns) {
    fmt::print(o, "{{ns: {}, topic: {}}}", tp_ns.ns, tp_ns.tp);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const model::topic_namespace_view& tp_ns) {
    fmt::print(o, "{{ns: {}, topic: {}}}", tp_ns.ns, tp_ns.tp);
    return o;
}

std::ostream& operator<<(std::ostream& os, timestamp_type ts) {
    /**
     * We need to use specific string representations of timestamp_type as this
     * is related with protocol correctness
     */
    switch (ts) {
    case timestamp_type::append_time:
        return os << "LogAppendTime";
    case timestamp_type::create_time:
        return os << "CreateTime";
    }
    return os << "{unknown timestamp:" << static_cast<int>(ts) << "}";
}

std::ostream& operator<<(std::ostream& o, const record_header& h) {
    return o << "{key_size=" << h.key_size() << ", key=" << h.key()
             << ", value_size=" << h.value_size() << ", value=" << h.value()
             << "}";
}

std::ostream& operator<<(std::ostream& o, const record_attributes& a) {
    return o << "{" << a._attributes << "}";
}
std::ostream& operator<<(std::ostream& o, const record& r) {
    o << "{record: size_bytes=" << r.size_bytes()
      << ", attributes=" << r.attributes()
      << ", timestamp_delta=" << r._timestamp_delta
      << ", offset_delta=" << r._offset_delta << ", key_size=" << r._key_size
      << ", key=" << r.key() << ", value_size=" << r.value_size()
      << ", value=" << r.value() << ", header_size:" << r.headers().size()
      << ", headers=[";

    for (auto& h : r.headers()) {
        o << h;
    }
    return o << "]}";
}

std::ostream& operator<<(std::ostream& o, const producer_identity& pid) {
    fmt::print(o, "{{producer_identity: id={}, epoch={}}}", pid.id, pid.epoch);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const record_batch_attributes& attrs) {
    o << "{compression:";
    if (attrs.is_valid_compression()) {
        // this method... sadly, just throws
        o << attrs.compression();
    } else {
        o << "invalid compression";
    }
    return o << ", type:" << attrs.timestamp_type()
             << ", transactional: " << attrs.is_transactional()
             << ", control: " << attrs.is_control() << "}";
}

std::ostream& operator<<(std::ostream& o, const record_batch_header& h) {
    o << "{header_crc:" << h.header_crc << ", size_bytes:" << h.size_bytes
      << ", base_offset:" << h.base_offset << ", type:" << h.type
      << ", crc:" << h.crc << ", attrs:" << h.attrs
      << ", last_offset_delta:" << h.last_offset_delta
      << ", first_timestamp:" << h.first_timestamp
      << ", max_timestamp:" << h.max_timestamp
      << ", producer_id:" << h.producer_id
      << ", producer_epoch:" << h.producer_epoch
      << ", base_sequence:" << h.base_sequence
      << ", record_count:" << h.record_count;
    o << ", ctx:{term:" << h.ctx.term << ", owner_shard:";
    if (h.ctx.owner_shard) {
        o << h.ctx.owner_shard << "}";
    } else {
        o << "nullopt}";
    }
    o << "}";
    return o;
}

std::ostream&
operator<<(std::ostream& os, const record_batch::compressed_records& records) {
    fmt::print(
      os, "{{compressed_records: size_bytes={}}}", records.size_bytes());
    return os;
}

std::ostream& operator<<(std::ostream& os, const record_batch& batch) {
    os << "{record_batch=" << batch.header() << ", records=";
    if (batch.compressed()) {
        os << "{compressed=" << batch.data().size_bytes() << " bytes}";
    } else {
        os << "{";
        batch.for_each_record([&os](const model::record& r) { os << r; });
        os << "}";
    }
    os << "}";
    return os;
}

ss::sstring ntp::path() const {
    return ssx::sformat("{}/{}/{}", ns(), tp.topic(), tp.partition());
}

std::filesystem::path ntp::topic_path() const {
    return fmt::format("{}/{}", ns(), tp.topic());
}

std::istream& operator>>(std::istream& i, compression& c) {
    ss::sstring s;
    i >> s;
    c = string_switch<compression>(s)
          .match_all("none", "uncompressed", compression::none)
          .match("gzip", compression::gzip)
          .match("snappy", compression::snappy)
          .match("lz4", compression::lz4)
          .match("zstd", compression::zstd)
          .match("producer", compression::producer);
    return i;
}

std::ostream& operator<<(std::ostream& o, const model::broker_properties& b) {
    fmt::print(
      o,
      "{{cores {}, mem_available {}, disk_available {}}}",
      b.cores,
      b.available_memory_gb,
      b.available_disk_gb,
      b.mount_paths,
      b.etc_props);
    return o;
}

std::ostream& operator<<(std::ostream& o, const model::broker& b) {
    fmt::print(
      o,
      "{{id: {}, kafka_advertised_listeners: {}, rpc_address: {}, rack: {}, "
      "properties: {}}}",
      b.id(),
      b.kafka_advertised_listeners(),
      b.rpc_address(),
      b.rack(),
      b.properties());
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_metadata& t_md) {
    fmt::print(
      o, "{{topic_namespace: {}, partitons: {}}}", t_md.tp_ns, t_md.partitions);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_metadata& p_md) {
    fmt::print(
      o,
      "{{id: {}, leader_id: {}, replicas: {}}}",
      p_md.id,
      p_md.leader_node,
      p_md.replicas);
    return o;
}

std::ostream& operator<<(std::ostream& o, const broker_shard& bs) {
    fmt::print(o, "{{node_id: {}, shard: {}}}", bs.node_id, bs.shard);
    return o;
}

std::ostream& operator<<(std::ostream& o, compaction_strategy c) {
    switch (c) {
    case compaction_strategy::offset:
        return o << "offset";
    case compaction_strategy::timestamp:
        return o << "timestamp";
    case compaction_strategy::header:
        return o << "header";
    }
    __builtin_unreachable();
}

std::istream& operator>>(std::istream& i, compaction_strategy& cs) {
    ss::sstring s;
    i >> s;
    cs = string_switch<compaction_strategy>(s)
           .match("offset", compaction_strategy::offset)
           .match("header", compaction_strategy::header)
           .match("timestamp", compaction_strategy::timestamp);
    return i;
};

std::istream& operator>>(std::istream& i, timestamp_type& ts_type) {
    ss::sstring s;
    i >> s;
    ts_type = string_switch<timestamp_type>(s)
                .match("LogAppendTime", timestamp_type::append_time)
                .match("CreateTime", timestamp_type::create_time);
    return i;
};

std::ostream& operator<<(std::ostream& o, cleanup_policy_bitflags c) {
    if (c == model::cleanup_policy_bitflags::none) {
        o << "none";
        return o;
    }

    auto compaction = (c & model::cleanup_policy_bitflags::compaction)
                      == model::cleanup_policy_bitflags::compaction;
    auto deletion = (c & model::cleanup_policy_bitflags::deletion)
                    == model::cleanup_policy_bitflags::deletion;

    if (compaction && deletion) {
        o << "compact,delete";
        return o;
    }

    if (compaction) {
        o << "compact";
    } else if (deletion) {
        o << "delete";
    }

    return o;
}

std::istream& operator>>(std::istream& i, cleanup_policy_bitflags& cp) {
    ss::sstring s;
    i >> s;
    cp = string_switch<cleanup_policy_bitflags>(s)
           .match("delete", cleanup_policy_bitflags::deletion)
           .match("compact", cleanup_policy_bitflags::compaction)
           .match_all(
             "compact,delete",
             "delete,compact",
             cleanup_policy_bitflags::deletion
               | cleanup_policy_bitflags::compaction);
    return i;
}

std::ostream& operator<<(std::ostream& os, const model::broker_endpoint& ep) {
    fmt::print(os, "{{{}:{}}}", ep.name, ep.address);
    return os;
}

std::ostream& operator<<(std::ostream& o, record_batch_type bt) {
    switch (bt) {
    case record_batch_type::raft_data:
        return o << "batch_type::raft_data";
    case record_batch_type::raft_configuration:
        return o << "batch_type::raft_configuration";
    case record_batch_type::controller:
        return o << "batch_type::controller";
    case record_batch_type::kvstore:
        return o << "batch_type::kvstore";
    case record_batch_type::checkpoint:
        return o << "batch_type::checkpoint";
    case record_batch_type::topic_management_cmd:
        return o << "batch_type::topic_management_cmd";
    case record_batch_type::ghost_batch:
        return o << "batch_type::ghost_batch";
    case record_batch_type::id_allocator:
        return o << "batch_type::id_allocator";
    case record_batch_type::tx_prepare:
        return o << "batch_type::tx_prepare";
    case record_batch_type::tx_fence:
        return o << "batch_type::tx_fence";
    case record_batch_type::tm_update:
        return o << "batch_type::tm_update";
    case record_batch_type::user_management_cmd:
        return o << "batch_type::user_management_cmd";
    case record_batch_type::acl_management_cmd:
        return o << "batch_type::acl_management_cmd";
    case record_batch_type::group_prepare_tx:
        return o << "batch_type::group_prepare_tx";
    case record_batch_type::group_commit_tx:
        return o << "batch_type::group_commit_tx";
    case record_batch_type::group_abort_tx:
        return o << "batch_type::group_abort_tx";
    case record_batch_type::node_management_cmd:
        return o << "batch_type::node_management_cmd";
    case record_batch_type::data_policy_management_cmd:
        return o << "batch_type::data_policy_management_cmd";
    case record_batch_type::archival_metadata:
        return o << "batch_type::archival_metadata";
    case record_batch_type::cluster_config_cmd:
        return o << "batch_type::cluster_config_cmd";
    case record_batch_type::feature_update:
        return o << "batch_type::feature_update";
    case record_batch_type::cluster_bootstrap_cmd:
        return o << "batch_type::cluster_bootstrap_cmd";
    case record_batch_type::version_fence:
        return o << "batch_type::version_fence";
    }

    return o << "batch_type::unknown{" << static_cast<int>(bt) << "}";
}

std::ostream& operator<<(std::ostream& o, membership_state st) {
    switch (st) {
    case membership_state::active:
        return o << "active";
    case membership_state::draining:
        return o << "draining";
    case membership_state::removed:
        return o << "removed";
    }
    return o << "unknown membership state {" << static_cast<int>(st) << "}";
}

std::ostream& operator<<(std::ostream& o, maintenance_state st) {
    switch (st) {
    case maintenance_state::active:
        return o << "active";
    case maintenance_state::inactive:
        return o << "inactive";
    }

    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const cloud_credentials_source& cs) {
    switch (cs) {
    case cloud_credentials_source::config_file:
        return os << "config_file";
    case cloud_credentials_source::aws_instance_metadata:
        return os << "aws_instance_metadata";
    case cloud_credentials_source::sts:
        return os << "sts";
    case cloud_credentials_source::gcp_instance_metadata:
        return os << "gcp_instance_metadata";
    }
}

std::ostream& operator<<(std::ostream& o, const shadow_indexing_mode& si) {
    switch (si) {
    case shadow_indexing_mode::disabled:
        o << "disabled";
        break;
    case shadow_indexing_mode::archival:
        o << "archival";
        break;
    case shadow_indexing_mode::fetch:
        o << "fetch";
        break;
    case shadow_indexing_mode::full:
        o << "full";
        break;
    case shadow_indexing_mode::drop_archival:
        o << "drop_archival";
        break;
    case shadow_indexing_mode::drop_fetch:
        o << "drop_fetch";
        break;
    case shadow_indexing_mode::drop_full:
        o << "drop_full";
        break;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, leader_balancer_mode lbt) {
    o << leader_balancer_mode_to_string(lbt);
    return o;
}

std::istream& operator>>(std::istream& i, leader_balancer_mode& lbt) {
    ss::sstring s;
    i >> s;
    lbt = string_switch<leader_balancer_mode>(s)
            .match(
              leader_balancer_mode_to_string(
                leader_balancer_mode::random_hill_climbing),
              leader_balancer_mode::random_hill_climbing)
            .match(
              leader_balancer_mode_to_string(
                leader_balancer_mode::greedy_balanced_shards),
              leader_balancer_mode::greedy_balanced_shards);
    return i;
}

} // namespace model
