// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "model/validation.h"
#include "utils/string_switch.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <fmt/ostream.h>

#include <iostream>
#include <type_traits>

namespace model {

std::optional<materialized_topic>
make_materialized_topic(const model::topic& topic) {
    // Materialized topics will follow this schema:
    // <source_topic>.$<destination_topic>$
    // Note that dollar signs are not valid kafka topic names but they
    // can be part of a valid coproc materialized_topic name
    const auto found = topic().find('.');
    if (
      found == ss::sstring::npos || found == 0 || found == topic().size() - 1) {
        // If the char '.' is never found, or at a position that would make
        // either of the parts empty, fail out
        return std::nullopt;
    }
    std::string_view src(topic().begin(), found);
    std::string_view dest(
      (topic().begin() + found + 1), topic().size() - found - 1);
    if (!(dest.size() >= 3 && dest[0] == '$' && dest.back() == '$')) {
        // Dest must have at least two dollar chars surronding the topic name
        return std::nullopt;
    }

    // No need for '$' chars in the string_view, its implied by being in the
    // 'dest' mvar
    dest.remove_prefix(1);
    dest.remove_suffix(1);

    model::topic_view src_tv(src);
    model::topic_view dest_tv(dest);
    if (
      model::validate_kafka_topic_name(src_tv).value() != 0
      || model::validate_kafka_topic_name(dest_tv).value() != 0) {
        // The parts of this whole must be valid kafka topics
        return std::nullopt;
    }

    return materialized_topic{.src = src_tv, .dest = dest_tv};
}

materialized_ntp::materialized_ntp(model::ntp ntp) noexcept
  : _input(std::move(ntp))
  , _maybe_source(make_materialized_src_ntp(_input))
  , _source_or_input(_maybe_source ? *_maybe_source : _input) {}

materialized_ntp::materialized_ntp(const materialized_ntp& other) noexcept
  : _input(other._input)
  , _maybe_source(other._maybe_source)
  , _source_or_input(_maybe_source ? *_maybe_source : _input) {}

materialized_ntp::materialized_ntp(materialized_ntp&& other) noexcept
  : _input(std::move(other._input))
  , _maybe_source(std::move(other._maybe_source))
  , _source_or_input(_maybe_source ? *_maybe_source : _input) {}

std::optional<model::ntp>
materialized_ntp::make_materialized_src_ntp(const model::ntp& ntp) {
    if (auto mt = make_materialized_topic(ntp.tp.topic)) {
        return std::optional<model::ntp>(
          model::ntp(ntp.ns, mt->src, ntp.tp.partition));
    }
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const materialized_ntp& m) {
    os << "Original ntp: " << m.input_ntp() << " is_materialized: {}"
       << m.is_materialized() << " reference content: " << m.source_ntp();
    return os;
}

std::ostream& operator<<(std::ostream& os, timestamp ts) {
    if (ts != timestamp::missing()) {
        return ss::fmt_print(os, "{{timestamp: {}}}", ts.value());
    }
    return os << "{timestamp: missing}";
}

std::ostream& operator<<(std::ostream& os, const topic_partition& tp) {
    return ss::fmt_print(
      os, "{{topic_partition: {}:{}}}", tp.topic, tp.partition);
}

std::ostream& operator<<(std::ostream& os, const ntp& n) {
    fmt::print(os, "{{{}/{}/{}}}", n.ns(), n.tp.topic(), n.tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& o, const model::topic_namespace& tp_ns) {
    return ss::fmt_print(o, "{{ns: {}, topic: {}}}", tp_ns.ns, tp_ns.tp);
}

std::ostream&
operator<<(std::ostream& o, const model::topic_namespace_view& tp_ns) {
    return ss::fmt_print(o, "{{ns: {}, topic: {}}}", tp_ns.ns, tp_ns.tp);
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
    return o << ", type:" << attrs.timestamp_type() << "}";
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
    return ss::fmt_print(
      os, "{{compressed_records: size_bytes={}}}", records.size_bytes());
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
    return ss::fmt_print(
      o,
      "{{cores {}, mem_available {}, disk_available {}}}",
      b.cores,
      b.available_memory,
      b.available_disk,
      b.mount_paths,
      b.etc_props);
}

std::ostream& operator<<(std::ostream& o, const model::broker& b) {
    return ss::fmt_print(
      o,
      "{{id: {}, kafka_advertised_listeners: {}, rpc_address: {}, rack: {}, "
      "properties: {}, membership_state: {}}}",
      b.id(),
      b.kafka_advertised_listeners(),
      b.rpc_address(),
      b.rack(),
      b.properties(),
      b.get_membership_state());
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
        return o << "{compaction_strategy::offset}";
    case compaction_strategy::timestamp:
        return o << "{compaction_strategy::timestamp}";
    case compaction_strategy::header:
        return o << "{compaction_strategy::header}";
    }
    return o << "{unknown model::compaction_strategy}";
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

std::ostream& operator<<(std::ostream& o, const shadow_indexing_mode& si) {
    switch (si) {
    case shadow_indexing_mode::disabled:
        o << "disabled";
        break;
    case shadow_indexing_mode::archival_storage:
        o << "archival_storage";
        break;
    case shadow_indexing_mode::shadow_indexing:
        o << "shadow_indexing";
        break;
    }
    return o;
}

} // namespace model
