// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "model/validation.h"
#include "serde/rw/enum.h"
#include "serde/rw/rw.h"
#include "serde/rw/sstring.h"
#include "serde/serde_exception.h"
#include "strings/string_switch.h"
#include "utils/base64.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <fmt/ostream.h>

#include <iostream>
#include <optional>
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
  iobuf_parser& in, timestamp& ts, const size_t bytes_left_limit) {
    serde::read_nested(in, ts._v, bytes_left_limit);
}

void write_nested(iobuf& out, timestamp ts) { serde::write(out, ts._v); }

std::ostream& operator<<(std::ostream& os, const topic_partition_view& tp) {
    fmt::print(os, "{{{}/{}}}", tp.topic(), tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& os, const topic_partition& tp) {
    fmt::print(os, "{{{}/{}}}", tp.topic(), tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& os, const topic_id_partition& tp) {
    fmt::print(os, "{{{}/{}}}", tp.topic_id(), tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& os, const ntp& n) {
    fmt::print(os, "{{{}/{}/{}}}", n.ns(), n.tp.topic(), n.tp.partition());
    return os;
}

std::ostream& operator<<(std::ostream& o, const model::topic_namespace& tp_ns) {
    fmt::print(o, "{{{}/{}}}", tp_ns.ns(), tp_ns.tp());
    return o;
}

std::ostream&
operator<<(std::ostream& o, const model::topic_namespace_view& tp_ns) {
    fmt::print(o, "{{{}/{}}}", tp_ns.ns(), tp_ns.tp());
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

ss::sstring topic_namespace::path() const {
    return ssx::sformat("{}/{}", ns(), tp());
}

std::filesystem::path ntp::topic_path() const {
    return fmt::format("{}/{}", ns(), tp.topic());
}

std::istream& operator>>(std::istream& i, compression& c) {
    ss::sstring s;
    i >> s;
    auto tmp = string_switch<std::optional<compression>>(s)
                 .match_all("none", "uncompressed", compression::none)
                 .match("gzip", compression::gzip)
                 .match("snappy", compression::snappy)
                 .match("lz4", compression::lz4)
                 .match("zstd", compression::zstd)
                 .match("producer", compression::producer)
                 .default_match(std::nullopt);

    if (tmp.has_value()) {
        c = tmp.value();
    } else {
        i.setstate(std::ios_base::failbit);
    }

    return i;
}

std::ostream& operator<<(std::ostream& o, const model::broker_properties& b) {
    fmt::print(
      o,
      "{{cores {}, mem_available {}, disk_available {}, in_fips_mode {}}}",
      b.cores,
      b.available_memory_bytes,
      b.available_disk_gb,
      b.in_fips_mode,
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

    auto compaction = model::is_compaction_enabled(c);
    auto deletion = model::is_deletion_enabled(c);

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
    case record_batch_type::tx_tm_hosted_trasactions:
        return o << "batch_type::tx_tm_hosted_trasactions";
    case record_batch_type::prefix_truncate:
        return o << "batch_type::prefix_truncate";
    case record_batch_type::plugin_update:
        return o << "batch_type::plugin_update";
    case record_batch_type::tx_registry:
        return o << "batch_type::tx_registry";
    case record_batch_type::cluster_recovery_cmd:
        return o << "batch_type::cluster_recovery_cmd";
    case record_batch_type::compaction_placeholder:
        return o << "batch_type::compaction_placeholder";
    case record_batch_type::role_management_cmd:
        return o << "batch_type::role_management_cmd";
    case record_batch_type::client_quota:
        return o << "batch_type::client_quota";
    case record_batch_type::data_migration_cmd:
        return o << "batch_type::data_migration_cmd";
    case record_batch_type::group_fence_tx:
        return o << "batch_type::group_fence_tx";
    case record_batch_type::partition_properties_update:
        return o << "batch_type::partition_properties_update";
    case record_batch_type::datalake_coordinator:
        return o << "batch_type::datalake_coordinator";
    case record_batch_type::ctp_placeholder:
        return o << "batch_type::ctp_placeholder";
    case record_batch_type::ctp_stm_command:
        return o << "batch_type::ctp_stm_command";
    case record_batch_type::datalake_translation_state:
        return o << "datalake_translation_state";
    case record_batch_type::cluster_link:
        return o << "cluster_link";
    case record_batch_type::group_block:
        return o << "group_block";
    case record_batch_type::l1_stm:
        return o << "l1_stm";
    case record_batch_type::ct_read_replica_stm:
        return o << "ct_read_replica_stm";
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
    case cloud_credentials_source::azure_aks_oidc_federation:
        return os << "azure_aks_oidc_federation";
    case cloud_credentials_source::azure_vm_instance_metadata:
        return os << "azure_vm_instance_metadata";
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

std::ostream& operator<<(std::ostream& o, const control_record_type& crt) {
    switch (crt) {
    case control_record_type::tx_abort:
        return o << "tx_abort";
    case control_record_type::tx_commit:
        return o << "tx_commit";
    case control_record_type::unknown:
        return o << "unknown";
    }
}

std::ostream& operator<<(std::ostream& o, const batch_identity& bid) {
    fmt::print(
      o,
      "{{pid: {}, first_seq: {}, is_transactional: {}, record_count: {}, "
      "last_seq: {}}}",
      bid.pid,
      bid.first_seq,
      bid.is_transactional,
      bid.record_count,
      bid.last_seq);
    return o;
}

std::ostream& operator<<(std::ostream& o, fetch_read_strategy s) {
    o << fetch_read_strategy_to_string(s);
    return o;
}

std::istream& operator>>(std::istream& i, fetch_read_strategy& strat) {
    ss::sstring s;
    i >> s;
    strat = string_switch<fetch_read_strategy>(s)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::polling),
                fetch_read_strategy::polling)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::non_polling),
                fetch_read_strategy::non_polling)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_debounce),
                fetch_read_strategy::non_polling_with_debounce)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_pid),
                fetch_read_strategy::non_polling_with_pid);
    return i;
}

std::ostream& operator<<(std::ostream& o, write_caching_mode mode) {
    o << write_caching_mode_to_string(mode);
    return o;
}

std::istream& operator>>(std::istream& i, write_caching_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = write_caching_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

std::optional<write_caching_mode>
write_caching_mode_from_string(std::string_view s) {
    return string_switch<std::optional<write_caching_mode>>(s)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_true),
        model::write_caching_mode::default_true)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_false),
        model::write_caching_mode::default_false)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::disabled),
        model::write_caching_mode::disabled)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& o, redpanda_storage_mode mode) {
    o << redpanda_storage_mode_to_string(mode);
    return o;
}

std::istream& operator>>(std::istream& i, redpanda_storage_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = redpanda_storage_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

std::optional<redpanda_storage_mode>
redpanda_storage_mode_from_string(std::string_view s) {
    return string_switch<std::optional<redpanda_storage_mode>>(s)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::local),
        model::redpanda_storage_mode::local)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::tiered),
        model::redpanda_storage_mode::tiered)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::cloud),
        model::redpanda_storage_mode::cloud)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::tiered_cloud),
        model::redpanda_storage_mode::tiered_cloud)
      .match(
        model::redpanda_storage_mode_to_string(
          model::redpanda_storage_mode::unset),
        model::redpanda_storage_mode::unset)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, recovery_validation_mode vm) {
    using enum recovery_validation_mode;
    switch (vm) {
    case check_manifest_existence:
        return os << "check_manifest_existence";
    case check_manifest_and_segment_metadata:
        return os << "check_manifest_and_segment_metadata";
    case no_check:
        return os << "no_check";
    }
}

std::istream& operator>>(std::istream& is, recovery_validation_mode& vm) {
    using enum recovery_validation_mode;
    auto s = ss::sstring{};
    is >> s;
    try {
        vm = string_switch<recovery_validation_mode>(s)
               .match("check_manifest_existence", check_manifest_existence)
               .match(
                 "check_manifest_and_segment_metadata",
                 check_manifest_and_segment_metadata)
               .match("no_check", no_check);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

iceberg_mode iceberg_mode::disabled
  = iceberg_mode::make<iceberg_mode::variant::disabled>();
iceberg_mode iceberg_mode::key_value
  = iceberg_mode::make<iceberg_mode::variant::key_value>();
iceberg_mode iceberg_mode::value_schema_id_prefix
  = iceberg_mode::make<iceberg_mode::variant::value_schema_id_prefix>();

void write_nested(iobuf& out, const iceberg_mode& m) {
    using serde::write;
    write(out, m.kind());
    if (m.kind() == iceberg_mode::variant::value_schema_latest) {
        write(out, m.protobuf_full_name().value_or(""));
        write(out, m.subject_name().value_or(""));
    }
}

void read_nested(
  iobuf_parser& in, iceberg_mode& m, const std::size_t bytes_left_limit) {
    using serde::read_nested;
    iceberg_mode::variant v = iceberg_mode::variant::disabled;
    read_nested(in, v, bytes_left_limit);
    switch (v) {
    case iceberg_mode::variant::disabled:
        m = iceberg_mode::disabled;
        return;
    case iceberg_mode::variant::key_value:
        m = iceberg_mode::key_value;
        return;
    case iceberg_mode::variant::value_schema_id_prefix:
        m = iceberg_mode::value_schema_id_prefix;
        return;
    case iceberg_mode::variant::value_schema_latest:
        ss::sstring msg_name;
        read_nested(in, msg_name, bytes_left_limit);
        ss::sstring subject;
        read_nested(in, subject, bytes_left_limit);
        m = iceberg_mode::value_schema_latest(msg_name, subject);
        return;
    }
    throw serde::serde_exception(
      fmt::format("unknown iceberg_mode variant: {}", std::to_underlying(v)));
}

std::ostream& operator<<(std::ostream& os, const iceberg_mode& mode) {
    switch (mode.kind()) {
    case iceberg_mode::variant::disabled:
        return os << "disabled";
    case iceberg_mode::variant::key_value:
        return os << "key_value";
    case iceberg_mode::variant::value_schema_id_prefix:
        return os << "value_schema_id_prefix";
    case iceberg_mode::variant::value_schema_latest:
        os << "value_schema_latest";
        bool delimiter = false;
        auto emit_delimiter = [&delimiter, &os]() {
            os << (delimiter ? "," : ":");
            delimiter = true;
        };
        if (auto protobuf_name = mode.protobuf_full_name()) {
            emit_delimiter();
            os << "protobuf_name=" << protobuf_name.value();
        }
        if (auto subject = mode.subject_name()) {
            emit_delimiter();
            os << "subject=" << subject.value();
        }
        return os;
    }
}

namespace {
// Parse configuration options for iceberg_mode's value_schema_latest, which
// is a grammar like: `:(<name>=<value>)+`
std::optional<absl::flat_hash_map<std::string, std::string>>
parse_config_options(std::string_view str) {
    if (str.empty()) {
        return absl::flat_hash_map<std::string, std::string>{};
    }
    if (!absl::ConsumePrefix(&str, ":")) {
        return std::nullopt;
    }
    if (str.empty()) {
        return std::nullopt;
    }
    absl::flat_hash_map<std::string, std::string> result;
    for (std::string_view pair : absl::StrSplit(str, ",")) {
        auto [it, inserted] = result.insert(
          absl::StrSplit(pair, absl::MaxSplits("=", 1)));
        // Don't allow duplicates
        if (!inserted) {
            return std::nullopt;
        }
        // Don't allow empty keys or values
        if (it->first.empty() || it->second.empty()) {
            return std::nullopt;
        }
    }
    return result;
}
} // namespace

std::istream& operator>>(std::istream& is, iceberg_mode& mode) {
    ss::sstring s;
    is >> s;
    if (s == "disabled") {
        mode = iceberg_mode::disabled;
    } else if (s == "key_value") {
        mode = iceberg_mode::key_value;
    } else if (s == "value_schema_id_prefix") {
        mode = iceberg_mode::value_schema_id_prefix;
    } else if (s.starts_with("value_schema_latest")) {
        s = s.substr(std::strlen("value_schema_latest"));
        auto options = parse_config_options(s);
        if (!options.has_value()) {
            is.setstate(std::ios::failbit);
            return is;
        }
        std::string_view protobuf_name;
        std::string_view subject;
        for (const auto& [key, value] : options.value()) {
            if (key == "protobuf_name") {
                protobuf_name = value;
            } else if (key == "subject") {
                subject = value;
            } else {
                is.setstate(std::ios::failbit);
                return is;
            }
        }
        mode = iceberg_mode::value_schema_latest(protobuf_name, subject);
    } else {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::ostream&
operator<<(std::ostream& os, const iceberg_invalid_record_action& a) {
    switch (a) {
    case iceberg_invalid_record_action::drop:
        return os << "drop";
    case iceberg_invalid_record_action::dlq_table:
        return os << "dlq_table";
    }
}

std::istream& operator>>(std::istream& is, iceberg_invalid_record_action& a) {
    using enum iceberg_invalid_record_action;
    ss::sstring s;
    is >> s;
    try {
        a = string_switch<iceberg_invalid_record_action>(s)
              .match("drop", drop)
              .match("dlq_table", dlq_table);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::ostream& operator<<(std::ostream& os, const fips_mode_flag& f) {
    return os << to_string_view(f);
}

std::istream& operator>>(std::istream& is, fips_mode_flag& f) {
    ss::sstring s;
    is >> s;
    f = string_switch<fips_mode_flag>(s)
          .match(
            to_string_view(fips_mode_flag::disabled), fips_mode_flag::disabled)
          .match(
            to_string_view(fips_mode_flag::enabled), fips_mode_flag::enabled)
          .match(
            to_string_view(fips_mode_flag::permissive),
            fips_mode_flag::permissive);
    return is;
}

fmt::iterator topic_id::format_to(fmt::iterator it) const {
    const auto& uuid = (*this)().uuid();
    const bytes_view bv{uuid.begin(), uuid.size()};
    return fmt::format_to(it, "{}", bytes_to_base64(bv));
}

topic_id_partition topic_id_partition::from(std::string_view s) {
    std::vector<ss::sstring> ss = absl::StrSplit(s, "/");
    if (ss.size() != 2) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    auto tid = uuid_t::from_string(ss[0]);
    int p{0};
    if (!absl::SimpleAtoi(ss[1].data(), &p)) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    return model::topic_id_partition(
      model::topic_id(tid), model::partition_id(p));
}

std::optional<kafka_batch_validation_mode>
kafka_batch_validation_mode_from_string(std::string_view s) {
    return string_switch<std::optional<kafka_batch_validation_mode>>(s)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::legacy),
        model::kafka_batch_validation_mode::legacy)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::relaxed),
        model::kafka_batch_validation_mode::relaxed)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::strict),
        model::kafka_batch_validation_mode::strict)
      .default_match(std::nullopt);
}

std::ostream&
operator<<(std::ostream& o, const kafka_batch_validation_mode& mode) {
    o << kafka_batch_validation_mode_to_string(mode);
    return o;
}

std::istream& operator>>(std::istream& i, kafka_batch_validation_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = kafka_batch_validation_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

} // namespace model
