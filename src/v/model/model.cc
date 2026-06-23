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
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/sstring.h"
#include "serde/serde_exception.h"
#include "strings/string_switch.h"
#include "utils/base64.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

#include <expected>
#include <iostream>
#include <optional>
#include <type_traits>

namespace model {

fmt::iterator timestamp::format_to(fmt::iterator it) const {
    if (*this != missing()) {
        return fmt::format_to(it, "{{timestamp: {}}}", _v);
    }
    return fmt::format_to(it, "{{timestamp: missing}}");
}

void read_nested(
  iobuf_parser& in, timestamp& ts, const size_t bytes_left_limit) {
    serde::read_nested(in, ts._v, bytes_left_limit);
}

void write_nested(iobuf& out, timestamp ts) { serde::write(out, ts._v); }

fmt::iterator topic_partition_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_id_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic_id(), partition());
}

fmt::iterator ntp::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}/{}}}", ns(), tp.topic(), tp.partition());
}

fmt::iterator topic_namespace::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

fmt::iterator topic_namespace_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

fmt::iterator format_to(timestamp_type ts, fmt::iterator out) {
    /**
     * We need to use specific string representations of timestamp_type as this
     * is related with protocol correctness
     */
    switch (ts) {
    case timestamp_type::append_time:
        return fmt::format_to(out, "LogAppendTime");
    case timestamp_type::create_time:
        return fmt::format_to(out, "CreateTime");
    }
    return fmt::format_to(
      out, "{{unknown timestamp:{}}}", static_cast<int>(ts));
}

fmt::iterator record_header::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{key_size={}, key={}, value_size={}, value={}}}",
      _key_size,
      _key,
      _val_size,
      _value);
}

fmt::iterator record_attributes::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}}}", _attributes);
}

fmt::iterator record::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{record: size_bytes={}, attributes={}, timestamp_delta={}, "
      "offset_delta={}, key_size={}, key={}, value_size={}, value={}, "
      "header_size:{}, headers=[",
      _size_bytes,
      _attributes,
      _timestamp_delta,
      _offset_delta,
      _key_size,
      _key,
      _val_size,
      _value,
      _headers.size());
    for (const auto& h : _headers) {
        it = fmt::format_to(it, "{}", h);
    }
    return fmt::format_to(it, "]}}");
}

fmt::iterator producer_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{producer_identity: id={}, epoch={}}}", id, epoch);
}

fmt::iterator record_batch_attributes::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{compression:");
    if (is_valid_compression()) {
        // this method... sadly, just throws
        it = fmt::format_to(it, "{}", compression());
    } else {
        it = fmt::format_to(it, "invalid compression");
    }
    return fmt::format_to(
      it,
      ", type:{}, transactional: {}, control: {}}}",
      timestamp_type(),
      is_transactional(),
      is_control());
}

fmt::iterator record_batch_header::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{header_crc:{}, size_bytes:{}, base_offset:{}, type:{}, crc:{}, "
      "attrs:{}, last_offset_delta:{}, first_timestamp:{}, "
      "max_timestamp:{}, producer_id:{}, producer_epoch:{}, "
      "base_sequence:{}, record_count:{}, ctx:{{term:{}, owner_shard:",
      header_crc,
      size_bytes,
      base_offset,
      type,
      crc,
      attrs,
      last_offset_delta,
      first_timestamp,
      max_timestamp,
      producer_id,
      producer_epoch,
      base_sequence,
      record_count,
      ctx.term);
    if (ctx.owner_shard) {
        it = fmt::format_to(it, "{}}}}}", *ctx.owner_shard);
    } else {
        it = fmt::format_to(it, "nullopt}}}}");
    }
    return it;
}

fmt::iterator record_batch::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{record_batch={}, records=", _header);
    if (_compressed) {
        it = fmt::format_to(
          it, "{{compressed={} bytes}}", _records.size_bytes());
    } else {
        it = fmt::format_to(it, "{{");
        for_each_record(
          [&it](const model::record& r) { it = fmt::format_to(it, "{}", r); });
        it = fmt::format_to(it, "}}");
    }
    return fmt::format_to(it, "}}");
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

fmt::iterator broker_properties::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{cores {}, mem_available {}, disk_available {}, in_fips_mode {}}}",
      cores,
      available_memory_bytes,
      available_disk_gb,
      in_fips_mode);
}

fmt::iterator broker::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, kafka_advertised_listeners: {}, rpc_address: {}, rack: {}, "
      "properties: {}}}",
      _id,
      _kafka_advertised_listeners,
      _rpc_address,
      _rack,
      _properties);
}

fmt::iterator topic_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic_namespace: {}, partitons: {}}}", tp_ns, partitions);
}

fmt::iterator partition_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{id: {}, leader_id: {}, replicas: {}}}", id, leader_node, replicas);
}

fmt::iterator broker_shard::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{node_id: {}, shard: {}}}", node_id, shard);
}

fmt::iterator format_to(compaction_strategy c, fmt::iterator out) {
    switch (c) {
    case compaction_strategy::offset:
        return fmt::format_to(out, "offset");
    case compaction_strategy::timestamp:
        return fmt::format_to(out, "timestamp");
    case compaction_strategy::header:
        return fmt::format_to(out, "header");
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

fmt::iterator format_to(cleanup_policy_bitflags c, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(c));
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

fmt::iterator broker_endpoint::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}:{}}}", name, address);
}

fmt::iterator format_to(record_batch_type bt, fmt::iterator out) {
    switch (bt) {
    case record_batch_type::raft_data:
        return fmt::format_to(out, "batch_type::raft_data");
    case record_batch_type::raft_configuration:
        return fmt::format_to(out, "batch_type::raft_configuration");
    case record_batch_type::controller:
        return fmt::format_to(out, "batch_type::controller");
    case record_batch_type::kvstore:
        return fmt::format_to(out, "batch_type::kvstore");
    case record_batch_type::checkpoint:
        return fmt::format_to(out, "batch_type::checkpoint");
    case record_batch_type::topic_management_cmd:
        return fmt::format_to(out, "batch_type::topic_management_cmd");
    case record_batch_type::ghost_batch:
        return fmt::format_to(out, "batch_type::ghost_batch");
    case record_batch_type::id_allocator:
        return fmt::format_to(out, "batch_type::id_allocator");
    case record_batch_type::tx_prepare:
        return fmt::format_to(out, "batch_type::tx_prepare");
    case record_batch_type::tx_fence:
        return fmt::format_to(out, "batch_type::tx_fence");
    case record_batch_type::tm_update:
        return fmt::format_to(out, "batch_type::tm_update");
    case record_batch_type::user_management_cmd:
        return fmt::format_to(out, "batch_type::user_management_cmd");
    case record_batch_type::acl_management_cmd:
        return fmt::format_to(out, "batch_type::acl_management_cmd");
    case record_batch_type::group_prepare_tx:
        return fmt::format_to(out, "batch_type::group_prepare_tx");
    case record_batch_type::group_commit_tx:
        return fmt::format_to(out, "batch_type::group_commit_tx");
    case record_batch_type::group_abort_tx:
        return fmt::format_to(out, "batch_type::group_abort_tx");
    case record_batch_type::node_management_cmd:
        return fmt::format_to(out, "batch_type::node_management_cmd");
    case record_batch_type::data_policy_management_cmd:
        return fmt::format_to(out, "batch_type::data_policy_management_cmd");
    case record_batch_type::archival_metadata:
        return fmt::format_to(out, "batch_type::archival_metadata");
    case record_batch_type::cluster_config_cmd:
        return fmt::format_to(out, "batch_type::cluster_config_cmd");
    case record_batch_type::feature_update:
        return fmt::format_to(out, "batch_type::feature_update");
    case record_batch_type::cluster_bootstrap_cmd:
        return fmt::format_to(out, "batch_type::cluster_bootstrap_cmd");
    case record_batch_type::version_fence:
        return fmt::format_to(out, "batch_type::version_fence");
    case record_batch_type::tx_tm_hosted_trasactions:
        return fmt::format_to(out, "batch_type::tx_tm_hosted_trasactions");
    case record_batch_type::prefix_truncate:
        return fmt::format_to(out, "batch_type::prefix_truncate");
    case record_batch_type::plugin_update:
        return fmt::format_to(out, "batch_type::plugin_update");
    case record_batch_type::tx_registry:
        return fmt::format_to(out, "batch_type::tx_registry");
    case record_batch_type::cluster_recovery_cmd:
        return fmt::format_to(out, "batch_type::cluster_recovery_cmd");
    case record_batch_type::compaction_placeholder:
        return fmt::format_to(out, "batch_type::compaction_placeholder");
    case record_batch_type::role_management_cmd:
        return fmt::format_to(out, "batch_type::role_management_cmd");
    case record_batch_type::client_quota:
        return fmt::format_to(out, "batch_type::client_quota");
    case record_batch_type::data_migration_cmd:
        return fmt::format_to(out, "batch_type::data_migration_cmd");
    case record_batch_type::group_fence_tx:
        return fmt::format_to(out, "batch_type::group_fence_tx");
    case record_batch_type::partition_properties_update:
        return fmt::format_to(out, "batch_type::partition_properties_update");
    case record_batch_type::datalake_coordinator:
        return fmt::format_to(out, "batch_type::datalake_coordinator");
    case record_batch_type::ctp_placeholder:
        return fmt::format_to(out, "batch_type::ctp_placeholder");
    case record_batch_type::ctp_stm_command:
        return fmt::format_to(out, "batch_type::ctp_stm_command");
    case record_batch_type::datalake_translation_state:
        return fmt::format_to(out, "datalake_translation_state");
    case record_batch_type::cluster_link:
        return fmt::format_to(out, "cluster_link");
    case record_batch_type::group_block:
        return fmt::format_to(out, "group_block");
    case record_batch_type::l1_stm:
        return fmt::format_to(out, "l1_stm");
    case record_batch_type::ct_read_replica_stm:
        return fmt::format_to(out, "ct_read_replica_stm");
    }
    return fmt::format_to(
      out, "batch_type::unknown{{{}}}", static_cast<int>(bt));
}

fmt::iterator format_to(membership_state st, fmt::iterator out) {
    switch (st) {
    case membership_state::active:
        return fmt::format_to(out, "active");
    case membership_state::draining:
        return fmt::format_to(out, "draining");
    case membership_state::removed:
        return fmt::format_to(out, "removed");
    }
    return fmt::format_to(
      out, "unknown membership state {{{}}}", static_cast<int>(st));
}

fmt::iterator format_to(maintenance_state st, fmt::iterator out) {
    switch (st) {
    case maintenance_state::active:
        return fmt::format_to(out, "active");
    case maintenance_state::inactive:
        return fmt::format_to(out, "inactive");
    }
    __builtin_unreachable();
}

fmt::iterator format_to(cloud_credentials_source cs, fmt::iterator out) {
    switch (cs) {
    case cloud_credentials_source::config_file:
        return fmt::format_to(out, "config_file");
    case cloud_credentials_source::aws_instance_metadata:
        return fmt::format_to(out, "aws_instance_metadata");
    case cloud_credentials_source::sts:
        return fmt::format_to(out, "sts");
    case cloud_credentials_source::gcp_instance_metadata:
        return fmt::format_to(out, "gcp_instance_metadata");
    case cloud_credentials_source::azure_aks_oidc_federation:
        return fmt::format_to(out, "azure_aks_oidc_federation");
    case cloud_credentials_source::azure_vm_instance_metadata:
        return fmt::format_to(out, "azure_vm_instance_metadata");
    }
    return fmt::format_to(out, "unknown");
}

fmt::iterator format_to(shadow_indexing_mode si, fmt::iterator out) {
    switch (si) {
    case shadow_indexing_mode::disabled:
        return fmt::format_to(out, "disabled");
    case shadow_indexing_mode::archival:
        return fmt::format_to(out, "archival");
    case shadow_indexing_mode::fetch:
        return fmt::format_to(out, "fetch");
    case shadow_indexing_mode::full:
        return fmt::format_to(out, "full");
    case shadow_indexing_mode::drop_archival:
        return fmt::format_to(out, "drop_archival");
    case shadow_indexing_mode::drop_fetch:
        return fmt::format_to(out, "drop_fetch");
    case shadow_indexing_mode::drop_full:
        return fmt::format_to(out, "drop_full");
    }
    return fmt::format_to(out, "unknown");
}

fmt::iterator format_to(control_record_type crt, fmt::iterator out) {
    switch (crt) {
    case control_record_type::tx_abort:
        return fmt::format_to(out, "tx_abort");
    case control_record_type::tx_commit:
        return fmt::format_to(out, "tx_commit");
    case control_record_type::unknown:
        return fmt::format_to(out, "unknown");
    }
    return fmt::format_to(out, "unknown");
}

fmt::iterator batch_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{pid: {}, first_seq: {}, is_transactional: {}, record_count: {}, "
      "last_seq: {}}}",
      pid,
      first_seq,
      is_transactional,
      record_count,
      last_seq);
}

fmt::iterator format_to(fetch_read_strategy s, fmt::iterator out) {
    return fmt::format_to(out, "{}", fetch_read_strategy_to_string(s));
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

fmt::iterator format_to(write_caching_mode mode, fmt::iterator out) {
    return fmt::format_to(out, "{}", write_caching_mode_to_string(mode));
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

fmt::iterator format_to(redpanda_storage_mode mode, fmt::iterator out) {
    return fmt::format_to(out, "{}", redpanda_storage_mode_to_string(mode));
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

fmt::iterator format_to(recovery_validation_mode vm, fmt::iterator out) {
    using enum recovery_validation_mode;
    switch (vm) {
    case check_manifest_existence:
        return fmt::format_to(out, "check_manifest_existence");
    case check_manifest_and_segment_metadata:
        return fmt::format_to(out, "check_manifest_and_segment_metadata");
    case no_check:
        return fmt::format_to(out, "no_check");
    }
    return fmt::format_to(out, "unknown");
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

iceberg_mode iceberg_mode::disabled = iceberg_mode{};
iceberg_mode iceberg_mode::key_value = iceberg_mode{enabled_impl{}};
iceberg_mode iceberg_mode::value_schema_id_prefix = []() {
    enabled_impl e{};
    e.value.mode = iceberg_mode::schema_mode::schema_id_prefix;
    return iceberg_mode{std::move(e)};
}();

// Wire format discriminants. Values 0-3 are the legacy per-variant
// discriminants preserved for backward compatibility. Value 4 is the new
// generic encoding for configs that cannot be expressed in the old format
// (i.e. key.mode != binary or headers.value_type != binary).
namespace {
// The discriminant is serialized as int32_t (serde_enum_serialized_t) to
// maintain backward compatibility with the original iceberg_mode::variant enum
// encoding, which used serde::write on an enum class and thus wrote int32_t.
constexpr int32_t wire_disabled = 0;
constexpr int32_t wire_key_value = 1;
constexpr int32_t wire_value_schema_id_prefix = 2;
constexpr int32_t wire_value_schema_latest = 3;
constexpr int32_t wire_canonical_string = 4;

// Payload for wire_canonical_string: the canonicalized config string wrapped
// in a versioned envelope for forward/backward compatibility.
struct iceberg_mode_string
  : serde::envelope<
      iceberg_mode_string,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring config;
    auto serde_fields() { return std::tie(config); }
};

constexpr std::string_view to_sv(iceberg_mode::schema_mode m) {
    switch (m) {
    case iceberg_mode::schema_mode::binary:
        return "binary";
    case iceberg_mode::schema_mode::schema_id_prefix:
        return "schema_id_prefix";
    case iceberg_mode::schema_mode::schema_latest:
        return "schema_latest";
    }
    __builtin_unreachable();
}

constexpr std::string_view to_sv(iceberg_mode::header_schema_mode t) {
    switch (t) {
    case iceberg_mode::header_schema_mode::binary:
        return "binary";
    case iceberg_mode::header_schema_mode::string:
        return "string";
    }
    __builtin_unreachable();
}

// Parse "opt1=val1,opt2=val2" into a map, enforcing no duplicate keys and no
// empty keys or values. An empty string returns an empty map (all defaults).
std::optional<absl::flat_hash_map<std::string, std::string>>
parse_section_opts(std::string_view str) {
    absl::flat_hash_map<std::string, std::string> result;
    if (str.empty()) {
        return result;
    }
    for (std::string_view pair : absl::StrSplit(str, ",")) {
        auto [it, inserted] = result.insert(
          absl::StrSplit(pair, absl::MaxSplits("=", 1)));
        if (!inserted) {
            return std::nullopt; // duplicate key
        }
        if (it->first.empty() || it->second.empty()) {
            return std::nullopt; // empty key or value
        }
    }
    return result;
}

// Parse the section-based iceberg_mode grammar:
//   <section> (";" <section>)*
//   <section> ::= ("key"|"value"|"headers") ":" <opts>
//
// Unknown sections and unknown option keys are parse errors. Duplicate
// sections or duplicate option keys are also parse errors.
std::expected<iceberg_mode, ss::sstring>
parse_extended_iceberg_mode(std::string_view str) {
    using opts_map = absl::flat_hash_map<std::string, std::string>;

    // Collect each section's raw options, checking for duplicates and unknown
    // section names before doing any semantic parsing.
    std::optional<opts_map> key_opts, value_opts, headers_opts;

    for (std::string_view sec_str : absl::StrSplit(str, ";")) {
        if (sec_str.empty()) {
            return std::unexpected(
              ss::sstring("empty section in iceberg_mode config"));
        }
        auto colon = sec_str.find(':');
        if (colon == std::string_view::npos) {
            return std::unexpected(
              fmt::format("missing ':' in section '{}'", sec_str));
        }
        auto sec_name = sec_str.substr(0, colon);
        auto opts_str = sec_str.substr(colon + 1);

        auto opts = parse_section_opts(opts_str);
        if (!opts) {
            return std::unexpected(
              fmt::format("malformed options in section '{}'", sec_name));
        }

        if (sec_name == "key") {
            if (key_opts) {
                return std::unexpected(ss::sstring("duplicate section 'key'"));
            }
            key_opts = std::move(*opts);
        } else if (sec_name == "value") {
            if (value_opts) {
                return std::unexpected(
                  ss::sstring("duplicate section 'value'"));
            }
            value_opts = std::move(*opts);
        } else if (sec_name == "headers") {
            if (headers_opts) {
                return std::unexpected(
                  ss::sstring("duplicate section 'headers'"));
            }
            headers_opts = std::move(*opts);
        } else {
            return std::unexpected(
              fmt::format("unknown section '{}'", sec_name));
        }
    }

    iceberg_mode::enabled_impl result{};

    auto parse_schema_opts =
      [](const opts_map& opts, auto& cfg, std::string_view sec_name)
      -> std::expected<void, ss::sstring> {
        for (const auto& [k, v] : opts) {
            if (k == "mode") {
                using sm = iceberg_mode::schema_mode;
                auto m = string_switch<std::optional<sm>>(v)
                           .match("binary", sm::binary)
                           .match("schema_id_prefix", sm::schema_id_prefix)
                           .match("schema_latest", sm::schema_latest)
                           .default_match(std::nullopt);
                if (!m) {
                    return std::unexpected(
                      fmt::format(
                        "unknown mode '{}' in section '{}'", v, sec_name));
                }
                cfg.mode = *m;
            } else if (k == "subject") {
                cfg.subject = ss::sstring(v);
            } else if (k == "protobuf_name") {
                cfg.protobuf_name = ss::sstring(v);
            } else {
                return std::unexpected(
                  fmt::format(
                    "unknown option '{}' in section '{}'", k, sec_name));
            }
        }
        if (
          cfg.mode != iceberg_mode::schema_mode::schema_latest
          && (!cfg.subject.empty() || !cfg.protobuf_name.empty())) {
            return std::unexpected(
              fmt::format(
                "subject and protobuf_name require mode=schema_latest in "
                "section '{}'",
                sec_name));
        }
        return {};
    };

    if (key_opts) {
        if (auto r = parse_schema_opts(*key_opts, result.key, "key"); !r) {
            return std::unexpected(std::move(r.error()));
        }
    }
    if (value_opts) {
        if (
          auto r = parse_schema_opts(*value_opts, result.value, "value"); !r) {
            return std::unexpected(std::move(r.error()));
        }
    }
    if (headers_opts) {
        for (const auto& [k, v] : *headers_opts) {
            if (k == "value_type") {
                using hsm = iceberg_mode::header_schema_mode;
                auto t = string_switch<std::optional<hsm>>(v)
                           .match("binary", hsm::binary)
                           .match("string", hsm::string)
                           .default_match(std::nullopt);
                if (!t) {
                    return std::unexpected(
                      fmt::format(
                        "unknown value_type '{}' in section 'headers'", v));
                }
                result.headers.value_type = *t;
            } else {
                return std::unexpected(
                  fmt::format("unknown option '{}' in section 'headers'", k));
            }
        }
    }

    return iceberg_mode{std::move(result)};
}
} // namespace

void write_nested(iobuf& out, const iceberg_mode& m) {
    using serde::write;
    if (m.is_disabled()) {
        write(out, wire_disabled);
        return;
    }
    const auto& e = std::get<iceberg_mode::enabled_impl>(m._impl);
    // Configs where key and headers are at their defaults can be expressed
    // with the old wire discriminants so that old nodes can read them.
    if (
      e.key == iceberg_mode::key_config{}
      && e.headers == iceberg_mode::headers_config{}) {
        switch (e.value.mode) {
        case iceberg_mode::schema_mode::binary:
            write(out, wire_key_value);
            return;
        case iceberg_mode::schema_mode::schema_id_prefix:
            write(out, wire_value_schema_id_prefix);
            return;
        case iceberg_mode::schema_mode::schema_latest:
            write(out, wire_value_schema_latest);
            write(out, e.value.protobuf_name);
            write(out, e.value.subject);
            return;
        }
    }
    // Discriminant 4: the encoded form is the canonicalized config string.
    write(out, wire_canonical_string);
    write(out, iceberg_mode_string{.config = fmt::format("{}", m)});
}

void read_nested(
  iobuf_parser& in, iceberg_mode& m, const std::size_t bytes_left_limit) {
    using serde::read_nested;
    int32_t disc = 0;
    read_nested(in, disc, bytes_left_limit);
    switch (disc) {
    case wire_disabled:
        m = iceberg_mode::disabled;
        return;
    case wire_key_value:
        m = iceberg_mode::key_value;
        return;
    case wire_value_schema_id_prefix:
        m = iceberg_mode::value_schema_id_prefix;
        return;
    case wire_value_schema_latest: {
        ss::sstring proto_name;
        ss::sstring subject;
        read_nested(in, proto_name, bytes_left_limit);
        read_nested(in, subject, bytes_left_limit);
        m = iceberg_mode::value_schema_latest(proto_name, subject);
        return;
    }
    case wire_canonical_string: {
        iceberg_mode_string payload;
        read_nested(in, payload, bytes_left_limit);
        auto result = parse_iceberg_mode(payload.config);
        if (!result) {
            throw serde::serde_exception(
              fmt::format(
                "invalid iceberg_mode config in wire format: {}",
                result.error()));
        }
        m = std::move(*result);
        return;
    }
    }
    throw serde::serde_exception(
      fmt::format("unknown iceberg_mode discriminant: {}", disc));
}

fmt::iterator iceberg_mode::format_to(fmt::iterator it) const {
    if (is_disabled()) {
        return fmt::format_to(it, "disabled");
    }
    const auto& e = std::get<enabled_impl>(_impl);

    // If key and headers are at their defaults the config is expressible as a
    // legacy string; prefer that for maximal compatibility.
    if (e.key == key_config{} && e.headers == headers_config{}) {
        switch (e.value.mode) {
        case schema_mode::binary:
            return fmt::format_to(it, "key_value");
        case schema_mode::schema_id_prefix:
            return fmt::format_to(it, "value_schema_id_prefix");
        case schema_mode::schema_latest:
            it = fmt::format_to(it, "value_schema_latest");
            bool delim = false;
            auto emit = [&]() {
                it = fmt::format_to(it, "{}", delim ? "," : ":");
                delim = true;
            };
            if (!e.value.protobuf_name.empty()) {
                emit();
                it = fmt::format_to(
                  it, "protobuf_name={}", e.value.protobuf_name);
            }
            if (!e.value.subject.empty()) {
                emit();
                it = fmt::format_to(it, "subject={}", e.value.subject);
            }
            return it;
        }
    }

    // Section-based format. Only emit sections that differ from defaults.
    bool any = false;
    auto sep = [&]() {
        if (any) {
            it = fmt::format_to(it, ";");
        }
        any = true;
    };

    auto emit_schema_section = [&](std::string_view name, const auto& cfg) {
        if (cfg.mode == schema_mode::binary) {
            return; // all defaults, omit
        }
        sep();
        it = fmt::format_to(it, "{}:mode={}", name, to_sv(cfg.mode));
        if (!cfg.subject.empty()) {
            it = fmt::format_to(it, ",subject={}", cfg.subject);
        }
        if (!cfg.protobuf_name.empty()) {
            it = fmt::format_to(it, ",protobuf_name={}", cfg.protobuf_name);
        }
    };

    emit_schema_section("key", e.key);
    emit_schema_section("value", e.value);

    if (e.headers.value_type != header_schema_mode::binary) {
        sep();
        it = fmt::format_to(
          it, "headers:value_type={}", to_sv(e.headers.value_type));
    }

    return it;
}

namespace {
// Parse the legacy value_schema_latest options string (everything after the
// "value_schema_latest" prefix), which has the grammar `:(<name>=<value>)*`.
std::expected<iceberg_mode, ss::sstring>
parse_legacy_schema_latest(std::string_view suffix) {
    if (suffix.empty()) {
        return iceberg_mode::value_schema_latest("", "");
    }
    if (!absl::ConsumePrefix(&suffix, ":")) {
        return std::unexpected(
          ss::sstring(
            "expected ':' or end of string after 'value_schema_latest'"));
    }
    if (suffix.empty()) {
        return std::unexpected(
          ss::sstring("expected options after ':' in value_schema_latest"));
    }
    absl::flat_hash_map<std::string, std::string> opts;
    for (std::string_view pair : absl::StrSplit(suffix, ",")) {
        auto [it, inserted] = opts.insert(
          absl::StrSplit(pair, absl::MaxSplits("=", 1)));
        if (!inserted || it->first.empty() || it->second.empty()) {
            return std::unexpected(
              ss::sstring("malformed options in value_schema_latest"));
        }
    }
    std::string_view proto_name;
    std::string_view subject;
    for (const auto& [k, v] : opts) {
        if (k == "protobuf_name") {
            proto_name = v;
        } else if (k == "subject") {
            subject = v;
        } else {
            return std::unexpected(
              fmt::format("unknown option '{}' in value_schema_latest", k));
        }
    }
    return iceberg_mode::value_schema_latest(proto_name, subject);
}
} // namespace

std::expected<iceberg_mode, ss::sstring>
parse_iceberg_mode(std::string_view s) {
    if (s == "disabled") {
        return iceberg_mode::disabled;
    } else if (s == "key_value") {
        return iceberg_mode::key_value;
    } else if (s == "value_schema_id_prefix") {
        return iceberg_mode::value_schema_id_prefix;
    } else if (s.starts_with("value_schema_latest")) {
        return parse_legacy_schema_latest(
          s.substr(std::strlen("value_schema_latest")));
    } else {
        return parse_extended_iceberg_mode(s);
    }
}

std::istream& operator>>(std::istream& is, iceberg_mode& mode) {
    ss::sstring s;
    is >> s;
    auto result = parse_iceberg_mode(s);
    if (!result) {
        is.setstate(std::ios::failbit);
        return is;
    }
    mode = std::move(*result);
    return is;
}

fmt::iterator format_to(iceberg_invalid_record_action a, fmt::iterator out) {
    switch (a) {
    case iceberg_invalid_record_action::drop:
        return fmt::format_to(out, "drop");
    case iceberg_invalid_record_action::dlq_table:
        return fmt::format_to(out, "dlq_table");
    }
    return fmt::format_to(out, "unknown");
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

fmt::iterator format_to(iceberg_schema_case_insensitive n, fmt::iterator out) {
    switch (n) {
    case iceberg_schema_case_insensitive::auto_:
        return fmt::format_to(out, "auto");
    case iceberg_schema_case_insensitive::no:
        return fmt::format_to(out, "no");
    case iceberg_schema_case_insensitive::yes:
        return fmt::format_to(out, "yes");
    }
    return fmt::format_to(out, "unknown");
}

std::istream& operator>>(std::istream& is, iceberg_schema_case_insensitive& n) {
    using enum iceberg_schema_case_insensitive;
    ss::sstring s;
    is >> s;
    try {
        n = string_switch<iceberg_schema_case_insensitive>(s)
              .match("auto", auto_)
              .match("no", no)
              .match("yes", yes);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

fmt::iterator format_to(fips_mode_flag f, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(f));
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

fmt::iterator format_to(kafka_batch_validation_mode mode, fmt::iterator out) {
    return fmt::format_to(
      out, "{}", kafka_batch_validation_mode_to_string(mode));
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

fmt::iterator format_to(isolation_level l, fmt::iterator out) {
    switch (l) {
    case isolation_level::read_uncommitted:
        return fmt::format_to(out, "read_uncommitted");
    case isolation_level::read_committed:
        return fmt::format_to(out, "read_committed");
    }
    return fmt::format_to(out, "unknown");
}

} // namespace model
