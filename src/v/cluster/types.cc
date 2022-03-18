// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"

#include "cluster/fwd.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "security/acl.h"
#include "tristate.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

namespace cluster {

bool topic_properties::is_compacted() const {
    if (!cleanup_policy_bitflags) {
        return false;
    }
    return (cleanup_policy_bitflags.value()
            & model::cleanup_policy_bitflags::compaction)
           == model::cleanup_policy_bitflags::compaction;
}

bool topic_properties::has_overrides() const {
    return cleanup_policy_bitflags || compaction_strategy || segment_size
           || retention_bytes.has_value() || retention_bytes.is_disabled()
           || retention_duration.has_value() || retention_duration.is_disabled()
           || recovery.has_value() || shadow_indexing.has_value();
}

storage::ntp_config::default_overrides
topic_properties::get_ntp_cfg_overrides() const {
    storage::ntp_config::default_overrides ret;
    ret.cleanup_policy_bitflags = cleanup_policy_bitflags;
    ret.compaction_strategy = compaction_strategy;
    ret.retention_bytes = retention_bytes;
    ret.retention_time = retention_duration;
    ret.segment_size = segment_size;
    ret.shadow_indexing_mode = shadow_indexing
                                 ? *shadow_indexing
                                 : model::shadow_indexing_mode::disabled;
    return ret;
}

topic_configuration::topic_configuration(
  model::ns n, model::topic t, int32_t count, int16_t rf)
  : tp_ns(std::move(n), std::move(t))
  , partition_count(count)
  , replication_factor(rf) {}

storage::ntp_config topic_configuration::make_ntp_config(
  const ss::sstring& work_dir,
  model::partition_id p_id,
  model::revision_id rev,
  model::initial_revision_id init_rev) const {
    auto has_overrides = properties.has_overrides() || is_internal();
    std::unique_ptr<storage::ntp_config::default_overrides> overrides = nullptr;

    if (has_overrides) {
        overrides = std::make_unique<storage::ntp_config::default_overrides>(
          storage::ntp_config::default_overrides{
            .cleanup_policy_bitflags = properties.cleanup_policy_bitflags,
            .compaction_strategy = properties.compaction_strategy,
            .segment_size = properties.segment_size,
            .retention_bytes = properties.retention_bytes,
            .retention_time = properties.retention_duration,
            // we disable cache for internal topics as they are read only once
            // during bootstrap.
            .cache_enabled = storage::with_cache(!is_internal()),
            .recovery_enabled = storage::topic_recovery_enabled(
              properties.recovery ? *properties.recovery : false),
            .shadow_indexing_mode = properties.shadow_indexing
                                      ? *properties.shadow_indexing
                                      : model::shadow_indexing_mode::disabled});
    }
    return {
      model::ntp(tp_ns.ns, tp_ns.tp, p_id),
      work_dir,
      std::move(overrides),
      rev,
      init_rev};
}

model::topic_metadata topic_configuration_assignment::get_metadata() const {
    model::topic_metadata ret(cfg.tp_ns);
    ret.partitions.reserve(assignments.size());
    std::transform(
      std::cbegin(assignments),
      std::cend(assignments),
      std::back_inserter(ret.partitions),
      [](const partition_assignment& pd) {
          return pd.create_partition_metadata();
      });

    std::sort(
      ret.partitions.begin(),
      ret.partitions.begin(),
      [](
        const model::partition_metadata& a,
        const model::partition_metadata& b) { return a.id < b.id; });
    return ret;
}

topic_table_delta::topic_table_delta(
  model::ntp ntp,
  cluster::partition_assignment new_assignment,
  model::offset o,
  op_type tp,
  std::optional<partition_assignment> previous)
  : ntp(std::move(ntp))
  , new_assignment(std::move(new_assignment))
  , offset(o)
  , type(tp)
  , previous_assignment(std::move(previous)) {}

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp,
  std::vector<backend_operation> ops,
  reconciliation_status status)
  : ntp_reconciliation_state(
    std::move(ntp), std::move(ops), status, errc::success) {}

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp,
  std::vector<backend_operation> ops,
  reconciliation_status status,
  errc ec)
  : _ntp(std::move(ntp))
  , _backend_operations(std::move(ops))
  , _status(status)
  , _error(ec) {}

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp, cluster::errc ec)
  : ntp_reconciliation_state(
    std::move(ntp), {}, reconciliation_status::error, ec) {}

create_partititions_configuration::create_partititions_configuration(
  model::topic_namespace tp_ns, int32_t cnt)
  : tp_ns(std::move(tp_ns))
  , new_total_partition_count(cnt) {}

std::ostream& operator<<(std::ostream& o, const topic_configuration& cfg) {
    fmt::print(
      o,
      "{{ topic: {}, partition_count: {}, replication_factor: {}, "
      "properties: "
      "{}}}",
      cfg.tp_ns,
      cfg.partition_count,
      cfg.replication_factor,
      cfg.properties);

    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_properties& properties) {
    fmt::print(
      o,
      "{{ compression: {}, cleanup_policy_bitflags: {}, "
      "compaction_strategy: "
      "{}, retention_bytes: {}, retention_duration_ms: {}, segment_size: "
      "{}, "
      "timestamp_type: {}, recovery_enabled: {}, shadow_indexing: {} }}",
      properties.compression,
      properties.cleanup_policy_bitflags,
      properties.compaction_strategy,
      properties.retention_bytes,
      properties.retention_duration,
      properties.segment_size,
      properties.timestamp_type,
      properties.recovery,
      properties.shadow_indexing);

    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_result& r) {
    fmt::print(o, "topic: {}, result: {}", r.tp_ns, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration_invariants& c) {
    fmt::print(
      o,
      "{{ version: {}, node_id: {}, core_count: {} }}",
      c.version,
      c.node_id,
      c.core_count);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_assignment& p_as) {
    fmt::print(
      o,
      "{{ id: {}, group_id: {}, replicas: {} }}",
      p_as.id,
      p_as.group,
      p_as.replicas);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const topic_table_delta::op_type& tp) {
    switch (tp) {
    case topic_table_delta::op_type::add:
        return o << "addition";
    case topic_table_delta::op_type::del:
        return o << "deletion";
    case topic_table_delta::op_type::update:
        return o << "update";
    case topic_table_delta::op_type::update_finished:
        return o << "update_finished";
    case topic_table_delta::op_type::update_properties:
        return o << "update_properties";
    case topic_table_delta::op_type::add_non_replicable:
        return o << "add_non_replicable_addition";
    case topic_table_delta::op_type::del_non_replicable:
        return o << "del_non_replicable_deletion";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const topic_table_delta& d) {
    fmt::print(
      o,
      "{{type: {}, ntp: {}, offset: {}, new_assignment: {}, "
      "previous_assignment: {}}}",
      d.type,
      d.ntp,
      d.offset,
      d.new_assignment,
      d.previous_assignment);

    return o;
}

std::ostream& operator<<(std::ostream& o, const backend_operation& op) {
    fmt::print(
      o,
      "{{partition_assignment: {}, shard: {},  type: {}}}",
      op.p_as,
      op.source_shard,
      op.type);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cluster_config_delta_cmd_data& data) {
    fmt::print(
      o,
      "{{cluster_config_delta_cmd_data: {} upserts, {} removes)}}",
      data.upsert.size(),
      data.remove.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status& s) {
    fmt::print(
      o,
      "{{cluster_status: node {}, version: {}, restart: {} ({} invalid, {} "
      "unknown)}}",
      s.node,
      s.version,
      s.restart,
      s.invalid.size(),
      s.unknown.size());
    return o;
}

bool config_status::operator==(const config_status& rhs) const {
    return std::tie(node, version, restart, unknown, invalid)
           == std::tie(
             rhs.node, rhs.version, rhs.restart, rhs.unknown, rhs.invalid);
}

std::ostream& operator<<(std::ostream& o, const non_replicable_topic& d) {
    fmt::print(
      o, "{{Source topic: {}, non replicable topic: {}}}", d.source, d.name);
    return o;
}

std::ostream& operator<<(std::ostream& o, const reconciliation_status& s) {
    switch (s) {
    case reconciliation_status::done:
        return o << "done";
    case reconciliation_status::error:
        return o << "error";
    case reconciliation_status::in_progress:
        return o << "in_progress";
    }
    __builtin_unreachable();
}

std::ostream&
operator<<(std::ostream& o, const ntp_reconciliation_state& state) {
    fmt::print(
      o,
      "{{ntp: {}, backend_operations: {}, error: {}, status: {}}}",
      state._ntp,
      state._backend_operations,
      state._error,
      state._status);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const create_partititions_configuration& cfg) {
    fmt::print(
      o,
      "{{topic: {}, new total partition count: {}, custom assignments: {}}}",
      cfg.tp_ns,
      cfg.new_total_partition_count,
      cfg.custom_assignments);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const create_partititions_configuration_assignment& cpca) {
    fmt::print(
      o, "{{configuration: {}, assignments: {}}}", cpca.cfg, cpca.assignments);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const custom_assignable_topic_configuration& catc) {
    fmt::print(
      o,
      "{{configuration: {}, custom_assignments: {}}}",
      catc.cfg,
      catc.custom_assignments);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const custom_partition_assignment& cas) {
    fmt::print(o, "{{partition_id: {}, replicas: {}}}", cas.id, cas.replicas);
    return o;
}

std::ostream& operator<<(std::ostream& o, const leader_term& lt) {
    fmt::print(o, "{{leader: {}, term: {}}}", lt.leader, lt.term);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_update_action& fua) {
    std::string_view action_name;
    switch (fua.action) {
    case feature_update_action::action_t::complete_preparing:
        action_name = "complete_preparing";
        break;
    case feature_update_action::action_t::administrative_activate:
        action_name = "administrative_activate";
        break;
    case feature_update_action::action_t::administrative_deactivate:
        action_name = "administrative_deactivate";
        break;
    }

    fmt::print(o, "{{action {} {} }}", fua.feature_name, action_name);
    return o;
}

} // namespace cluster

namespace reflection {

void adl<cluster::topic_configuration>::to(
  iobuf& out, cluster::topic_configuration&& t) {
    int32_t version = -1;
    reflection::serialize(
      out,
      version,
      t.tp_ns,
      t.partition_count,
      t.replication_factor,
      t.properties.compression,
      t.properties.cleanup_policy_bitflags,
      t.properties.compaction_strategy,
      t.properties.timestamp_type,
      t.properties.segment_size,
      t.properties.retention_bytes,
      t.properties.retention_duration,
      t.properties.recovery,
      t.properties.shadow_indexing);
}

cluster::topic_configuration
adl<cluster::topic_configuration>::from(iobuf_parser& in) {
    // NOTE: The first field of the topic_configuration is a
    // model::ns which has length prefix which is always
    // positive.
    // We're using negative length value to encode version. So if
    // the first int32_t value is positive then we're dealing with
    // the old format. The negative value means that the new format
    // was used.
    auto version = adl<int32_t>{}.from(in.peek(4));
    if (version < 0) {
        // Consume version from stream
        in.skip(4);
        vassert(
          -1 == version,
          "topic_configuration version {} is not supported",
          version);
    } else {
        version = 0;
    }
    auto ns = model::ns(adl<ss::sstring>{}.from(in));
    auto topic = model::topic(adl<ss::sstring>{}.from(in));
    auto partition_count = adl<int32_t>{}.from(in);
    auto rf = adl<int16_t>{}.from(in);

    auto cfg = cluster::topic_configuration(
      std::move(ns), std::move(topic), partition_count, rf);

    cfg.properties.compression = adl<std::optional<model::compression>>{}.from(
      in);
    cfg.properties.cleanup_policy_bitflags
      = adl<std::optional<model::cleanup_policy_bitflags>>{}.from(in);
    cfg.properties.compaction_strategy
      = adl<std::optional<model::compaction_strategy>>{}.from(in);
    cfg.properties.timestamp_type
      = adl<std::optional<model::timestamp_type>>{}.from(in);
    cfg.properties.segment_size = adl<std::optional<size_t>>{}.from(in);
    cfg.properties.retention_bytes = adl<tristate<size_t>>{}.from(in);
    cfg.properties.retention_duration
      = adl<tristate<std::chrono::milliseconds>>{}.from(in);
    if (version < 0) {
        cfg.properties.recovery = adl<std::optional<bool>>{}.from(in);
        cfg.properties.shadow_indexing
          = adl<std::optional<model::shadow_indexing_mode>>{}.from(in);
    }
    return cfg;
}

void adl<cluster::join_request>::to(iobuf& out, cluster::join_request&& r) {
    adl<model::broker>().to(out, std::move(r.node));
}

cluster::join_request adl<cluster::join_request>::from(iobuf io) {
    return reflection::from_iobuf<cluster::join_request>(std::move(io));
}

cluster::join_request adl<cluster::join_request>::from(iobuf_parser& in) {
    return cluster::join_request(adl<model::broker>().from(in));
}

void adl<cluster::join_node_request>::to(
  iobuf& out, cluster::join_node_request&& r) {
    adl<uint8_t>().to(out, r.current_version);
    adl<cluster::cluster_version>().to(out, r.logical_version);
    adl<std::vector<uint8_t>>().to(out, r.node_uuid);
    adl<model::broker>().to(out, std::move(r.node));
}

cluster::join_node_request adl<cluster::join_node_request>::from(iobuf io) {
    return reflection::from_iobuf<cluster::join_node_request>(std::move(io));
}

cluster::join_node_request
adl<cluster::join_node_request>::from(iobuf_parser& in) {
    auto version = adl<uint8_t>().from(in);
    vassert(version >= 1, "Malformed join_node_request");
    auto logical_version = adl<cluster::cluster_version>().from(in);
    auto node_uuid = adl<std::vector<uint8_t>>().from(in);
    auto node = adl<model::broker>().from(in);

    return cluster::join_node_request{logical_version, node_uuid, node};
}

void adl<cluster::configuration_update_request>::to(
  iobuf& out, cluster::configuration_update_request&& r) {
    serialize(out, r.node, r.target_node);
}

cluster::configuration_update_request
adl<cluster::configuration_update_request>::from(iobuf_parser& in) {
    auto broker = adl<model::broker>().from(in);
    auto target_id = adl<model::node_id>().from(in);
    return cluster::configuration_update_request(broker, target_id);
}

void adl<cluster::topic_result>::to(iobuf& out, cluster::topic_result&& t) {
    reflection::serialize(out, std::move(t.tp_ns), t.ec);
}

cluster::topic_result adl<cluster::topic_result>::from(iobuf_parser& in) {
    auto tp_ns = adl<model::topic_namespace>{}.from(in);
    auto ec = adl<cluster::errc>{}.from(in);
    return cluster::topic_result(std::move(tp_ns), ec);
}

void adl<cluster::create_topics_request>::to(
  iobuf& out, cluster::create_topics_request&& r) {
    reflection::serialize(out, std::move(r.topics), r.timeout);
}

cluster::create_topics_request
adl<cluster::create_topics_request>::from(iobuf io) {
    return reflection::from_iobuf<cluster::create_topics_request>(
      std::move(io));
}

cluster::create_topics_request
adl<cluster::create_topics_request>::from(iobuf_parser& in) {
    using underlying_t = std::vector<cluster::topic_configuration>;
    auto configs = adl<underlying_t>().from(in);
    auto timeout = adl<model::timeout_clock::duration>().from(in);
    return cluster::create_topics_request{std::move(configs), timeout};
}

void adl<cluster::create_non_replicable_topics_request>::to(
  iobuf& out, cluster::create_non_replicable_topics_request&& r) {
    reflection::serialize(
      out,
      cluster::create_non_replicable_topics_request::current_version,
      std::move(r.topics),
      r.timeout);
}

cluster::create_non_replicable_topics_request
adl<cluster::create_non_replicable_topics_request>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::create_non_replicable_topics_request::current_version,
      "Unexpected version: {} (expected: {})",
      version,
      cluster::create_non_replicable_topics_request::current_version);
    auto topics = adl<std::vector<cluster::non_replicable_topic>>().from(in);
    auto timeout = adl<model::timeout_clock::duration>().from(in);
    return cluster::create_non_replicable_topics_request{
      std::move(topics), timeout};
}

void adl<cluster::create_non_replicable_topics_reply>::to(
  iobuf& out, cluster::create_non_replicable_topics_reply&& r) {
    reflection::serialize(
      out,
      cluster::create_non_replicable_topics_reply::current_version,
      std::move(r.results));
}

cluster::create_non_replicable_topics_reply
adl<cluster::create_non_replicable_topics_reply>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::create_non_replicable_topics_reply::current_version,
      "Unexpected version: {} (expected: {})",
      version,
      cluster::create_non_replicable_topics_reply::current_version);
    auto results = adl<std::vector<cluster::topic_result>>().from(in);
    return cluster::create_non_replicable_topics_reply{
      .results = std::move(results)};
}

void adl<cluster::create_topics_reply>::to(
  iobuf& out, cluster::create_topics_reply&& r) {
    reflection::serialize(
      out, std::move(r.results), std::move(r.metadata), std::move(r.configs));
}

cluster::create_topics_reply adl<cluster::create_topics_reply>::from(iobuf io) {
    return reflection::from_iobuf<cluster::create_topics_reply>(std::move(io));
}

cluster::create_topics_reply
adl<cluster::create_topics_reply>::from(iobuf_parser& in) {
    auto results = adl<std::vector<cluster::topic_result>>().from(in);
    auto md = adl<std::vector<model::topic_metadata>>().from(in);
    auto cfg = adl<std::vector<cluster::topic_configuration>>().from(in);
    return cluster::create_topics_reply{
      std::move(results), std::move(md), std::move(cfg)};
}

void adl<model::timeout_clock::duration>::to(iobuf& out, duration dur) {
    // This is a clang bug that cause ss::cpu_to_le to become ambiguous
    // because rep has type of long long
    // adl<rep>{}.to(out, dur.count());
    adl<uint64_t>{}.to(out, std::chrono::milliseconds(dur).count());
}

model::timeout_clock::duration
adl<model::timeout_clock::duration>::from(iobuf_parser& in) {
    // This is a clang bug that cause ss::cpu_to_le to become ambiguous
    // because rep has type of long long
    // auto rp = adl<rep>{}.from(in);
    auto rp = adl<uint64_t>{}.from(in);
    return duration(rp);
}

void adl<cluster::topic_configuration_assignment>::to(
  iobuf& b, cluster::topic_configuration_assignment&& assigned_cfg) {
    reflection::serialize(
      b, std::move(assigned_cfg.cfg), std::move(assigned_cfg.assignments));
}

cluster::topic_configuration_assignment
adl<cluster::topic_configuration_assignment>::from(iobuf_parser& in) {
    auto cfg = adl<cluster::topic_configuration>{}.from(in);
    auto assignments = adl<std::vector<cluster::partition_assignment>>{}.from(
      in);
    return cluster::topic_configuration_assignment(
      std::move(cfg), std::move(assignments));
}

void adl<cluster::configuration_invariants>::to(
  iobuf& out, cluster::configuration_invariants&& r) {
    reflection::serialize(out, r.version, r.node_id, r.core_count);
}

cluster::configuration_invariants
adl<cluster::configuration_invariants>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::configuration_invariants::current_version,
      "Currently only version 0 of configuration invariants is supported");

    auto node_id = adl<model::node_id>{}.from(parser);
    auto core_count = adl<uint16_t>{}.from(parser);

    cluster::configuration_invariants ret(node_id, core_count);

    return ret;
}

void adl<cluster::topic_properties_update>::to(
  iobuf& out, cluster::topic_properties_update&& r) {
    reflection::serialize(
      out, r.version, r.tp_ns, r.properties, r.custom_properties);
}

cluster::topic_properties_update
adl<cluster::topic_properties_update>::from(iobuf_parser& parser) {
    /**
     * We use the same versioning trick as for `cluster::topic_configuration`.
     *
     * NOTE: The first field of the topic_properties_update is a
     * model::topic_namespace. Serialized ss::string starts from either
     * int32_t for string length. We use negative version to encode new format
     * of incremental topic_properties_update
     */

    auto version = adl<int32_t>{}.from(parser.peek(4));
    if (version < 0) {
        // Consume version from stream
        parser.skip(4);
        vassert(
          version == cluster::topic_properties_update::version,
          "topic_properties_update version {} is not supported",
          version);
    } else {
        version = 0;
    }

    auto tp_ns = adl<model::topic_namespace>{}.from(parser);
    cluster::topic_properties_update ret(std::move(tp_ns));
    ret.properties = adl<cluster::incremental_topic_updates>{}.from(parser);
    if (version < 0) {
        ret.custom_properties
          = adl<cluster::incremental_topic_custom_updates>{}.from(parser);
    }

    return ret;
}

/*
 * Important information about ACL state serialization:
 *
 * The following serialization specializations are not part of a public
 * interface and are used to support the serialization of the public type
 * `cluster::create_acls_cmd_data` used by create acls api.
 *
 * They are private because they all depend on the embedded versioning of
 * `cluster::create_acls_cmd_data`, instead of their own independent versioning.
 * Because the same versioning applies to the entire AST rooted at this command
 * object type it should make transitions to the new serialization v2 much
 * simpler than having to deal with conversion of all of the constituent types.
 */
template<>
struct adl<security::acl_principal> {
    void to(iobuf& out, const security::acl_principal& p) {
        serialize(out, p.type(), p.name());
    }

    security::acl_principal from(iobuf_parser& in) {
        auto pt = adl<security::principal_type>{}.from(in);
        auto name = adl<ss::sstring>{}.from(in);
        return security::acl_principal(pt, std::move(name));
    }
};

template<>
struct adl<security::acl_host> {
    void to(iobuf& out, const security::acl_host& host) {
        bool ipv4 = false;
        std::optional<iobuf> data;
        if (host.address()) { // wildcard
            ipv4 = host.address()->is_ipv4();
            data = iobuf();
            data->append( // NOLINTNEXTLINE
              (const char*)host.address()->data(),
              host.address()->size());
        }
        serialize(out, ipv4, std::move(data));
    }

    security::acl_host from(iobuf_parser& in) {
        auto ipv4 = adl<bool>{}.from(in);
        auto opt_data = adl<std::optional<iobuf>>{}.from(in);

        if (opt_data) {
            auto data = iobuf_to_bytes(*opt_data);
            if (ipv4) {
                ::in_addr addr{};
                vassert(data.size() == sizeof(addr), "Unexpected ipv4 size");
                std::memcpy(&addr, data.c_str(), sizeof(addr));
                return security::acl_host(ss::net::inet_address(addr));
            } else {
                ::in6_addr addr{};
                vassert(data.size() == sizeof(addr), "Unexpected ipv6 size");
                std::memcpy(&addr, data.c_str(), sizeof(addr));
                return security::acl_host(ss::net::inet_address(addr));
            }
        }

        return security::acl_host::wildcard_host();
    }
};

template<>
struct adl<security::acl_entry> {
    void to(iobuf& out, const security::acl_entry& e) {
        serialize(out, e.principal(), e.host(), e.operation(), e.permission());
    }

    security::acl_entry from(iobuf_parser& in) {
        auto prin = adl<security::acl_principal>{}.from(in);
        auto host = adl<security::acl_host>{}.from(in);
        auto op = adl<security::acl_operation>{}.from(in);
        auto perm = adl<security::acl_permission>{}.from(in);
        return security::acl_entry(std::move(prin), host, op, perm);
    }
};

template<>
struct adl<security::resource_pattern> {
    void to(iobuf& out, const security::resource_pattern& b) {
        serialize(out, b.resource(), b.name(), b.pattern());
    }

    security::resource_pattern from(iobuf_parser& in) {
        auto r = adl<security::resource_type>{}.from(in);
        auto n = adl<ss::sstring>{}.from(in);
        auto p = adl<security::pattern_type>{}.from(in);
        return security::resource_pattern(r, std::move(n), p);
    }
};

template<>
struct adl<security::acl_binding> {
    void to(iobuf& out, const security::acl_binding& b) {
        serialize(out, b.pattern(), b.entry());
    }

    security::acl_binding from(iobuf_parser& in) {
        auto r = adl<security::resource_pattern>{}.from(in);
        auto e = adl<security::acl_entry>{}.from(in);
        return security::acl_binding(std::move(r), std::move(e));
    }
};

/*
 * A pattern_type_filter contains a normal pattern type in addition to a match
 * pattern type. However, match type isn't part of the enum for pattern type and
 * only makes sense in the context of the filter. To accomodate this we use a
 * variant type on the filter interface with tag dispatch, and as a result, need
 * special handling in serialization for this type.
 */
template<>
struct adl<security::resource_pattern_filter> {
    enum class pattern_type : int8_t {
        literal = 0,
        prefixed = 1,
        match = 2,
    };

    static pattern_type to_pattern(security::pattern_type from) {
        switch (from) {
        case security::pattern_type::literal:
            return pattern_type::literal;
        case security::pattern_type::prefixed:
            return pattern_type::prefixed;
        }
        __builtin_unreachable();
    }

    void to(iobuf& out, const security::resource_pattern_filter& b) {
        std::optional<pattern_type> pattern;
        if (b.pattern()) {
            if (std::holds_alternative<
                  security::resource_pattern_filter::pattern_match>(
                  *b.pattern())) {
                pattern = pattern_type::match;
            } else {
                auto source_pattern = std::get<security::pattern_type>(
                  *b.pattern());
                pattern = to_pattern(source_pattern);
            }
        }
        serialize(out, b.resource(), b.name(), pattern);
    }

    security::resource_pattern_filter from(iobuf_parser& in) {
        auto resource = adl<std::optional<security::resource_type>>{}.from(in);
        auto name = adl<std::optional<ss::sstring>>{}.from(in);
        auto pattern = adl<std::optional<pattern_type>>{}.from(in);

        if (!pattern) {
            return security::resource_pattern_filter(
              resource, std::move(name), std::nullopt);
        }

        switch (*pattern) {
        case pattern_type::literal:
            return security::resource_pattern_filter(
              resource, std::move(name), security::pattern_type::literal);

        case pattern_type::prefixed:
            return security::resource_pattern_filter(
              resource, std::move(name), security::pattern_type::prefixed);

        case pattern_type::match:
            return security::resource_pattern_filter(
              resource,
              std::move(name),
              security::resource_pattern_filter::pattern_match{});
        }
        __builtin_unreachable();
    }
};

template<>
struct adl<security::acl_entry_filter> {
    void to(iobuf& out, const security::acl_entry_filter& f) {
        serialize(out, f.principal(), f.host(), f.operation(), f.permission());
    }

    security::acl_entry_filter from(iobuf_parser& in) {
        auto prin = adl<std::optional<security::acl_principal>>{}.from(in);
        auto host = adl<std::optional<security::acl_host>>{}.from(in);
        auto op = adl<std::optional<security::acl_operation>>{}.from(in);
        auto perm = adl<std::optional<security::acl_permission>>{}.from(in);
        return security::acl_entry_filter(std::move(prin), host, op, perm);
    }
};

template<>
struct adl<security::acl_binding_filter> {
    void to(iobuf& out, const security::acl_binding_filter& b) {
        serialize(out, b.pattern(), b.entry());
    }

    security::acl_binding_filter from(iobuf_parser& in) {
        auto r = adl<security::resource_pattern_filter>{}.from(in);
        auto e = adl<security::acl_entry_filter>{}.from(in);
        return security::acl_binding_filter(std::move(r), std::move(e));
    }
};

void adl<cluster::create_acls_cmd_data>::to(
  iobuf& out, cluster::create_acls_cmd_data&& data) {
    adl<int8_t>{}.to(out, cluster::create_acls_cmd_data::current_version);
    serialize(out, std::move(data.bindings));
}

cluster::create_acls_cmd_data
adl<cluster::create_acls_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::create_acls_cmd_data::current_version,
      "Unexpected create acls cmd version {} (expected {})",
      version,
      cluster::create_acls_cmd_data::current_version);
    auto bindings = adl<std::vector<security::acl_binding>>().from(in);
    return cluster::create_acls_cmd_data{
      .bindings = std::move(bindings),
    };
}

void adl<cluster::delete_acls_cmd_data>::to(
  iobuf& out, cluster::delete_acls_cmd_data&& data) {
    adl<int8_t>{}.to(out, cluster::delete_acls_cmd_data::current_version);
    serialize(out, std::move(data.filters));
}

cluster::delete_acls_cmd_data
adl<cluster::delete_acls_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::delete_acls_cmd_data::current_version,
      "Unexpected delete acls cmd version {} (expected {})",
      version,
      cluster::delete_acls_cmd_data::current_version);
    auto filters = adl<std::vector<security::acl_binding_filter>>().from(in);
    return cluster::delete_acls_cmd_data{
      .filters = std::move(filters),
    };
}

void adl<cluster::delete_acls_result>::to(
  iobuf& out, cluster::delete_acls_result&& result) {
    serialize(out, result.error, std::move(result.bindings));
}

cluster::delete_acls_result
adl<cluster::delete_acls_result>::from(iobuf_parser& in) {
    auto error = adl<cluster::errc>{}.from(in);
    auto bindings = adl<std::vector<security::acl_binding>>().from(in);
    return cluster::delete_acls_result{
      .error = error,
      .bindings = std::move(bindings),
    };
}

void adl<cluster::ntp_reconciliation_state>::to(
  iobuf& b, cluster::ntp_reconciliation_state&& state) {
    auto ntp = state.ntp();
    reflection::serialize(
      b,
      std::move(ntp),
      state.pending_operations(),
      state.status(),
      state.cluster_errc());
}

cluster::ntp_reconciliation_state
adl<cluster::ntp_reconciliation_state>::from(iobuf_parser& in) {
    auto ntp = adl<model::ntp>{}.from(in);
    auto ops = adl<std::vector<cluster::backend_operation>>{}.from(in);
    auto status = adl<cluster::reconciliation_status>{}.from(in);
    auto error = adl<cluster::errc>{}.from(in);

    return cluster::ntp_reconciliation_state(
      std::move(ntp), std::move(ops), status, error);
}

void adl<cluster::create_partititions_configuration>::to(
  iobuf& out, cluster::create_partititions_configuration&& pc) {
    return serialize(
      out, pc.tp_ns, pc.new_total_partition_count, pc.custom_assignments);
}

cluster::create_partititions_configuration
adl<cluster::create_partititions_configuration>::from(iobuf_parser& in) {
    auto tp_ns = adl<model::topic_namespace>{}.from(in);
    auto partition_count = adl<int32_t>{}.from(in);
    auto custom_assignment = adl<std::vector<
      cluster::create_partititions_configuration::custom_assignment>>{}
                               .from(in);

    cluster::create_partititions_configuration ret(
      std::move(tp_ns), partition_count);
    ret.custom_assignments = std::move(custom_assignment);
    return ret;
}

void adl<cluster::create_partititions_configuration_assignment>::to(
  iobuf& out, cluster::create_partititions_configuration_assignment&& ca) {
    return serialize(out, std::move(ca.cfg), std::move(ca.assignments));
}

cluster::create_partititions_configuration_assignment
adl<cluster::create_partititions_configuration_assignment>::from(
  iobuf_parser& in) {
    auto cfg = adl<cluster::create_partititions_configuration>{}.from(in);
    auto p_as = adl<std::vector<cluster::partition_assignment>>{}.from(in);

    return cluster::create_partititions_configuration_assignment(
      std::move(cfg), std::move(p_as));
};

void adl<cluster::create_data_policy_cmd_data>::to(
  iobuf& out, cluster::create_data_policy_cmd_data&& dp_cmd_data) {
    return serialize(
      out, dp_cmd_data.current_version, std::move(dp_cmd_data.dp));
}

cluster::create_data_policy_cmd_data
adl<cluster::create_data_policy_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::create_data_policy_cmd_data::current_version,
      "Unexpected set_data_policy_cmd version {} (expected {})",
      version,
      cluster::create_data_policy_cmd_data::current_version);
    auto dp = adl<v8_engine::data_policy>{}.from(in);
    return cluster::create_data_policy_cmd_data{.dp = std::move(dp)};
}

void adl<cluster::non_replicable_topic>::to(
  iobuf& out, cluster::non_replicable_topic&& cm_cmd_data) {
    return serialize(
      out,
      cm_cmd_data.current_version,
      std::move(cm_cmd_data.source),
      std::move(cm_cmd_data.name));
}

cluster::non_replicable_topic
adl<cluster::non_replicable_topic>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::non_replicable_topic::current_version,
      "Unexpected version: {} (expected: {})",
      version,
      cluster::non_replicable_topic::current_version);
    auto source = adl<model::topic_namespace>{}.from(in);
    auto name = adl<model::topic_namespace>{}.from(in);
    return cluster::non_replicable_topic{
      .source = std::move(source), .name = std::move(name)};
}

void adl<cluster::incremental_topic_updates>::to(
  iobuf& out, cluster::incremental_topic_updates&& t) {
    reflection::serialize(
      out,
      cluster::incremental_topic_updates::version,
      t.compression,
      t.cleanup_policy_bitflags,
      t.compaction_strategy,
      t.timestamp_type,
      t.segment_size,
      t.retention_bytes,
      t.retention_duration,
      t.shadow_indexing);
}

cluster::incremental_topic_updates
adl<cluster::incremental_topic_updates>::from(iobuf_parser& in) {
    /**
     * We use the same versioning trick as for `cluster::topic_configuration`.
     *
     * NOTE: The first field of the incremental_topic_updates is a
     * property_value<std::optional<model::compression>>. Serialized
     * std::optional starts from either 0 or 1 (int8_t). We use negative version
     * to encode new format of incremental topic updates
     */

    auto version = adl<int8_t>{}.from(in.peek(1));
    if (version < 0) {
        // Consume version from stream
        in.skip(1);
        vassert(
          version >= cluster::incremental_topic_updates::version,
          "topic_configuration version {} is not supported",
          version);
    } else {
        version = 0;
    }

    cluster::incremental_topic_updates updates;
    updates.compression
      = adl<cluster::property_update<std::optional<model::compression>>>{}.from(
        in);
    updates.cleanup_policy_bitflags = adl<cluster::property_update<
      std::optional<model::cleanup_policy_bitflags>>>{}
                                        .from(in);
    updates.compaction_strategy
      = adl<
          cluster::property_update<std::optional<model::compaction_strategy>>>{}
          .from(in);
    updates.timestamp_type
      = adl<cluster::property_update<std::optional<model::timestamp_type>>>{}
          .from(in);
    updates.segment_size
      = adl<cluster::property_update<std::optional<size_t>>>{}.from(in);
    updates.retention_bytes
      = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
    updates.retention_duration
      = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
          .from(in);

    if (
      version == cluster::incremental_topic_updates::version_with_data_policy) {
        // data_policy property from update_topic_properties_cmd is never used.
        // data_policy_frontend replicates this property and store it to
        // create_data_policy_cmd_data, data_policy_manager handles it
        adl<cluster::property_update<std::optional<v8_engine::data_policy>>>{}
          .from(in);
    }
    if (
      version
      <= cluster::incremental_topic_updates::version_with_shadow_indexing) {
        updates.shadow_indexing = adl<cluster::property_update<
          std::optional<model::shadow_indexing_mode>>>{}
                                    .from(in);
    }
    return updates;
}

void adl<cluster::config_status>::to(iobuf& out, cluster::config_status&& s) {
    return serialize(out, s.node, s.version, s.restart, s.unknown, s.invalid);
}

cluster::config_status adl<cluster::config_status>::from(iobuf_parser& in) {
    auto node_id = adl<model::node_id>().from(in);
    auto version = adl<cluster::config_version>().from(in);
    auto restart = adl<bool>().from(in);
    auto unknown = adl<std::vector<ss::sstring>>().from(in);
    auto invalid = adl<std::vector<ss::sstring>>().from(in);

    return cluster::config_status{
      .node = std::move(node_id),
      .version = std::move(version),
      .restart = std::move(restart),
      .unknown = std::move(unknown),
      .invalid = std::move(invalid),
    };
}

void adl<cluster::cluster_config_delta_cmd_data>::to(
  iobuf& out, cluster::cluster_config_delta_cmd_data&& cmd) {
    return serialize(
      out, cmd.current_version, std::move(cmd.upsert), std::move(cmd.remove));
}

cluster::cluster_config_delta_cmd_data
adl<cluster::cluster_config_delta_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::cluster_config_delta_cmd_data::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::cluster_config_delta_cmd_data::current_version);
    auto upsert = adl<std::vector<std::pair<ss::sstring, ss::sstring>>>().from(
      in);
    auto remove = adl<std::vector<ss::sstring>>().from(in);

    return cluster::cluster_config_delta_cmd_data{
      .upsert = std::move(upsert),
      .remove = std::move(remove),
    };
}

void adl<cluster::cluster_config_status_cmd_data>::to(
  iobuf& out, cluster::cluster_config_status_cmd_data&& cmd) {
    return serialize(out, cmd.current_version, std::move(cmd.status));
}

cluster::cluster_config_status_cmd_data
adl<cluster::cluster_config_status_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::cluster_config_status_cmd_data::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::cluster_config_status_cmd_data::current_version);
    auto status = adl<cluster::config_status>().from(in);

    return cluster::cluster_config_status_cmd_data{
      .status = std::move(status),
    };
}

void adl<cluster::feature_update_action>::to(
  iobuf& out, cluster::feature_update_action&& cmd) {
    return serialize(
      out,
      cmd.current_version,
      std::move(cmd.feature_name),
      std::move(cmd.action));
}

cluster::feature_update_action
adl<cluster::feature_update_action>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::feature_update_action::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::feature_update_action::current_version);
    auto feature_name = adl<ss::sstring>().from(in);
    auto action = adl<cluster::feature_update_action::action_t>().from(in);

    return cluster::feature_update_action{
      .feature_name = std::move(feature_name),
      .action = std::move(action),
    };
}

void adl<cluster::incremental_topic_custom_updates>::to(
  iobuf& out, cluster::incremental_topic_custom_updates&& t) {
    reflection::serialize(out, t.data_policy);
}

cluster::incremental_topic_custom_updates
adl<cluster::incremental_topic_custom_updates>::from(iobuf_parser& in) {
    cluster::incremental_topic_custom_updates updates;
    updates.data_policy
      = adl<cluster::property_update<std::optional<v8_engine::data_policy>>>{}
          .from(in);
    return updates;
}

void adl<cluster::feature_update_cmd_data>::to(
  iobuf& out, cluster::feature_update_cmd_data&& data) {
    reflection::serialize(
      out, data.current_version, data.logical_version, data.actions);
}

cluster::feature_update_cmd_data
adl<cluster::feature_update_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    std::ignore = version;

    auto logical_version = adl<cluster::cluster_version>{}.from(in);
    auto actions = adl<std::vector<cluster::feature_update_action>>{}.from(in);
    return {.logical_version = logical_version, .actions = std::move(actions)};
}

void adl<cluster::feature_barrier_request>::to(
  iobuf& out, cluster::feature_barrier_request&& r) {
    reflection::serialize(out, r.current_version, r.tag, r.peer, r.entered);
}

cluster::feature_barrier_request
adl<cluster::feature_barrier_request>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::feature_barrier_request::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::feature_barrier_request::current_version);

    auto tag = adl<cluster::feature_barrier_tag>{}.from(parser);
    auto peer = adl<model::node_id>{}.from(parser);
    auto entered = adl<bool>{}.from(parser);

    return cluster::feature_barrier_request{
      .tag = std::move(tag), .peer = peer, .entered = entered};
}

void adl<cluster::feature_barrier_response>::to(
  iobuf& out, cluster::feature_barrier_response&& r) {
    reflection::serialize(out, r.current_version, r.entered, r.complete);
}

cluster::feature_barrier_response
adl<cluster::feature_barrier_response>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::feature_barrier_response::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::feature_barrier_response::current_version);

    auto entered = adl<bool>{}.from(parser);
    auto complete = adl<bool>{}.from(parser);

    return cluster::feature_barrier_response{
      .entered = entered, .complete = complete};
}

void adl<cluster::set_maintenance_mode_request>::to(
  iobuf& out, cluster::set_maintenance_mode_request&& r) {
    reflection::serialize(out, r.current_version, r.id, r.enabled);
}

cluster::set_maintenance_mode_request
adl<cluster::set_maintenance_mode_request>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::set_maintenance_mode_request::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::set_maintenance_mode_request::current_version);

    auto id = adl<model::node_id>{}.from(parser);
    auto enabled = adl<bool>{}.from(parser);

    return cluster::set_maintenance_mode_request{.id = id, .enabled = enabled};
}

void adl<cluster::set_maintenance_mode_reply>::to(
  iobuf& out, cluster::set_maintenance_mode_reply&& r) {
    reflection::serialize(out, r.current_version, r.error);
}

cluster::set_maintenance_mode_reply
adl<cluster::set_maintenance_mode_reply>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::set_maintenance_mode_reply::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::set_maintenance_mode_reply::current_version);

    auto error = adl<cluster::errc>{}.from(parser);

    return cluster::set_maintenance_mode_reply{.error = error};
}

} // namespace reflection
