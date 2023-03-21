// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"

#include "cluster/fwd.h"
#include "config/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "security/acl.h"
#include "tristate.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

namespace cluster {

std::ostream& operator<<(std::ostream& o, const tx_errc& err) {
    switch (err) {
    case tx_errc::none:
        o << "tx_errc::none";
        break;
    case tx_errc::leader_not_found:
        o << "tx_errc::leader_not_found";
        break;
    case tx_errc::shard_not_found:
        o << "tx_errc::shard_not_found";
        break;
    case tx_errc::partition_not_found:
        o << "tx_errc::partition_not_found";
        break;
    case tx_errc::stm_not_found:
        o << "tx_errc::stm_not_found";
        break;
    case tx_errc::partition_not_exists:
        o << "tx_errc::partition_not_exists";
        break;
    case tx_errc::pid_not_found:
        o << "tx_errc::pid_not_found";
        break;
    case tx_errc::timeout:
        o << "tx_errc::timeout";
        break;
    case tx_errc::conflict:
        o << "tx_errc::conflict";
        break;
    case tx_errc::fenced:
        o << "tx_errc::fenced";
        break;
    case tx_errc::stale:
        o << "tx_errc::stale";
        break;
    case tx_errc::not_coordinator:
        o << "tx_errc::not_coordinator";
        break;
    case tx_errc::coordinator_not_available:
        o << "tx_errc::coordinator_not_available";
        break;
    case tx_errc::preparing_rebalance:
        o << "tx_errc::preparing_rebalance";
        break;
    case tx_errc::rebalance_in_progress:
        o << "tx_errc::rebalance_in_progress";
        break;
    case tx_errc::coordinator_load_in_progress:
        o << "tx_errc::coordinator_load_in_progress";
        break;
    case tx_errc::unknown_server_error:
        o << "tx_errc::unknown_server_error";
        break;
    case tx_errc::request_rejected:
        o << "tx_errc::request_rejected";
        break;
    case tx_errc::invalid_producer_epoch:
        o << "tx_errc::invalid_producer_epoch";
        break;
    case tx_errc::invalid_txn_state:
        o << "tx_errc::invalid_txn_state";
        break;
    case tx_errc::invalid_producer_id_mapping:
        o << "tx_errc::invalid_producer_id_mapping";
        break;
    case tx_errc::tx_not_found:
        o << "tx_errc::tx_not_found";
        break;
    case tx_errc::tx_id_not_found:
        o << "tx_errc::tx_id_not_found";
        break;
    }
    return o;
}

std::string tx_errc_category::message(int c) const {
    return fmt::format("{{{}}}", static_cast<tx_errc>(c));
}

kafka_stages::kafka_stages(
  ss::future<> enq, ss::future<result<kafka_result>> offset_future)
  : request_enqueued(std::move(enq))
  , replicate_finished(std::move(offset_future)) {}

kafka_stages::kafka_stages(raft::errc ec)
  : request_enqueued(ss::now())
  , replicate_finished(
      ss::make_ready_future<result<kafka_result>>(make_error_code(ec))){};

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
           || retention_bytes.is_engaged() || retention_duration.is_engaged()
           || recovery.has_value() || shadow_indexing.has_value()
           || read_replica.has_value() || batch_max_bytes.has_value()
           || retention_local_target_bytes.is_engaged()
           || retention_local_target_ms.is_engaged()
           || remote_delete != storage::ntp_config::default_remote_delete
           || segment_ms.is_engaged();
}

storage::ntp_config::default_overrides
topic_properties::get_ntp_cfg_overrides() const {
    storage::ntp_config::default_overrides ret;
    ret.cleanup_policy_bitflags = cleanup_policy_bitflags;
    ret.compaction_strategy = compaction_strategy;
    ret.retention_bytes = retention_bytes;
    ret.retention_time = retention_duration;
    ret.segment_size = segment_size;
    ret.shadow_indexing_mode = shadow_indexing;
    ret.read_replica = read_replica;
    ret.retention_local_target_bytes = retention_local_target_bytes;
    ret.retention_local_target_ms = retention_local_target_ms;
    ret.remote_delete = remote_delete;
    ret.segment_ms = segment_ms;
    return ret;
}

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
            .shadow_indexing_mode = properties.shadow_indexing,
            .read_replica = properties.read_replica,
            .retention_local_target_bytes
            = properties.retention_local_target_bytes,
            .retention_local_target_ms = properties.retention_local_target_ms,
            .remote_delete = properties.remote_delete,
            .segment_ms = properties.segment_ms,
          });
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

    return ret;
}

topic_table_delta::topic_table_delta(
  model::ntp ntp,
  cluster::partition_assignment new_assignment,
  model::offset o,
  op_type tp,
  std::optional<std::vector<model::broker_shard>> previous,
  std::optional<replicas_revision_map> replica_revisions)
  : ntp(std::move(ntp))
  , new_assignment(std::move(new_assignment))
  , offset(o)
  , type(tp)
  , previous_replica_set(std::move(previous))
  , replica_revisions(std::move(replica_revisions)) {}

model::revision_id
topic_table_delta::get_replica_revision(model::node_id replica) const {
    vassert(
      replica_revisions, "ntp {}: replica_revisions map must be present", ntp);
    auto rev_it = replica_revisions->find(replica);
    vassert(
      rev_it != replica_revisions->end(),
      "ntp {}: node {} must be present in the replica_revisions map",
      ntp,
      replica);
    return rev_it->second;
}

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

create_partitions_configuration::create_partitions_configuration(
  model::topic_namespace tp_ns, int32_t cnt)
  : tp_ns(std::move(tp_ns))
  , new_total_partition_count(cnt) {}

std::ostream&
operator<<(std::ostream& o, const update_topic_properties_request& r) {
    fmt::print(o, "{{updates: {}}}", r.updates);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const update_topic_properties_reply& r) {
    fmt::print(o, "{{results: {}}}", r.results);
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_properties_update& tpu) {
    fmt::print(
      o,
      "tp_ns: {} properties: {} custom_properties: {}",
      tpu.tp_ns,
      tpu.properties,
      tpu.custom_properties);
    return o;
}

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
      "{{compression: {}, cleanup_policy_bitflags: {}, compaction_strategy: "
      "{}, retention_bytes: {}, retention_duration_ms: {}, segment_size: {}, "
      "timestamp_type: {}, recovery_enabled: {}, shadow_indexing: {}, "
      "read_replica: {}, read_replica_bucket: {} remote_topic_properties: {}, "
      "batch_max_bytes: {}, retention_local_target_bytes: {}, "
      "retention_local_target_ms: {}, remote_delete: {}, segment_ms: {}}}",
      properties.compression,
      properties.cleanup_policy_bitflags,
      properties.compaction_strategy,
      properties.retention_bytes,
      properties.retention_duration,
      properties.segment_size,
      properties.timestamp_type,
      properties.recovery,
      properties.shadow_indexing,
      properties.read_replica,
      properties.read_replica_bucket,
      properties.remote_topic_properties,
      properties.batch_max_bytes,
      properties.retention_local_target_bytes,
      properties.retention_local_target_ms,
      properties.remote_delete,
      properties.segment_ms);

    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_result& r) {
    fmt::print(o, "topic: {}, result: {}", r.tp_ns, r.ec);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const finish_partition_update_request& r) {
    fmt::print(o, "{{ntp: {}, new_replica_set: {}}}", r.ntp, r.new_replica_set);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const finish_partition_update_reply& r) {
    fmt::print(o, "{{result: {}}}", r.result);
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
    case topic_table_delta::op_type::reset:
        return o << "reset";
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
    case topic_table_delta::op_type::cancel_update:
        return o << "cancel_update";
    case topic_table_delta::op_type::force_abort_update:
        return o << "force_abort_update";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const topic_table_delta& d) {
    fmt::print(
      o,
      "{{type: {}, ntp: {}, offset: {}, new_assignment: {}, "
      "previous_replica_set: {}, replica_revisions: {}}}",
      d.type,
      d.ntp,
      d.offset,
      d.new_assignment,
      d.previous_replica_set,
      d.replica_revisions);

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

std::ostream& operator<<(std::ostream& o, const begin_tx_request& r) {
    fmt::print(
      o,
      "{{ ntp: {}, pid: {}, tx_seq: {}, tm_partition: {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.tm_partition);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_tx_reply& r) {
    fmt::print(o, "{{ ntp: {}, etag: {}, ec: {} }}", r.ntp, r.etag, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_tx_request& r) {
    fmt::print(
      o,
      "{{ ntp: {}, etag: {}, tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      r.ntp,
      r.etag,
      r.tm,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_tx_reply& r) {
    fmt::print(o, "{{ ec: {} }}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const init_tm_tx_request& r) {
    fmt::print(
      o,
      "{{ tx_id: {}, transaction_timeout_ms: {}, timeout: {} }}",
      r.tx_id,
      r.transaction_timeout_ms,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const init_tm_tx_reply& r) {
    fmt::print(o, "{{ pid: {}, ec: {} }}", r.pid, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const try_abort_request& r) {
    fmt::print(
      o,
      "{{ tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      r.tm,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const try_abort_reply& r) {
    fmt::print(
      o,
      "{{ commited: {}, aborted: {}, ec: {} }}",
      bool(r.commited),
      bool(r.aborted),
      r.ec);
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

std::ostream& operator<<(std::ostream& o, const cluster_property_kv& kv) {
    fmt::print(
      o, "{{cluster_property_kv: key {}, value: {})}}", kv.key, kv.value);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_update_request& crq) {
    fmt::print(
      o,
      "{{config_update_request: upsert {}, remove: {})}}",
      crq.upsert,
      crq.remove);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_update_reply& crr) {
    fmt::print(
      o,
      "{{config_update_reply: error {}, latest_version: {})}}",
      crr.error,
      crr.latest_version);
    return o;
}

std::ostream& operator<<(std::ostream& o, const hello_request& h) {
    fmt::print(
      o, "{{hello_request: peer {}, start_time: {})}}", h.peer, h.start_time);
    return o;
}

std::ostream& operator<<(std::ostream& o, const hello_reply& h) {
    fmt::print(o, "{{hello_reply: error {}}}", h.error);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_topics_request& r) {
    fmt::print(
      o,
      "{{create_topics_request: topics: {} timeout: {}}}",
      r.topics,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_topics_reply& r) {
    fmt::print(
      o,
      "{{create_topics_reply: results: {} metadata: {} configs: {}}}",
      r.results,
      r.metadata,
      r.configs);
    return o;
}

std::ostream& operator<<(std::ostream& o, const incremental_topic_updates& i) {
    fmt::print(
      o,
      "{{incremental_topic_custom_updates: compression: {} "
      "cleanup_policy_bitflags: {} compaction_strategy: {} timestamp_type: {} "
      "segment_size: {} retention_bytes: {} retention_duration: {} "
      "shadow_indexing: {}, batch_max_bytes: {}, retention_local_target_bytes: "
      "{}, retention_local_target_ms: {}, remote_delete: {}, segment_ms: {}}}",
      i.compression,
      i.cleanup_policy_bitflags,
      i.compaction_strategy,
      i.timestamp_type,
      i.segment_size,
      i.retention_bytes,
      i.retention_duration,
      i.shadow_indexing,
      i.batch_max_bytes,
      i.retention_local_target_bytes,
      i.retention_local_target_ms,
      i.remote_delete,
      i.segment_ms);
    return o;
}

std::istream& operator>>(std::istream& i, replication_factor& cs) {
    ss::sstring s;
    i >> s;
    cs = replication_factor(boost::lexical_cast<replication_factor::type>(s));
    return i;
};

replication_factor parsing_replication_factor(const ss::sstring& value) {
    auto raw_value = boost::lexical_cast<int32_t>(value);
    if (
      raw_value <= 0
      || raw_value
           > std::numeric_limits<cluster::replication_factor::type>::max()) {
        throw boost::bad_lexical_cast();
    }

    return cluster::replication_factor(raw_value);
}

std::ostream&
operator<<(std::ostream& o, const incremental_topic_custom_updates& i) {
    fmt::print(
      o,
      "{{incremental_topic_custom_updates: data_policy: {}, "
      "replication_factor: {}}}",
      i.data_policy,
      i.replication_factor);
    return o;
}

namespace {
cluster::assignments_set to_assignments_map(
  std::vector<cluster::partition_assignment> assignment_vector) {
    cluster::assignments_set ret;
    for (auto& p_as : assignment_vector) {
        ret.emplace(std::move(p_as));
    }
    return ret;
}
} // namespace

topic_metadata_fields::topic_metadata_fields(
  topic_configuration cfg,
  std::optional<model::topic> st,
  model::revision_id rid,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : configuration(std::move(cfg))
  , source_topic(std::move(st))
  , revision(rid)
  , remote_revision(remote_revision_id) {}

topic_metadata::topic_metadata(
  topic_configuration_assignment c,
  model::revision_id rid,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : _fields(std::move(c.cfg), std::nullopt, rid, remote_revision_id)
  , _assignments(to_assignments_map(std::move(c.assignments))) {}

topic_metadata::topic_metadata(
  topic_configuration cfg,
  assignments_set assignments,
  model::revision_id rid,
  model::topic st,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : _fields(std::move(cfg), std::move(st), rid, remote_revision_id)
  , _assignments(std::move(assignments)) {}

topic_metadata::topic_metadata(
  topic_metadata_fields fields, assignments_set assignments) noexcept
  : _fields(std::move(fields))
  , _assignments(std::move(assignments)) {}

bool topic_metadata::is_topic_replicable() const {
    return _fields.source_topic.has_value() == false;
}

model::revision_id topic_metadata::get_revision() const {
    vassert(
      is_topic_replicable(), "Query for revision_id on a non-replicable topic");
    return _fields.revision;
}

std::optional<model::initial_revision_id>
topic_metadata::get_remote_revision() const {
    vassert(
      is_topic_replicable(), "Query for revision_id on a non-replicable topic");
    return _fields.remote_revision;
}

const model::topic& topic_metadata::get_source_topic() const {
    vassert(
      !is_topic_replicable(), "Query for source_topic on a replicable topic");
    return _fields.source_topic.value();
}

const topic_configuration& topic_metadata::get_configuration() const {
    return _fields.configuration;
}

const assignments_set& topic_metadata::get_assignments() const {
    return _assignments;
}

topic_configuration& topic_metadata::get_configuration() {
    return _fields.configuration;
}

assignments_set& topic_metadata::get_assignments() { return _assignments; }

replication_factor topic_metadata::get_replication_factor() const {
    // The main idea is do not use anymore replication_factor from topic_config.
    // replication factor is dynamic property. And it is size of assigments set.
    // So we will return rf for 0 partition, becasue it should exist for each
    // topic
    auto it = _assignments.find(model::partition_id(0));
    return replication_factor(
      static_cast<replication_factor::type>(it->replicas.size()));
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
operator<<(std::ostream& o, const create_partitions_configuration& cfg) {
    fmt::print(
      o,
      "{{topic: {}, new total partition count: {}, custom assignments: {}}}",
      cfg.tp_ns,
      cfg.new_total_partition_count,
      cfg.custom_assignments);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const create_partitions_configuration_assignment& cpca) {
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

std::ostream& operator<<(std::ostream& o, const partition_move_direction& s) {
    switch (s) {
    case partition_move_direction::to_node:
        return o << "to_node";
    case partition_move_direction::from_node:
        return o << "from_node";
    case partition_move_direction::all:
        return o << "all";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const move_cancellation_result& r) {
    fmt::print(o, "{{ntp: {}, result: {}}}", r.ntp, r.result);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cancel_node_partition_movements_request& r) {
    fmt::print(o, "{{node_id: {}, direction: {}}}", r.node_id, r.direction);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cancel_partition_movements_reply& r) {
    fmt::print(
      o,
      "{{general_error: {}, partition_results: {}}}",
      r.general_error,
      r.partition_results);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_action_request& far) {
    fmt::print(o, "{{feature_update_request: {}}}", far.action);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_action_response& far) {
    fmt::print(o, "{{error: {}}}", far.error);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_barrier_request& fbr) {
    fmt::print(
      o, "{{tag: {} peer: {} entered: {}}}", fbr.tag, fbr.peer, fbr.entered);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_barrier_response& fbr) {
    fmt::print(o, "{{entered: {} complete: {}}}", fbr.entered, fbr.complete);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const create_non_replicable_topics_request& r) {
    fmt::print(o, "{{topics: {} timeout: {}}}", r.topics, r.timeout);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const create_non_replicable_topics_reply& r) {
    fmt::print(o, "{{results: {}}}", r.results);
    return o;
}

std::ostream& operator<<(std::ostream& o, const move_topic_replicas_data& r) {
    fmt::print(o, "{{partition: {}, replicas: {}}}", r.partition, r.replicas);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const feature_update_license_update_cmd_data& fulu) {
    fmt::print(o, "{{redpanda_license {}}}", fulu.redpanda_license);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status_request& r) {
    fmt::print(o, "{{status: {}}}", r.status);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status_reply& r) {
    fmt::print(o, "{{error: {}}}", r.error);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} group_id {} pid {} tx_seq {} timeout {} tm_partition: {}}}",
      r.ntp,
      r.group_id,
      r.pid,
      r.tx_seq,
      r.timeout,
      r.tm_partition);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_group_tx_reply& r) {
    fmt::print(o, "{{etag {} ec {}}}", r.etag, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} group_id {} etag {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.group_id,
      r.etag,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.group_id,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.group_id,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const configuration_update_request& cr) {
    fmt::print(o, "{{broker: {} target_node: {}}}", cr.node, cr.target_node);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const configuration_update_reply& cr) {
    fmt::print(o, "{{success: {}}}", cr.success);
    return o;
}

std::ostream& operator<<(std::ostream& o, const remote_topic_properties& rtps) {
    fmt::print(
      o,
      "{{remote_revision: {} remote_partition_count: {}}}",
      rtps.remote_revision,
      rtps.remote_partition_count);
    return o;
}

std::ostream& operator<<(std::ostream& o, const broker_state& state) {
    fmt::print(
      o,
      "{{membership_state: {}, maintenance_state: {}}}",
      state._membership_state,
      state._maintenance_state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_metadata& nm) {
    fmt::print(o, "{{broker: {}, state: {} }}", nm.broker, nm.state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_update_type& tp) {
    switch (tp) {
    case node_update_type::added:
        return o << "added";
    case node_update_type::decommissioned:
        return o << "decommissioned";
    case node_update_type::recommissioned:
        return o << "recommissioned";
    case node_update_type::reallocation_finished:
        return o << "reallocation_finished";
    case node_update_type::removed:
        return o << "removed";
    case node_update_type::interrupted:
        return o << "interrupted";
    }
    return o << "unknown";
}

std::ostream& operator<<(std::ostream& o, reconfiguration_state update) {
    switch (update) {
    case reconfiguration_state::in_progress:
        return o << "in_progress";
    case reconfiguration_state::cancelled:
        return o << "cancelled";
    case reconfiguration_state::force_cancelled:
        return o << "force_cancelled";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const cloud_storage_mode& mode) {
    switch (mode) {
    case cloud_storage_mode::disabled:
        return o << "disabled";
    case cloud_storage_mode::write_only:
        return o << "write_only";
    case cloud_storage_mode::read_only:
        return o << "read_only";
    case cloud_storage_mode::full:
        return o << "full";
    case cloud_storage_mode::read_replica:
        return o << "read_replica";
    }
    __builtin_unreachable();
}

} // namespace cluster

namespace reflection {

// note: adl serialization doesn't support read replica fields since serde
// should be used for new versions.
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

// note: adl deserialization doesn't support read replica fields since serde
// should be used for new versions.
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

    // Legacy topics from pre-22.3 get remote delete disabled.
    cfg.properties.remote_delete = storage::ntp_config::legacy_remote_delete;

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
    adl<cluster::cluster_version>().to(out, r.latest_logical_version);
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

    return cluster::join_node_request{
      logical_version, cluster::invalid_version, node_uuid, node};
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
    return cluster::create_topics_request{
      .topics = std::move(configs), .timeout = timeout};
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
      .topics = std::move(topics), .timeout = timeout};
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

void adl<cluster::create_partitions_configuration>::to(
  iobuf& out, cluster::create_partitions_configuration&& pc) {
    return serialize(
      out, pc.tp_ns, pc.new_total_partition_count, pc.custom_assignments);
}

cluster::create_partitions_configuration
adl<cluster::create_partitions_configuration>::from(iobuf_parser& in) {
    auto tp_ns = adl<model::topic_namespace>{}.from(in);
    auto partition_count = adl<int32_t>{}.from(in);
    auto custom_assignment = adl<std::vector<
      cluster::create_partitions_configuration::custom_assignment>>{}
                               .from(in);

    cluster::create_partitions_configuration ret(
      std::move(tp_ns), partition_count);
    ret.custom_assignments = std::move(custom_assignment);
    return ret;
}

void adl<cluster::create_partitions_configuration_assignment>::to(
  iobuf& out, cluster::create_partitions_configuration_assignment&& ca) {
    return serialize(out, std::move(ca.cfg), std::move(ca.assignments));
}

cluster::create_partitions_configuration_assignment
adl<cluster::create_partitions_configuration_assignment>::from(
  iobuf_parser& in) {
    auto cfg = adl<cluster::create_partitions_configuration>{}.from(in);
    auto p_as = adl<std::vector<cluster::partition_assignment>>{}.from(in);

    return cluster::create_partitions_configuration_assignment(
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
      t.shadow_indexing,
      t.batch_max_bytes,
      t.retention_local_target_bytes,
      t.retention_local_target_ms,
      t.remote_delete,
      t.segment_ms);
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

    if (
      version <= cluster::incremental_topic_updates::
        version_with_batch_max_bytes_and_local_retention) {
        updates.batch_max_bytes
          = adl<cluster::property_update<std::optional<uint32_t>>>{}.from(in);
        updates.retention_local_target_bytes
          = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
        updates.retention_local_target_ms
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
        updates.remote_delete = adl<cluster::property_update<bool>>{}.from(in);
    }

    if (
      version <= cluster::incremental_topic_updates::version_with_segment_ms) {
        updates.segment_ms
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
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
    auto upsert = adl<std::vector<cluster::cluster_property_kv>>().from(in);
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

void adl<cluster::partition_assignment>::to(
  iobuf& out, cluster::partition_assignment&& p_as) {
    reflection::serialize(out, p_as.group, p_as.id, std::move(p_as.replicas));
}

cluster::partition_assignment
adl<cluster::partition_assignment>::from(iobuf_parser& parser) {
    auto group = reflection::adl<raft::group_id>{}.from(parser);
    auto id = reflection::adl<model::partition_id>{}.from(parser);
    auto replicas = reflection::adl<std::vector<model::broker_shard>>{}.from(
      parser);

    return {group, id, std::move(replicas)};
}

void adl<cluster::remote_topic_properties>::to(
  iobuf& out, cluster::remote_topic_properties&& p) {
    reflection::serialize(out, p.remote_revision, p.remote_partition_count);
}

cluster::remote_topic_properties
adl<cluster::remote_topic_properties>::from(iobuf_parser& parser) {
    auto remote_revision = reflection::adl<model::initial_revision_id>{}.from(
      parser);
    auto remote_partition_count = reflection::adl<int32_t>{}.from(parser);

    return {remote_revision, remote_partition_count};
}

void adl<cluster::topic_properties>::to(
  iobuf& out, cluster::topic_properties&& p) {
    reflection::serialize(
      out,
      p.compression,
      p.cleanup_policy_bitflags,
      p.compaction_strategy,
      p.timestamp_type,
      p.segment_size,
      p.retention_bytes,
      p.retention_duration,
      p.recovery,
      p.shadow_indexing,
      p.read_replica,
      p.read_replica_bucket,
      p.remote_topic_properties);
}

cluster::topic_properties
adl<cluster::topic_properties>::from(iobuf_parser& parser) {
    auto compression
      = reflection::adl<std::optional<model::compression>>{}.from(parser);
    auto cleanup_policy_bitflags
      = reflection::adl<std::optional<model::cleanup_policy_bitflags>>{}.from(
        parser);
    auto compaction_strategy
      = reflection::adl<std::optional<model::compaction_strategy>>{}.from(
        parser);
    auto timestamp_type
      = reflection::adl<std::optional<model::timestamp_type>>{}.from(parser);
    auto segment_size = reflection::adl<std::optional<size_t>>{}.from(parser);
    auto retention_bytes = reflection::adl<tristate<size_t>>{}.from(parser);
    auto retention_duration
      = reflection::adl<tristate<std::chrono::milliseconds>>{}.from(parser);
    auto recovery = reflection::adl<std::optional<bool>>{}.from(parser);
    auto shadow_indexing
      = reflection::adl<std::optional<model::shadow_indexing_mode>>{}.from(
        parser);
    auto read_replica = reflection::adl<std::optional<bool>>{}.from(parser);
    auto read_replica_bucket
      = reflection::adl<std::optional<ss::sstring>>{}.from(parser);
    auto remote_topic_properties
      = reflection::adl<std::optional<cluster::remote_topic_properties>>{}.from(
        parser);

    return {
      compression,
      cleanup_policy_bitflags,
      compaction_strategy,
      timestamp_type,
      segment_size,
      retention_bytes,
      retention_duration,
      recovery,
      shadow_indexing,
      read_replica,
      read_replica_bucket,
      remote_topic_properties,
      std::nullopt,
      tristate<size_t>{std::nullopt},
      tristate<std::chrono::milliseconds>{std::nullopt},
      // Backward compat: ADL-generation (pre-22.3) topics use legacy tiered
      // storage mode in which topic deletion does not delete objects in S3
      false,
      tristate<std::chrono::milliseconds>{std::nullopt},
    };
}

void adl<cluster::cluster_property_kv>::to(
  iobuf& out, cluster::cluster_property_kv&& kv) {
    reflection::serialize(out, std::move(kv.key), std::move(kv.value));
}

cluster::cluster_property_kv
adl<cluster::cluster_property_kv>::from(iobuf_parser& p) {
    cluster::cluster_property_kv kv;

    kv.key = adl<ss::sstring>{}.from(p);
    kv.value = adl<ss::sstring>{}.from(p);
    return kv;
}

} // namespace reflection
