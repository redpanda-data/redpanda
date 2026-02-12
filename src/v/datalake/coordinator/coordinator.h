/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/hash/hash.h"
#include "cluster/fwd.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "datalake/coordinator/file_committer.h"
#include "datalake/coordinator/partition_state_override.h"
#include "datalake/coordinator/snapshot_remover.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/fwd.h"
#include "model/fundamental.h"

namespace datalake::coordinator {

// Public interface that provides access to the coordinator STM. Conceptually,
// the STM focuses solely on persisting deterministic updates, while this:
// 1. wrangles additional aspects of these updates like concurrency, and
// 2. reconciles the STM state with external catalogs.
class coordinator {
public:
    enum class errc {
        not_leader,
        stm_apply_error,
        revision_mismatch,
        incompatible_schema,
        timedout,
        shutting_down,
        failed,
    };
    using remove_tombstone_f
      = ss::noncopyable_function<ss::future<checked<std::nullopt_t, errc>>(
        const model::topic&, model::revision_id)>;
    coordinator(
      ss::shared_ptr<coordinator_stm> stm,
      cluster::topic_table& topics,
      type_resolver& type_resolver,
      schema_manager& schema_mgr,
      remove_tombstone_f remove_tombstone,
      file_committer& file_committer,
      snapshot_remover& snapshot_remover,
      config::binding<std::chrono::milliseconds> commit_interval,
      config::binding<ss::sstring> default_partition_spec,
      config::binding<bool> disable_snapshot_expiry)
      : stm_(std::move(stm))
      , topic_table_(topics)
      , type_resolver_(type_resolver)
      , schema_mgr_(schema_mgr)
      , remove_tombstone_(std::move(remove_tombstone))
      , file_committer_(file_committer)
      , snapshot_remover_(snapshot_remover)
      , commit_interval_(std::move(commit_interval))
      , default_partition_spec_(std::move(default_partition_spec))
      , disable_snapshot_expiry_(std::move(disable_snapshot_expiry)) {}

    void start();
    ss::future<> stop_and_wait();

    ss::future<checked<std::nullopt_t, errc>> sync_ensure_table_exists(
      model::topic topic,
      model::revision_id topic_revision,
      record_schema_components);

    ss::future<checked<std::nullopt_t, errc>> sync_ensure_dlq_table_exists(
      model::topic topic, model::revision_id topic_revision);

    ss::future<checked<std::nullopt_t, errc>> sync_add_files(
      model::topic_partition tp,
      model::revision_id topic_revision,
      chunked_vector<translated_offset_range>);

    struct last_offsets {
        std::optional<kafka::offset> last_added_offset;
        std::optional<kafka::offset> last_committed_offset;
    };
    ss::future<checked<last_offsets, errc>> sync_get_last_added_offsets(
      model::topic_partition tp, model::revision_id topic_rev);

    ss::future<checked<datalake_usage_stats, errc>> sync_get_usage_stats();

    ss::future<checked<chunked_hash_map<model::topic, topic_state>, errc>>
    sync_get_topic_state(chunked_vector<model::topic> topics);

    ss::future<checked<void, errc>> sync_reset_topic_state(
      model::topic topic,
      model::revision_id topic_rev,
      bool reset_all_partitions,
      chunked_hash_map<model::partition_id, partition_state_override>
        partition_overrides);

    void notify_leadership(std::optional<model::node_id>);

    bool leader_loop_running() const { return term_as_.has_value(); }

private:
    // Key used to deduplicate requests for the same table.
    struct table_key {
        model::topic topic;
        model::revision_id topic_revision;
        record_schema_components record_comps;
        bool operator==(const table_key&) const = default;
        template<typename H>
        friend H AbslHashValue(H h, const table_key& k) {
            return H::combine(
              std::move(h), k.topic, k.topic_revision, k.record_comps);
        }
    };
    // Functions to help avoid piling up duplicate requests for the same table
    // key. The caller adds a waiter to an existing list of waiters if one
    // exists, in which case they are expected to wait on the returned future.
    // Otherwise, the caller may proceed immediately to make the request and
    // _must_ notify the waiters upon finishing.
    using ensure_table_result_t = checked<std::nullopt_t, errc>;
    using ensure_table_map_t = chunked_hash_map<
      table_key,
      chunked_vector<ss::promise<ensure_table_result_t>>>;
    static std::optional<ss::future<ensure_table_result_t>>
    maybe_add_waiter(const table_key&, ensure_table_map_t&);
    static ensure_table_result_t notify_waiters_and_erase(
      const table_key&, ensure_table_map_t&, ensure_table_result_t);

    checked<ss::gate::holder, errc> maybe_gate();

    // Waits for leadership, and then reconciles STM state with external state
    // (e.g. table state in the Iceberg catalog) while leader. Repeats until
    // aborted.
    ss::future<> run_until_abort();

    // Repeatedly reconciles the STM state with external state (e.g. table
    // state in the Iceberg catalog). Exits when leadership in the given term
    // has been lost.
    ss::future<checked<std::nullopt_t, errc>>
      run_until_term_change(model::term_id);

    ss::future<checked<ss::stop_iteration, errc>>
    update_lifecycle_state(const model::topic&, model::term_id);

    struct table_schema_provider;
    struct main_table_schema_provider;
    struct dlq_table_schema_provider;
    ss::future<checked<std::nullopt_t, errc>> do_ensure_table_exists(
      model::topic,
      model::revision_id topic_revision,
      record_schema_components,
      std::string_view method_name,
      const table_schema_provider&);

    // Get the effective default partition spec.
    // This is an AWS Glue compatibility kludge.
    ss::sstring get_effective_default_partition_spec(
      const std::optional<ss::sstring>& partition_spec) const;

    // Return whether the underlying catalog is the Glue REST catalog.
    // TODO: if the kludges start piling up, we should abstract some "catalog
    // capabilities" out.
    bool using_glue_catalog() const;

    ss::shared_ptr<coordinator_stm> stm_;
    cluster::topic_table& topic_table_;
    type_resolver& type_resolver_;
    schema_manager& schema_mgr_;
    remove_tombstone_f remove_tombstone_;
    file_committer& file_committer_;
    snapshot_remover& snapshot_remover_;
    config::binding<std::chrono::milliseconds> commit_interval_;
    config::binding<ss::sstring> default_partition_spec_;
    config::binding<bool> disable_snapshot_expiry_;

    ss::gate gate_;
    ss::abort_source as_;
    ss::condition_variable leader_cond_;

    // Abort source that can be used to stop work in a given term.
    // Is only set if there is an on-going call to run_until_term_change().
    std::optional<std::reference_wrapper<ss::abort_source>> term_as_;

    ensure_table_map_t in_flight_main_;
    ensure_table_map_t in_flight_dlq_;
};
std::ostream& operator<<(std::ostream&, coordinator::errc);

} // namespace datalake::coordinator
