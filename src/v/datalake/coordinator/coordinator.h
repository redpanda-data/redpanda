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

#include "cluster/fwd.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "datalake/coordinator/file_committer.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/state_update.h"
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
      table_creator& table_creator,
      remove_tombstone_f remove_tombstone,
      file_committer& file_committer,
      config::binding<std::chrono::milliseconds> commit_interval)
      : stm_(std::move(stm))
      , topic_table_(topics)
      , table_creator_(table_creator)
      , remove_tombstone_(std::move(remove_tombstone))
      , file_committer_(file_committer)
      , commit_interval_(std::move(commit_interval)) {}

    void start();
    ss::future<> stop_and_wait();

    ss::future<checked<std::nullopt_t, errc>> sync_ensure_table_exists(
      model::topic topic,
      model::revision_id topic_revision,
      record_schema_components);

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

    void notify_leadership(std::optional<model::node_id>);

    bool leader_loop_running() const { return term_as_.has_value(); }

private:
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

    ss::shared_ptr<coordinator_stm> stm_;
    cluster::topic_table& topic_table_;
    table_creator& table_creator_;
    remove_tombstone_f remove_tombstone_;
    file_committer& file_committer_;
    config::binding<std::chrono::milliseconds> commit_interval_;

    ss::gate gate_;
    ss::abort_source as_;
    ss::condition_variable leader_cond_;

    // Abort source that can be used to stop work in a given term.
    // Is only set if there is an on-going call to run_until_term_change().
    std::optional<std::reference_wrapper<ss::abort_source>> term_as_;
};
std::ostream& operator<<(std::ostream&, coordinator::errc);

} // namespace datalake::coordinator
