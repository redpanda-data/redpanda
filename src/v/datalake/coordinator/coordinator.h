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

#include "config/property.h"
#include "container/fragmented_vector.h"
#include "datalake/coordinator/file_committer.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/state_update.h"
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
        timedout,
        shutting_down,
    };
    coordinator(
      ss::shared_ptr<coordinator_stm> stm,
      file_committer& file_committer,
      config::binding<std::chrono::milliseconds> commit_interval)
      : stm_(std::move(stm))
      , file_committer_(file_committer)
      , commit_interval_(std::move(commit_interval)) {}

    void start();
    ss::future<> stop_and_wait();
    ss::future<checked<std::nullopt_t, errc>> sync_add_files(
      model::topic_partition tp, chunked_vector<translated_offset_range>);
    ss::future<checked<std::optional<kafka::offset>, errc>>
    sync_get_last_added_offset(model::topic_partition tp);
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

    ss::shared_ptr<coordinator_stm> stm_;
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
