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

#include "container/fragmented_vector.h"
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
    explicit coordinator(ss::shared_ptr<coordinator_stm> stm)
      : stm_(std::move(stm)) {}

    ss::future<> stop_and_wait();
    ss::future<checked<std::nullopt_t, errc>> sync_add_files(
      model::topic_partition tp, chunked_vector<translated_offset_range>);
    ss::future<checked<std::optional<kafka::offset>, errc>>
    sync_get_last_added_offset(model::topic_partition tp);
    void notify_leadership(std::optional<model::node_id>) {}

private:
    checked<ss::gate::holder, errc> maybe_gate();

    ss::shared_ptr<coordinator_stm> stm_;

    ss::gate gate_;
    ss::abort_source as_;
};
std::ostream& operator<<(std::ostream&, coordinator::errc);

} // namespace datalake::coordinator
