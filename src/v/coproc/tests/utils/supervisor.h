/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/supervisor.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/wasm_event_generator.h"
#include "coproc/types.h"
#include "model/record.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/map_reduce.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace coproc {
using script_map_t = absl::btree_map<script_id, std::unique_ptr<coprocessor>>;

// A super simplistic form of the javascript supervisor soley used for
// the purposes of testing. The sharded instance of script_map_t contains the
// same coprocessors across all cores. However due to the distribution of NTPs
// across shards, record_batches from a particular input topic will always
// arrive on the same shard.
class supervisor final : public coproc::supervisor_service {
public:
    /// class constructor
    ///
    /// Reference to sharded state map is passed so unit tests can query it
    /// poking and proding the internal state of the service for validity
    supervisor(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<script_map_t>&,
      ss::sharded<ss::lw_shared_ptr<bool>>&);

    /// Called when new coprocessor(s) are to be deployed
    ///
    /// Simply instantiate the script and place it in the working set
    ss::future<enable_copros_reply>
    enable_coprocessors(enable_copros_request&&, rpc::streaming_context&) final;

    /// Called when coprocessors(s) are to be removed
    ///
    /// Removes the script(s) from the map, deallocates their resources
    ss::future<disable_copros_reply> disable_coprocessors(
      disable_copros_request&&, rpc::streaming_context&) final;

    /// Called during some failure, or upon restart of redpanda
    ///
    /// Removes all state
    ss::future<disable_copros_reply>
    disable_all_coprocessors(empty_request&&, rpc::streaming_context&) final;

    /// Called when when new data from registered input topics arrives
    ///
    /// Call apply() on matching scripts, returning their transformed results.
    ss::future<process_batch_reply>
    process_batch(process_batch_request&&, rpc::streaming_context&) final;

    /// Called to verify the supervisor is up
    ss::future<state_size_t>
    heartbeat(empty_request&&, rpc::streaming_context&) final;

private:
    ss::future<absl::node_hash_set<script_id>> registered_scripts();
    ss::future<disable_copros_reply::ack> disable_coprocessor(script_id);
    ss::future<enable_copros_reply::data> enable_coprocessor(script_id, iobuf);
    ss::future<enable_copros_reply::data> launch(
      script_id,
      std::vector<enable_copros_reply::topic_policy>&&,
      registry::type_identifier);

    ss::future<std::vector<process_batch_reply::data>> invoke_coprocessor(
      const model::ntp&,
      const script_id,
      ss::circular_buffer<model::record_batch>&&);

    ss::future<std::vector<process_batch_reply::data>>
      invoke_coprocessors(process_batch_request::data);

private:
    /// Map of coprocessors organized by their global identifiers
    ss::sharded<script_map_t>& _coprocessors;

    /// When set to true, send a return heartbeat > 2s to trigger failure
    /// mechanism
    ss::sharded<ss::lw_shared_ptr<bool>>& _delay_heartbeat;
};
} // namespace coproc
