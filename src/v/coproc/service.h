/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "cluster/namespace.h"
#include "coproc/router.h"
#include "coproc/script_manager.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "rpc/server.h"
#include "rpc/types.h"
#include "storage/api.h"
#include "storage/log.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace coproc {

/// Coprocessing rpc registration/deregistration service
///
/// When a new coprocessor comes up or down this service will
/// be its interface for working with redpanda.
class service final : public script_manager_service {
public:
    service(ss::scheduling_group, ss::smp_service_group, ss::sharded<router>&);

    /// coproc client calls this to 'register'
    ///
    /// \param metdata_info list of topics coproc is interested in transforming
    /// \param stgreaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per topic
    ss::future<enable_copros_reply>
    enable_copros(enable_copros_request&&, rpc::streaming_context&) final;

    /// coproc client calls this to 'deregister'
    ///
    /// \param metdata_info list of topics coproc no longer wants updates on
    /// \param streaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per topic
    ss::future<disable_copros_reply>
    disable_copros(disable_copros_request&&, rpc::streaming_context&) final;

private:
    using id_resp_vec_pair = std::vector<enable_copros_reply::ack_id_pair>;

    ss::future<enable_copros_reply::ack_id_pair> evaluate_topics(
      const script_id, std::vector<enable_copros_request::data::topic_mode>);

    /// Verify if a script_id exists across all shards of the router
    /// Not called upon the hot path
    ss::future<bool> copro_exists(const script_id);

    /// Different implementation details of update_cache
    ss::future<enable_response_code>
    insert(script_id, model::topic_namespace&&, topic_ingestion_policy);
    ss::future<disable_response_code> remove(script_id);

    /// Main mapping for actively tracked coproc topics
    ss::sharded<router>& _router;
};

} // namespace coproc
