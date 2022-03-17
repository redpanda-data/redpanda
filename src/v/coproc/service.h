/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/pacemaker.h"
#include "coproc/script_manager.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "rpc/types.h"
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
    service(
      ss::scheduling_group, ss::smp_service_group, ss::sharded<pacemaker>&);

    /// \brief coproc client calls this to 'register'
    ///
    /// \param metdata_info list of coprocessors to register
    /// \param streaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per topic per coprocessor
    ss::future<enable_copros_reply>
    enable_copros(enable_copros_request&&, rpc::streaming_context&) final;

    /// \brief coproc client calls this to 'deregister'
    ///
    /// \param metdata_info list of coprocessor to deregister
    /// \param streaming_context rpc context expected at every endpoint
    ///
    /// \return structure representing an ack per deregistered coprocessor
    ss::future<disable_copros_reply>
    disable_copros(disable_copros_request&&, rpc::streaming_context&) final;

private:
    ss::future<enable_copros_reply::ack_id_pair>
    enable_copro(enable_copros_request::data&&);

    ss::future<disable_response_code> disable_copro(script_id);

    /// Main mapping for actively tracked coprocessor scripts
    ss::sharded<pacemaker>& _pacemaker;
};

} // namespace coproc
