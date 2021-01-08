/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/logger.h"
#include "coproc/script_manager.h"
#include "coproc/tests/utils/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/supervisor_test_fixture.h"
#include "coproc/types.h"

// Non-sharded rpc_service to emmulate the javascript engine
class router_test_fixture
  : public coproc_test_fixture
  , public supervisor_test_fixture {
public:
    using copro_map = coproc::supervisor::copro_map;
    using enable_reqs_data = std::vector<coproc::enable_copros_request::data>;
    using script_manager_client
      = rpc::client<coproc::script_manager_client_protocol>;

    /// \brief Initialize the storage layer, then submit all coprocessors to
    /// v/coproc/service
    ss::future<> startup(log_layout_map&&, script_manager_client&);

private:
    /// Sanity checks, throws if the service fails to register a
    /// coprocessor, helpful when debugging possible issues within the test
    /// setup process
    void validate_result(
      const enable_reqs_data&,
      result<rpc::client_context<coproc::enable_copros_reply>>);

    ss::future<> enable_coprocessors(enable_reqs_data&, script_manager_client&);

    void to_ecr_data(enable_reqs_data&, const copro_map&);
};
