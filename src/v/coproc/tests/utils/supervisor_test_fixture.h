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
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/supervisor.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "vassert.h"

#include <seastar/core/sharded.hh>

// Non-sharded rpc_service to emmulate the javascript engine
class supervisor_test_fixture : public rpc_sharded_integration_fixture {
public:
    using copro_map = coproc::supervisor::copro_map;

    using simple_input_set
      = std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>>;

    // Non-sharded rpc service to emulate the JS engine which is single threaded
    supervisor_test_fixture()
      : rpc_sharded_integration_fixture(43189) {
        _coprocessors.start().get();
        configure_server();
        register_service<coproc::supervisor>(std::ref(_coprocessors));
        start_server();
    }

    ~supervisor_test_fixture() override {
        stop_server();
        _coprocessors.stop().get();
    }

    template<typename CoprocessorType>
    ss::future<> add_copro(uint32_t sid, simple_input_set&& input) {
        return _coprocessors.invoke_on_all(
          [this, sid, input = std::move(input)](
            copro_map& coprocessors) mutable {
              coproc::script_id asid(sid);
              vassert(
                coprocessors.find(asid) == coprocessors.end(),
                "Cannot double insert coprocessor with same cp_id");
              coprocessors.emplace(
                asid,
                std::make_unique<CoprocessorType>(
                  asid, create_input_set(std::move(input))));
          });
    }

protected:
    ss::sharded<copro_map>& all_coprocessors() { return _coprocessors; }
    const ss::sharded<copro_map>& all_coprocessors() const {
        return _coprocessors;
    }

private:
    coprocessor::input_set create_input_set(simple_input_set&& input) {
        coprocessor::input_set iset;
        iset.reserve(input.size());
        std::transform(
          input.begin(), input.end(), std::back_inserter(iset), [](auto& p) {
              return std::make_pair(
                model::topic(std::move(p.first)), std::move(p.second));
          });
        return iset;
    }

    /// Map is identical accross all cores
    ss::sharded<copro_map> _coprocessors;
};
