#pragma once
#include "coproc/tests/coproc_test_fixture.h"
#include "coproc/tests/coprocessor.h"
#include "coproc/tests/supervisor_test_fixture.h"

// Non-sharded rpc_service to emmulate the javascript engine
class router_test_fixture
  : public coproc_test_fixture
  , public supervisor_test_fixture {
public:
    using copro_map = coproc::supervisor::copro_map;

    using simple_input_set
      = std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>>;

    // Non-sharded rpc service to emulate the JS engine which is single threaded
    router_test_fixture()
      : coproc_test_fixture(true)
      , supervisor_test_fixture() {}

    void startup(log_layout_map&& llm) {
        // assemble the active_copros from the '_coprocessors' map
        active_copros rlayout;
        all_coprocessors()
          .invoke_on(
            ss::this_shard_id(),
            [&rlayout](copro_map& coprocessors) {
                std::transform(
                  coprocessors.begin(),
                  coprocessors.end(),
                  std::inserter(rlayout, rlayout.end()),
                  [](const auto& p) {
                      return coproc::enable_copros_request::data{
                        .id = p.first, .topics = p.second->get_input_topics()};
                  });
            })
          .get();
        // Data on all shards is identical
        coproc_test_fixture::startup(std::move(llm), std::move(rlayout));
    }
};
