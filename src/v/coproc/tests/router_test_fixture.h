#pragma once
#include "coproc/tests/coproc_test_fixture.h"
#include "coproc/tests/coprocessor.h"
#include "coproc/tests/supervisor.h"
#include "vassert.h"

#include <seastar/core/sharded.hh>

// Non-sharded rpc_service to emmulate the javascript engine
class router_test_fixture
  : public coproc_test_fixture<rpc_sharded_service_tag> {
public:
    using copro_map = coproc::supervisor::copro_map;

    using simple_input_set
      = std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>>;

    // Non-sharded rpc service to emulate the JS engine which is single threaded
    router_test_fixture()
      : coproc_test_fixture<rpc_sharded_service_tag>(43189) {
        _coprocessors.start().get();
        register_service<coproc::supervisor>(std::ref(_coprocessors));
    }

    ~router_test_fixture() override { _coprocessors.stop().get(); }

    template<typename CoprocessorType>
    ss::future<> add_copro(uint32_t sid, simple_input_set&& input) {
        return _coprocessors.invoke_on_all(
          [sid, input = std::move(input)](copro_map& coprocessors) {
              coproc::script_id asid(sid);
              vassert(
                coprocessors.find(asid) == coprocessors.end(),
                "Cannot double insert coprocessor with same cp_id");
              coprocessor::input_set iset;
              iset.reserve(input.size());
              std::transform(
                input.begin(),
                input.end(),
                std::back_inserter(iset),
                [](auto& p) {
                    return std::make_pair(
                      model::topic(std::move(p.first)), std::move(p.second));
                });
              coprocessors.emplace(
                asid, std::make_unique<CoprocessorType>(asid, std::move(iset)));
          });
    }

    void startup(log_layout_map&& llm) {
        // assemble the active_copros from the '_coprocessors' map
        active_copros rlayout;
        _coprocessors
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
        coproc_test_fixture<rpc_sharded_service_tag>::startup(
          std::move(llm), std::move(rlayout));
    }

private:
    ss::sharded<copro_map> _coprocessors;
};
