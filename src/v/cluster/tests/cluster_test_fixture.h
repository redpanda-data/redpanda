#pragma once
#include "cluster/tests/controller_test_fixture.h"

#include <seastar/core/metrics_api.hh>

// clang-format off
template<typename Pred>
CONCEPT(requires requires(Pred p){
    {p()} -> bool;
})
// clang-format on
void wait_for(model::timeout_clock::duration timeout, Pred&& p) {
    with_timeout(
      model::timeout_clock::now() + timeout,
      do_until(
        [p = std::forward<Pred>(p)] { return p(); },
        [] { return ss::sleep(std::chrono::milliseconds(400)); }))
      .get0();
}

class cluster_test_fixture {
public:
    cluster_test_fixture() { set_configuration("disable_metrics", true); }

    void add_controller(
      model::node_id node_id,
      uint32_t cores,
      int16_t kafka_port,
      int16_t rpc_port,
      std::vector<config::seed_server> seeds) {
        _instances.push_back(std::make_unique<controller_tests_fixture>(
          node_id, cores, kafka_port, rpc_port, seeds));
    }

    ss::sharded<cluster::controller>& get_controller(int idx) {
        return _instances[idx]->get_controller();
    }

    cluster::metadata_cache& get_local_cache(int idx) {
        return _instances[idx]->get_local_cache();
    }

private:
    std::vector<std::unique_ptr<controller_tests_fixture>> _instances;
};
