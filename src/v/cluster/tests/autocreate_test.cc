#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

#include <vector>

std::vector<cluster::topic_configuration> test_topics_configuration() {
    return std::vector<cluster::topic_configuration>{
      cluster::topic_configuration(test_ns, model::topic("tp-1"), 10, 1),
      cluster::topic_configuration(test_ns, model::topic("tp-2"), 10, 1),
      cluster::topic_configuration(test_ns, model::topic("tp-3"), 10, 1),
    };
}

void validate_topic_metadata(cluster::metadata_cache& cache) {
    auto expected_topics = test_topics_configuration();

    for (auto& t_cfg : expected_topics) {
        auto tp_md = cache.get_topic_metadata(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(tp_md.has_value(), true);
        BOOST_REQUIRE_EQUAL(tp_md->partitions.size(), t_cfg.partition_count);
        auto cfg = cache.get_topic_cfg(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->tp_ns, t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->partition_count, t_cfg.partition_count);
        BOOST_REQUIRE_EQUAL(cfg->replication_factor, t_cfg.replication_factor);
        BOOST_REQUIRE_EQUAL(cfg->compaction, t_cfg.compaction);
        BOOST_REQUIRE_EQUAL(cfg->compression, t_cfg.compression);
    }
}

FIXTURE_TEST(
  recover_single_topic_test_at_current_broker, controller_tests_fixture) {
    auto& cntrl = get_controller();
    cntrl.invoke_on_all(&cluster::controller::start).get();
    // wait_for_leadership(cntrl.local());

    auto results = cntrl.local()
                     .autocreate_topics(
                       test_topics_configuration(), model::no_timeout)
                     .get0();

    BOOST_REQUIRE_EQUAL(results.size(), 3);

    for (auto& r : results) {
        if (r.ec == cluster::errc::success) {
            auto md = get_local_cache().get_topic_metadata(r.tp_ns);
            BOOST_REQUIRE_EQUAL(md.has_value(), true);
            BOOST_REQUIRE_EQUAL(md.value().tp_ns, r.tp_ns);
        }
    }
}

FIXTURE_TEST(test_autocreate_on_non_leader, cluster_test_fixture) {
    // root cluster node
    add_controller(model::node_id{1}, ss::smp::count, 9092, 11000, {});
    add_controller(
      model::node_id{2},
      ss::smp::count,
      9093,
      11001,
      {{.id = model::node_id{1},
        .addr = unresolved_address("127.0.0.1", 11000)}});

    // first controller
    auto& cntrl_0 = get_controller(0);
    cntrl_0.invoke_on_all(&cluster::controller::start).get();
    wait_for_leadership(cntrl_0.local());

    auto& cntrl_1 = get_controller(1);
    cntrl_1.invoke_on_all(&cluster::controller::start).get();

    BOOST_REQUIRE_EQUAL(cntrl_0.local().is_leader(), true);

    // Wait for cluster to reach stable state
    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return get_local_cache(0).all_brokers().size() == 2
               && get_local_cache(1).all_brokers().size() == 2;
    }).get();

    cntrl_1.local()
      .autocreate_topics(test_topics_configuration(), model::no_timeout)
      .get0();

    // Make sure caches are the same
    validate_topic_metadata(get_local_cache(0));
    validate_topic_metadata(get_local_cache(1));
}
