#include "cluster/simple_batch_builder.h"
#include "cluster/tests/controller_test_fixture.h"
#include "cluster/types.h"
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
