#include "kafka/errors.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/produce_request.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "pandaproxy/client/client.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/test/pandaproxy_client_fixture.h"
#include "pandaproxy/client/test/utils.h"
#include "utils/unresolved_address.h"

#include <chrono>

namespace ppc = pandaproxy::client;

FIXTURE_TEST(pandaproxy_client_reconnect, ppc_test_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    ppc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    ppc::shard_local_cfg().retries.set_value(size_t(0));

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();

    {
        info("Checking no topics");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 0);
    }

    {
        info("Adding known topic");
        auto ntp = make_default_ntp(tp.topic, tp.partition);
        add_topic(model::topic_namespace_view(ntp)).get();
    }

    {
        info("Checking for known topic");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.topics[0].name(), "t");
    }

    {
        info("Restarting broker");
        restart();
    }

    {
        info("Checking for known topic - expect controller not ready");
        auto res = client.dispatch(make_list_topics_req());
        BOOST_REQUIRE_THROW(res.get(), ppc::broker_error);
    }

    {
        ppc::shard_local_cfg().retries.set_value(size_t(5));
        info("Checking for known topic - controller ready");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.topics[0].name(), "t");
    }

    info("Stopping client");
    client.stop().get();
}
