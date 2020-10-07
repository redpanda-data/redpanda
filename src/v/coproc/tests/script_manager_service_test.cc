#include "coproc/router.h"
#include "coproc/script_manager.h"
#include "coproc/service.h"
#include "coproc/tests/coproc_test_fixture.h"
#include "coproc/tests/utils.h"
#include "coproc/types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/util/defer.hh>

ss::future<result<rpc::client_context<coproc::enable_copros_reply>>>
coproc_register_topics(
  rpc::client<coproc::script_manager_client_protocol>& client,
  std::vector<coproc::enable_copros_request::data>&& data) {
    coproc::enable_copros_request req{.inputs = std::move(data)};
    return client.enable_copros(
      std::move(req), rpc::client_opts(rpc::no_timeout));
}

ss::future<result<rpc::client_context<coproc::disable_copros_reply>>>
coproc_deregister_topics(
  rpc::client<coproc::script_manager_client_protocol>& client,
  std::vector<uint32_t>&& sids) {
    std::vector<coproc::script_id> script_ids;
    script_ids.reserve(sids.size());
    std::transform(
      sids.begin(),
      sids.end(),
      std::back_inserter(script_ids),
      [](uint32_t id) { return coproc::script_id(id); });
    coproc::disable_copros_request req{.ids = std::move(script_ids)};
    return client.disable_copros(
      std::move(req), rpc::client_opts(rpc::no_timeout));
}

class script_manager_service_fixture
  : public coproc_test_fixture
  , public rpc_sharded_integration_fixture {
public:
    script_manager_service_fixture()
      : coproc_test_fixture(false)
      , rpc_sharded_integration_fixture(43118) {
        configure_server();
        register_service<coproc::service>(std::ref(get_router()));
        start_server();
    }

    ~script_manager_service_fixture() override { stop_server(); }

    // This function ensures that the internals of the coproc::active_mappings
    // caches are laid out how they are expected to be i.e. the right ntps
    // existing on the correct cores determined by the _shard_table
    ss::future<int> coproc_validate(absl::flat_hash_set<model::topic> topics) {
        return get_router().map_reduce0(
          [this, topics = std::move(topics)](const coproc::router& r) {
              return std::count_if(
                get_data().cbegin(),
                get_data().cend(),
                [&topics, &r](const model::ntp& ntp) {
                    return topics.find(ntp.tp.topic) != topics.end()
                             ? r.ntp_exists(ntp)
                             : false;
                });
          },
          0, // 3. Aggregate sum
          std::plus<>());
    }
};

// This test fixture tests the edge cases, i.e. situations that should fail
FIXTURE_TEST(test_coproc_invalid_topics, script_manager_service_fixture) {
    startup(
      {{make_ts("foo"), 5}, {make_ts("bar"), 3}, {make_ts("baz"), 18}}, {});
    auto client = rpc::client<coproc::script_manager_client_protocol>(
      client_config());
    client.connect().get();
    auto dclient = ss::defer([&client] { client.stop().get(); });

    // This string is more then 249 chars, should be an error to attempt to
    // register
    static const ss::sstring too_long_topic(
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_");

    const auto resp
      = coproc_register_topics(
          client,
          {make_enable_req(
            1234,
            {{".", l}, {"..", l}, {"foo", l}, {"", l}, {too_long_topic, l}})})
          .get0()
          .value()
          .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
    const auto& first_acks = resp.acks[0].second;
    BOOST_CHECK_EQUAL(first_acks.size(), 5);
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(1234));
    BOOST_CHECK(first_acks[0] == erc::invalid_topic);
    BOOST_CHECK(first_acks[1] == erc::invalid_topic);
    BOOST_CHECK(first_acks[2] == erc::success);
    BOOST_CHECK(first_acks[3] == erc::invalid_topic);
    BOOST_CHECK(first_acks[4] == erc::invalid_topic);
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo"})).get0(), 5);
}

FIXTURE_TEST(
  test_coproc_reg_materialized_topic, script_manager_service_fixture) {
    startup(
      {{make_ts("foo"), 6}, {make_ts("bar"), 3}, {make_ts("baz"), 18}}, {});
    auto client = rpc::client<coproc::script_manager_client_protocol>(
      client_config());
    client.connect().get();
    auto dclient = ss::defer([&client] { client.stop().get(); });

    const auto resp = coproc_register_topics(
                        client,
                        {make_enable_req(2313, {{"foo", l}, {"foo.$bar$", l}})})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
    const auto& acks = resp.acks[0].second;
    BOOST_CHECK_EQUAL(coproc::script_id(2313), resp.acks[0].first);
    BOOST_CHECK_EQUAL(acks.size(), 2);
    BOOST_CHECK(acks[0] == erc::success);
    BOOST_CHECK(acks[1] == erc::materialized_topic);
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo"})).get0(), 6);
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo.$bar$"})).get0(), 0);
}

FIXTURE_TEST(
  test_coproc_script_id_already_exists, script_manager_service_fixture) {
    const uint32_t script_id = 55431;
    log_layout_map storage_layout = {
      {make_ts("foo"), 5}, {make_ts("bar"), 3}, {make_ts("baz"), 18}};
    active_copros router_layout = {make_enable_req(script_id, {{"foo", l}})};
    startup(std::move(storage_layout), std::move(router_layout));
    auto client = rpc::client<coproc::script_manager_client_protocol>(
      client_config());
    client.connect().get();
    auto dclient = ss::defer([&client] { client.stop().get(); });

    const auto resp = coproc_register_topics(
                        client,
                        {make_enable_req(3289, {{"foo", l}, {"bar", e}}),
                         {make_enable_req(script_id, {{"nogo", l}})}})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo", "bar"})).get0(), 5);
    BOOST_CHECK_EQUAL(resp.acks.size(), 2);
    const auto& acks_1 = resp.acks[0].second;
    const auto& acks_2 = resp.acks[1].second;
    BOOST_CHECK_EQUAL(acks_1.size(), 2);
    BOOST_CHECK_EQUAL(acks_2.size(), 1);
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(3289));
    BOOST_CHECK_EQUAL(resp.acks[1].first, coproc::script_id(script_id));
    BOOST_CHECK(acks_1[0] == erc::success);
    BOOST_CHECK(acks_1[1] == erc::invalid_ingestion_policy);
    BOOST_CHECK(acks_2[0] == erc::script_id_already_exists);
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo", "bar"})).get0(), 5);
}

FIXTURE_TEST(test_coproc_topics, script_manager_service_fixture) {
    startup(
      {{make_ts("foo"), 8}, {make_ts("bar"), 2}, {make_ts("baz"), 18}}, {});
    auto client = rpc::client<coproc::script_manager_client_protocol>(
      client_config());
    client.connect().get();
    auto dclient = ss::defer([&client] { client.stop().get(); });

    // 1. Attempt to register foo, bar and baz
    const auto resp = coproc_register_topics(
                        client,
                        {make_enable_req(
                           1523, {{"foo", l}, {"bar", l}, {"baz", l}}),
                         make_enable_req(123, {{"foo", l}})})
                        .get0()
                        .value()
                        .data;
    // 2. ensure server replied with success for all topics
    BOOST_CHECK_EQUAL(resp.acks.size(), 2);
    const auto& first_acks = resp.acks[0].second;
    const auto& second_acks = resp.acks[1].second;
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(1523));
    BOOST_CHECK_EQUAL(resp.acks[1].first, coproc::script_id(123));
    BOOST_CHECK_EQUAL(first_acks.size(), 3);
    BOOST_CHECK(first_acks[0] == erc::success);
    BOOST_CHECK(first_acks[1] == erc::success);
    BOOST_CHECK(first_acks[2] == erc::success);
    BOOST_CHECK_EQUAL(second_acks.size(), 1);
    BOOST_CHECK(second_acks[0] == erc::success);

    // 3. Verify they actually exist in the 'source_topics' struct
    BOOST_CHECK_EQUAL(
      coproc_validate(to_topic_set({"foo", "bar", "baz"})).get0(), 28);

    // 4-6. Attempt to deregister some
    const auto disable_acks
      = coproc_deregister_topics(client, {1523}).get0().value().data;
    BOOST_CHECK_EQUAL(disable_acks.acks.size(), 1);
    BOOST_CHECK(disable_acks.acks[0] == drc::success);
    BOOST_CHECK_EQUAL(
      coproc_validate(to_topic_set({"foo", "bar", "baz"})).get0(), 8);
    BOOST_CHECK_EQUAL(coproc_validate(to_topic_set({"foo"})).get0(), 8);

    const auto disable_acks2
      = coproc_deregister_topics(client, {123}).get0().value().data;
    BOOST_CHECK_EQUAL(disable_acks2.acks.size(), 1);
    BOOST_CHECK(disable_acks2.acks[0] == drc::success);
    BOOST_CHECK_EQUAL(
      coproc_validate(to_topic_set({"foo", "bar", "baz"})).get0(), 0);
}
