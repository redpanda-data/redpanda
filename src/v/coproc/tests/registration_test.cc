#include "cluster/namespace.h"
#include "coproc/service.h"
#include "coproc/types.h"
#include "random/generators.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "storage/api.h"
#include "storage/ntp_config.h"
#include "test_utils/fixture.h"

#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

class coproc_test_fixture : public rpc_base_integration_fixture {
public:
    using erc = coproc::enable_response_code;
    using drc = coproc::disable_response_code;

    using deferred_fn = ss::deferred_action<std::function<void()>>;
    using sample_data_map
      = absl::flat_hash_map<model::topic_namespace, int32_t>;

    coproc_test_fixture()
      : rpc_base_integration_fixture(_coproc_registration_port) {
        _notes.start().get();
        _mappings.start().get();
        _storage.start(default_kvstorecfg(), default_logcfg()).get0();
        _storage.invoke_on_all(&storage::api::start).get();
        _deferred = std::make_unique<deferred_fn>([this]() {
            _mappings.stop().get();
            _notes.stop().get();
            _storage.stop().get();
        });
    }

    ss::future<> init_with_sample_data(sample_data_map&& sdm) {
        return ss::smp::invoke_on_all([this, sdm = std::move(sdm)] {
            return ss::do_for_each(sdm, [this](const auto p) {
                return ss::do_for_each(
                  boost::make_counting_iterator(0),
                  boost::make_counting_iterator(p.second),
                  [this, p](int32_t i) {
                      return insert_ntp(model::ntp(
                        p.first.ns, p.first.tp, model::partition_id(i)));
                  });
            });
        });
    }

    void register_services() {
        register_service<coproc::service>(
          std::ref(_mappings), std::ref(_storage));
    }

    ss::future<result<rpc::client_context<coproc::enable_topics_reply>>>
    coproc_register_topics(
      rpc::client<coproc::registration_client_protocol>& client,
      std::vector<model::topic>&& inputs) {
        coproc::metadata_info md_info{.inputs = std::move(inputs)};
        return client.enable_topics(
          std::move(md_info), rpc::client_opts(rpc::no_timeout));
    }

    ss::future<result<rpc::client_context<coproc::disable_topics_reply>>>
    coproc_deregister_topics(
      rpc::client<coproc::registration_client_protocol>& client,
      std::vector<model::topic>&& inputs) {
        coproc::metadata_info md_info{.inputs = std::move(inputs)};
        return client.disable_topics(
          std::move(md_info), rpc::client_opts(rpc::no_timeout));
    }

    /// This function ensures that the internals of the coproc::active_mappings
    /// caches are laid out how they are expected to be i.e. the right ntps
    /// existing on the correct cores determined by the _shard_table
    ss::future<int> coproc_validate(absl::flat_hash_set<model::topic> tns) {
        return _mappings.map_reduce0(
          [this, tns = std::move(tns)](const coproc::active_mappings& ams) {
              // 1. Get all ntps for this current shard
              const auto& c = _notes.local();
              // 2. Count all ntps that match the input
              return std::count_if(
                c.cbegin(), c.cend(), [&ams, tns](const model::ntp& ntp) {
                    const auto found = std::find_if(
                      tns.begin(), tns.end(), [&ntp](const model::topic& t) {
                          // 2a. Match is good enough if topics are eq
                          return t == ntp.tp.topic;
                      });
                    // 2b. If this is what user is querying, check
                    return found != tns.end() ? (ams.find(ntp) != ams.end())
                                              : false;
                });
          },
          0, // 3. Aggregate sum
          std::plus<>());
    }

private:
    ss::future<> insert_ntp(const model::ntp& ntp) {
        const auto hash = std::hash<model::ntp>()(ntp) % ss::smp::count;
        if (hash == ss::this_shard_id()) {
            storage::ntp_config ntp_cfg(ntp, _cfg_dir);
            _notes.local().insert(ntp);
            return _storage.local()
              .log_mgr()
              .manage(std::move(ntp_cfg))
              .then([](auto log) {});
        }
        return ss::make_ready_future<>();
    }

    storage::kvstore_config default_kvstorecfg() {
        return storage::kvstore_config(
          8192,
          std::chrono::milliseconds(10),
          _cfg_dir,
          storage::debug_sanitize_files::yes);
    }

    storage::log_config default_logcfg() {
        return storage::log_config(
          storage::log_config::storage_type::memory,
          _cfg_dir,
          100_MiB,
          storage::debug_sanitize_files::yes);
    }

    ss::sharded<absl::flat_hash_set<model::ntp>> _notes;
    ss::sharded<coproc::active_mappings> _mappings;
    ss::sharded<storage::api> _storage;
    std::unique_ptr<deferred_fn> _deferred;
    ss::sstring _cfg_dir{
      "test_directory/" + random_generators::gen_alphanum_string(4)};

    static const inline uint16_t _coproc_registration_port
      = random_generators::get_int<uint16_t>(
        1025, std::numeric_limits<uint16_t>::max());
};

model::topic_namespace make_ts(ss::sstring&& topic) {
    return model::topic_namespace(
      cluster::kafka_namespace, model::topic(topic));
}

// to_topic_set
absl::flat_hash_set<model::topic> tts(std::vector<ss::sstring>&& topics) {
    absl::flat_hash_set<model::topic> topic_set;
    std::transform(
      topics.begin(),
      topics.end(),
      std::inserter(topic_set, topic_set.begin()),
      [](const ss::sstring& t) { return model::topic(t); });
    return topic_set;
}

// to_topic_vector
std::vector<model::topic> ttv(std::vector<ss::sstring>&& topics) {
    std::vector<model::topic> topic_vec;
    std::transform(
      topics.begin(),
      topics.end(),
      std::back_inserter(topic_vec),
      [](const ss::sstring& t) { return model::topic(t); });
    return topic_vec;
}

// This test fixture tests the edge cases, i.e. situations that should fail
FIXTURE_TEST(test_coproc_bad_topics, coproc_test_fixture) {
    configure_server();
    init_with_sample_data(
      {{make_ts("foo"), 5}, {make_ts("bar"), 3}, {make_ts("baz"), 18}})
      .get();
    register_services();
    start_server();
    info("Started coproc rpc registration server");
    auto client = rpc::client<coproc::registration_client_protocol>(
      client_config());
    info("coproc rpc client connecting");
    client.connect().get();
    info("coproc rpc client connected");
    auto dclient = ss::defer([&client] { client.stop().get(); });

    // This string is more then 249 chars, should be an error to attempt to
    // register
    static const ss::sstring too_long_topic(
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_");
    const auto resp = coproc_register_topics(
                        client, ttv({".", "..", "foo", "", too_long_topic}))
                        .get0()
                        .value()
                        .data;
    BOOST_REQUIRE_EQUAL(resp.acks.size(), 5);
    BOOST_REQUIRE(resp.acks[0] == erc::invalid_topic);
    BOOST_REQUIRE(resp.acks[1] == erc::invalid_topic);
    BOOST_REQUIRE(resp.acks[2] == erc::success);
    BOOST_REQUIRE(resp.acks[3] == erc::invalid_topic);
    BOOST_REQUIRE(resp.acks[4] == erc::invalid_topic);
    BOOST_REQUIRE_EQUAL(
      coproc_validate(tts({"foo", too_long_topic})).get0(), 5);

    // Service should ack with the 'erc::topic_already_enabled' response
    const auto more_resp = coproc_register_topics(
                             client, ttv({"foo", "foo.$bar$"}))
                             .get0()
                             .value()
                             .data;
    BOOST_REQUIRE_EQUAL(more_resp.acks.size(), 2);
    BOOST_REQUIRE(more_resp.acks[0] == erc::topic_already_enabled);
    BOOST_REQUIRE(more_resp.acks[1] == erc::materialized_topic);
    BOOST_REQUIRE_EQUAL(coproc_validate(tts({"foo"})).get0(), 5);

    const auto more_resp2
      = coproc_register_topics(client, ttv({"kzy"})).get0().value().data;
    BOOST_REQUIRE_EQUAL(more_resp2.acks.size(), 1);
    BOOST_REQUIRE(more_resp2.acks[0] == erc::topic_does_not_exist);
    BOOST_REQUIRE_EQUAL(coproc_validate(tts({"kzy"})).get0(), 0);

    // When attempting to deregister a topic that was never registered,
    // service should ack with 'drc::topic_never_enabled'
    const auto try_deregister_acks = coproc_deregister_topics(
                                       client, ttv({"shouldnt_exist"}))
                                       .get0()
                                       .value()
                                       .data;
    BOOST_REQUIRE_EQUAL(try_deregister_acks.acks.size(), 1);
    BOOST_REQUIRE(try_deregister_acks.acks[0] == drc::topic_never_enabled);
    BOOST_REQUIRE_EQUAL(coproc_validate(tts({"shouldnt_exist"})).get0(), 0);
}

// This fixture tests all of the cases where all data is considered good to
// interpret and every outcome should succeed.
FIXTURE_TEST(test_coproc_topics, coproc_test_fixture) {
    configure_server();
    init_with_sample_data(
      {{make_ts("foo"), 8}, {make_ts("bar"), 2}, {make_ts("baz"), 18}})
      .get();
    register_services();
    start_server();
    info("Started coproc rpc registration server");
    auto client = rpc::client<coproc::registration_client_protocol>(
      client_config());
    info("coproc rpc client connecting");
    client.connect().get();
    info("coproc rpc client connected");
    auto dclient = ss::defer([&client] { client.stop().get(); });

    // 1. Attempt to register foo, bar and baz
    const auto resp = coproc_register_topics(client, ttv({"foo", "bar", "baz"}))
                        .get0()
                        .value()
                        .data;
    // 2. ensure server replied with success for all topics
    BOOST_REQUIRE_EQUAL(resp.acks.size(), 3);
    for (const auto ack : resp.acks) {
        BOOST_REQUIRE(ack == erc::success);
    }
    // 3. Verify they actually exist in the 'source_topics' struct
    BOOST_REQUIRE_EQUAL(coproc_validate(tts({"foo", "bar", "baz"})).get0(), 28);

    // 4-6. Attempt to deregister some
    // Disabling more then 1 topic implicity tests the deletion of an
    // item in the collection while iterating
    const auto disable_acks = coproc_deregister_topics(
                                client, ttv({"foo", "bar"}))
                                .get0()
                                .value()
                                .data;
    BOOST_REQUIRE_EQUAL(disable_acks.acks.size(), 2);
    for (const auto ack : disable_acks.acks) {
        BOOST_REQUIRE(ack == drc::success);
    }
    BOOST_REQUIRE_EQUAL(coproc_validate(tts({"baz"})).get0(), 18);

    info("coproc rpc service stopping");
}
