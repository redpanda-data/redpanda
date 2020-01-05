#include "storage2/log_manager.h"
#include "storage2/tests/random_batch.h"
#include "storage2/tests/storage_test_fixture.h"

#include <seastar/testing/thread_test_case.hh>

using namespace storage;       // NOLINT
using namespace storage::test; // NOLINT

FIXTURE_TEST(test_log_manager_api_smoke_test, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    // write path
    auto t1p0 = mgr.create_ntp(make_ntp("default", "topic-one", 0)).get0();
    auto t1p1 = mgr.create_ntp(make_ntp("default", "topic-one", 1)).get0();

    t1p0.append(make_memory_record_batch_reader(make_random_batch_v2(100)))
      .wait();
    t1p0.append(make_memory_record_batch_reader(make_random_batch_v2(50)))
      .wait();
    t1p1.append(make_memory_record_batch_reader(make_random_batch_v2(10)))
      .wait();
    t1p1.append(make_memory_record_batch_reader(make_random_batch_v2(5)))
      .wait();

    t1p0.close().wait();
    t1p1.close().wait();

    // read path
    std::unordered_set<model::ntp> expected{
      make_ntp("default", "topic-one", 0),
      make_ntp("default", "topic-one", 1),
    };

    for (auto ntp : mgr.ntps()) {
        auto log = mgr.open_ntp(ntp).get0();
        log.close().wait();
        expected.erase(ntp);
    }
    BOOST_TEST_REQUIRE(expected.size() == 0);
};

FIXTURE_TEST(repo_read_write_one_batch, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto topic_one_0
      = mgr.create_ntp(make_ntp("default", "topic-one", 0)).get0();

    auto result = topic_one_0
                    .append(make_memory_record_batch_reader(
                      make_random_batch(model::offset(0), 100, false)))
                    .get0();

    topic_one_0.close().wait();
};

FIXTURE_TEST(test_log_manager_ntp_query, storage_test_fixture) {
    create_topic_dir("default", "topic-one", 0);
    create_topic_dir("default", "topic-two", 0);
    create_topic_dir("default", "topic-two", 1);
    create_topic_dir("default", "topic-three", 0);
    create_topic_dir("default", "topic-three", 1);
    create_topic_dir("default", "topic-three", 2);
    create_topic_dir("system", "sys-topic", 0);
    create_topic_dir("system", "sys-topic", 1);

    storage::log_manager mgr = make_log_manager();
    BOOST_TEST_REQUIRE(sstring(mgr.working_directory().string()) == test_dir);

    std::unordered_set<model::ntp> expected_topic_two{
      make_ntp("default", "topic-two", 0), make_ntp("default", "topic-two", 1)};

    std::unordered_set<model::ntp> expected_default{
      make_ntp("default", "topic-one", 0),
      make_ntp("default", "topic-three", 0),
      make_ntp("default", "topic-three", 1),
      make_ntp("default", "topic-three", 2)};

    std::unordered_set<model::ntp> expected_system{
      make_ntp("system", "sys-topic", 0), make_ntp("system", "sys-topic", 1)};

    BOOST_TEST_REQUIRE(expected_system.size() == 2);
    BOOST_TEST_REQUIRE(expected_default.size() == 4);
    BOOST_TEST_REQUIRE(expected_topic_two.size() == 2);

    auto query = mgr.ntps(model::ns("default"), model::topic("topic-two"));

    for (auto res : query) {
        expected_system.erase(res);
        expected_default.erase(res);
        expected_topic_two.erase(res);
    }

    BOOST_TEST_REQUIRE(expected_system.size() == 2);
    BOOST_TEST_REQUIRE(expected_default.size() == 4);
    BOOST_TEST_REQUIRE(expected_topic_two.size() == 0);

    // be default, only the default namespace is visible
    // and nothing from the 'system' namespace should be deleted.
    for (auto ntp : mgr.ntps()) {
        expected_system.erase(ntp);
        expected_default.erase(ntp);
    }

    // all 4 from default should be removed and none from system
    BOOST_TEST_REQUIRE(expected_system.size() == 2);
    BOOST_TEST_REQUIRE(expected_default.size() == 0);

    // query system ntps and remove them:
    for (auto ntp : mgr.ntps(model::ns("system"))) {
        expected_system.erase(ntp);
    }

    BOOST_TEST_REQUIRE(expected_system.size() == 0);
};
