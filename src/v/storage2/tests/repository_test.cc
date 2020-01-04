#include "storage2/repository.h"
#include "storage2/tests/random_batch.h"
#include "storage2/tests/storage_test_fixture.h"

#include <seastar/testing/thread_test_case.hh>

using namespace storage;       // NOLINT
using namespace storage::test; // NOLINT

FIXTURE_TEST(test_repository_api_smoke_test, storage_test_fixture) {
    storage::repository repo = make_repo();
    // write path
    auto t1p0 = repo.create_ntp(make_ntp("default", "topic-one", 0)).get0();
    auto t1p1 = repo.create_ntp(make_ntp("default", "topic-one", 1)).get0();

    t1p0.append(make_random_batch_v2(100)).wait();
    t1p0.append(make_random_batch_v2(50)).wait();
    t1p1.append(make_random_batch_v2(10)).wait();
    t1p1.append(make_random_batch_v2(5)).wait();

    t1p0.close().wait();
    t1p1.close().wait();

    // read path
    std::unordered_set<model::ntp> expected{
      make_ntp("default", "topic-one", 0),
      make_ntp("default", "topic-one", 1),
    };

    for (auto ntp : repo.ntps()) {
        auto partition = repo.open_ntp(ntp).get0();
        partition.close().wait();
        expected.erase(ntp);
    }
    BOOST_TEST_REQUIRE(expected.size() == 0);
};

FIXTURE_TEST(repo_read_write_one_batch, storage_test_fixture) {
    storage::repository repo = make_repo();
    auto topic_one_0
      = repo.create_ntp(make_ntp("default", "topic-one", 0)).get0();

    auto result = topic_one_0
                    .append(make_random_batch(model::offset(0), 100, false))
                    .get0();

    topic_one_0.close().wait();
};

FIXTURE_TEST(test_repository_ntp_query, storage_test_fixture) {
    create_topic_dir("default", "topic-one", 0);
    create_topic_dir("default", "topic-two", 0);
    create_topic_dir("default", "topic-two", 1);
    create_topic_dir("default", "topic-three", 0);
    create_topic_dir("default", "topic-three", 1);
    create_topic_dir("default", "topic-three", 2);
    create_topic_dir("system", "sys-topic", 0);
    create_topic_dir("system", "sys-topic", 1);

    storage::repository repo = make_repo();
    BOOST_TEST_REQUIRE(sstring(repo.working_directory().string()) == test_dir);

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

    auto query = repo.ntps(model::ns("default"), model::topic("topic-two"));

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
    for (auto ntp : repo.ntps()) {
        expected_system.erase(ntp);
        expected_default.erase(ntp);
    }

    // all 4 from default should be removed and none from system
    BOOST_TEST_REQUIRE(expected_system.size() == 2);
    BOOST_TEST_REQUIRE(expected_default.size() == 0);

    // query system ntps and remove them:
    for (auto ntp : repo.ntps(model::ns("system"))) {
        expected_system.erase(ntp);
    }

    BOOST_TEST_REQUIRE(expected_system.size() == 0);
};