#include "model/fundamental.h"
#include "storage2/repository.h"
#include "storage2/tests/random_batch.h"
#include "storage2/tests/test_common.h"

#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/interface.hpp>

#include <filesystem>
#include <set>

using namespace storage;       // NOLINT
using namespace storage::test; // NOLINT

static model::ntp
make_ntp(std::string ns, std::string topic, size_t partition_id) {
    return model::ntp{.ns = model::ns(std::move(ns)),
                      .tp = {.topic = model::topic(std::move(topic)),
                             .partition = model::partition_id(partition_id)}};
}

SEASTAR_THREAD_TEST_CASE(test_repository_api_smoke_test) {
    auto config = repository::config::testing_defaults();

    storage::repository repo
      = storage::repository::open(make_test_dir(), config).get0();

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

    repo.close().wait();

    // cleanup
    std::filesystem::remove_all(repo.working_directory());
}

SEASTAR_THREAD_TEST_CASE(repo_read_write_one_batch) {
    auto config = repository::config::testing_defaults();

    storage::repository repo
      = storage::repository::open(make_test_dir(), config).get0();

    auto topic_one_0
      = repo.create_ntp(make_ntp("default", "topic-one", 0)).get0();

    auto result = topic_one_0
                    .append(make_random_batch(model::offset(0), 100, false))
                    .get0();

    topic_one_0.close().wait();
}

SEASTAR_THREAD_TEST_CASE(test_repository_ntp_query) {
    auto testdir = make_test_dir();
    std::filesystem::path workingdir(make_test_dir().c_str());
    auto config = repository::config::testing_defaults();

    std::filesystem::create_directories(
      workingdir / "default" / "topic-one" / "0");

    std::filesystem::create_directories(
      workingdir / "default" / "topic-two" / "0");
    std::filesystem::create_directories(
      workingdir / "default" / "topic-two" / "1");

    std::filesystem::create_directories(
      workingdir / "default" / "topic-three" / "0");
    std::filesystem::create_directories(
      workingdir / "default" / "topic-three" / "1");
    std::filesystem::create_directories(
      workingdir / "default" / "topic-three" / "2");

    std::filesystem::create_directories(
      workingdir / "system" / "sys-topic" / "0");
    std::filesystem::create_directories(
      workingdir / "system" / "sys-topic" / "1");

    auto repo = storage::repository::open(workingdir.string(), config).get0();
    BOOST_TEST_REQUIRE(
      repo.working_directory().string() == workingdir.string());

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
    repo.close().wait();
}