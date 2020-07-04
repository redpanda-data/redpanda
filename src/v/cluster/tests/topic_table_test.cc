#include "cluster/tests/topic_table_fixture.h"

#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

FIXTURE_TEST(test_happy_path_create, topic_table_fixture) {
    create_topics();
    auto md = table.local().all_topics_metadata();

    BOOST_REQUIRE_EQUAL(md.size(), 3);
    std::sort(
      md.begin(),
      md.end(),
      [](const model::topic_metadata& a, const model::topic_metadata& b) {
          return a.tp_ns.tp < b.tp_ns.tp;
      });
    BOOST_REQUIRE_EQUAL(md[0].tp_ns, make_tp_ns("test_tp_1"));
    BOOST_REQUIRE_EQUAL(md[1].tp_ns, make_tp_ns("test_tp_2"));
    BOOST_REQUIRE_EQUAL(md[2].tp_ns, make_tp_ns("test_tp_3"));

    BOOST_REQUIRE_EQUAL(md[0].partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(md[1].partitions.size(), 12);
    BOOST_REQUIRE_EQUAL(md[2].partitions.size(), 8);

    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    BOOST_REQUIRE_EQUAL(d.size(), 3);
    validate_delta(d[0], 1, 1, 0, 0);
    validate_delta(d[1], 1, 12, 0, 0);
    validate_delta(d[2], 1, 8, 0, 0);
}

FIXTURE_TEST(test_happy_path_delete, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();
    auto res_1 = table.local()
                   .apply(cluster::delete_topic_cmd(
                     make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2")))
                   .get0();
    auto res_2 = table.local()
                   .apply(cluster::delete_topic_cmd(
                     make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3")))
                   .get0();

    auto md = table.local().all_topics_metadata();
    BOOST_REQUIRE_EQUAL(md.size(), 1);
    BOOST_REQUIRE_EQUAL(md[0].tp_ns, make_tp_ns("test_tp_1"));

    BOOST_REQUIRE_EQUAL(md[0].partitions.size(), 1);
    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    BOOST_REQUIRE_EQUAL(d.size(), 2);
    validate_delta(d[0], 0, 0, 1, 12);
    validate_delta(d[1], 0, 0, 1, 8);
}

FIXTURE_TEST(test_conflicts, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();

    auto res_1 = table.local()
                   .apply(cluster::delete_topic_cmd(
                     make_tp_ns("not_exists"), make_tp_ns("not_exists")))
                   .get0();
    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::topic_not_exists);

    auto res_2
      = table.local().apply(make_create_topic_cmd("test_tp_1", 2, 3)).get0();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::topic_already_exists);
    BOOST_REQUIRE_EQUAL(table.local().has_pending_changes(), false);
}

FIXTURE_TEST(get_getting_config, topic_table_fixture) {
    create_topics();
    auto cfg = table.local().get_topic_cfg(make_tp_ns("test_tp_1"));
    BOOST_REQUIRE(cfg.has_value());
    auto v = cfg.value();
    BOOST_REQUIRE_EQUAL(
      v.compaction_strategy, model::compaction_strategy::offset);

    BOOST_REQUIRE_EQUAL(
      v.cleanup_policy_bitflags, model::cleanup_policy_bitflags::compaction);
    BOOST_REQUIRE_EQUAL(v.compression, model::compression::lz4);
    BOOST_REQUIRE_EQUAL(v.retention_bytes, tristate(std::make_optional(2_GiB)));
    BOOST_REQUIRE_EQUAL(
      v.retention_duration,
      tristate(std::make_optional(std::chrono::milliseconds(3600000))));
}

FIXTURE_TEST(test_wait_aborted, topic_table_fixture) {
    ss::abort_source local_as;
    ss::timer<> timer;
    timer.set_callback([&local_as] { local_as.request_abort(); });
    timer.arm(500ms);
    // discard create delta
    BOOST_REQUIRE_THROW(
      table.local().wait_for_changes(local_as).get0(),
      ss::abort_requested_exception);
}