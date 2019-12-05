#include "cluster/simple_batch_builder.h"
#include "cluster/tests/controller_test_fixture.h"
#include "test_utils/fixture.h"

void validate_topic_metadata(
  cluster::metadata_cache& cache, const sstring& topic, int partition_count) {
    auto tp_md = cache.get_topic_metadata(model::topic(topic));
    BOOST_REQUIRE_EQUAL(tp_md.has_value(), true);
    BOOST_REQUIRE_EQUAL(tp_md->partitions.size(), partition_count);
    BOOST_REQUIRE_EQUAL(tp_md->tp, model::topic(topic));
}

FIXTURE_TEST(
  recover_single_topic_test_at_current_broker, controller_tests_fixture) {
    persist_test_batches(single_topic_current_broker());

    auto& cntrl = get_controller();
    cntrl.start().get0();
    wait_for_leadership(cntrl);
    // Check topics are in cache
    auto all_topics = get_local_cache().all_topics();
    BOOST_REQUIRE_EQUAL(all_topics.size(), 1);
    validate_topic_metadata(get_local_cache(), "topic_1", 2);
}

#if 0
// TODO: enable when assigning raft nodes will be ready

FIXTURE_TEST(
  recover_single_topic_test_at_different_node, controller_tests_fixture) {
    persist_test_batches(single_topic_other_broker());

    auto& cntrl = get_controller();
    cntrl.start().get0();
    wait_for_leadership(cntrl);
    auto all_topics = get_local_cache().all_topics();
    BOOST_REQUIRE_EQUAL(all_topics.size(), 1);
    auto tp_md = get_local_cache().get_topic_metadata(model::topic("topic_2"));
    validate_topic_metadata(get_local_cache(), "topic_2", 2);
}

FIXTURE_TEST(recover_multiple_topics, controller_tests_fixture) {
    persist_test_batches(two_topics());

    auto& cntrl = get_controller();
    cntrl.start().get0();
    wait_for_leadership(cntrl);
    auto all_topics = get_local_cache().all_topics();
    BOOST_REQUIRE_EQUAL(all_topics.size(), 2);
    validate_topic_metadata(get_local_cache(), "topic_1", 2);
    validate_topic_metadata(get_local_cache(), "topic_2", 2);
}
#endif

FIXTURE_TEST(recover_complex, controller_tests_fixture) {
    persist_test_batches(make_complex_topics());

    auto& cntrl = get_controller();
    cntrl.start().get0();
    wait_for_leadership(cntrl);
    auto all_topics = get_local_cache().all_topics();
    BOOST_REQUIRE_EQUAL(all_topics.size(), complex_topic_count);
    BOOST_REQUIRE_EQUAL(
      cntrl.get_highest_group_id()(), complex_partitions_count);
}