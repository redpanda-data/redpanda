#include "cluster/metadata_cache.h"
#include "cluster/tests/utils.h"

#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

cluster::metadata_cache create_test_cache() {
    cluster::metadata_cache cache;
    auto tp = model::topic("test_topic");
    cache.add_topic(tp);
    return cache;
}

model::broker create_test_broker(int32_t id) {
    return model::broker(
      model::node_id(id),                    // id
      unresolved_address("127.0.0.1", 9092), // kafka api address
      unresolved_address("127.0.0.1", 9999), // rpc address
      std::nullopt,
      model::broker_properties{
        .cores = 8 // cores
      });
}

SEASTAR_THREAD_TEST_CASE(test_getting_not_existing_topic_metatadata) {
    cluster::metadata_cache cache;
    auto md = cache.get_topic_metadata(model::topic_view("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), false);
}

SEASTAR_THREAD_TEST_CASE(test_getting_topics_list_from_empty_cache) {
    cluster::metadata_cache cache;
    auto all_topics = cache.all_topics();
    BOOST_REQUIRE_EQUAL(all_topics.empty(), true);
}

SEASTAR_THREAD_TEST_CASE(test_getting_topic_metadata) {
    auto cache = create_test_cache();
    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}, {1, 0}, {2, 0}}, 1));
    cache.update_partition_assignment(
      create_test_assignment("test_topic", 1, {{0, 0}, {1, 0}, {2, 0}}, 1));
    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(md->partitions.size(), 2);
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas.size(), 3);
    BOOST_REQUIRE_EQUAL(md->partitions[1].replicas.size(), 3);

    BOOST_REQUIRE_EQUAL(md->tp(), "test_topic");
}

SEASTAR_THREAD_TEST_CASE(test_getting_topics_list) {
    cluster::metadata_cache cache;
    cache.add_topic(model::topic("test_topic_1"));
    cache.add_topic(model::topic("test_topic_2"));
    cache.add_topic(model::topic("test_topic_3"));
    auto all_topics = cache.all_topics();

    auto find_topic = [&all_topics](const model::topic& tp) {
        return std::find_if(
          all_topics.begin(),
          all_topics.end(),
          [&tp](const model::topic_view& view) {
              return static_cast<model::topic_view>(tp) == view;
          });
    };
    BOOST_CHECK(find_topic(model::topic("test_topic_1")) != all_topics.end());
    BOOST_CHECK(find_topic(model::topic("test_topic_2")) != all_topics.end());
    BOOST_CHECK(find_topic(model::topic("test_topic_3")) != all_topics.end());
}

SEASTAR_THREAD_TEST_CASE(test_updating_not_existing_partition) {
    auto cache = create_test_cache();

    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}}, 1));

    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(md->partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(md->partitions[0].id, model::partition_id(0));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_adding_replica_to_existing_partition) {
    auto cache = create_test_cache();

    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}}, 1));
    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}, {1, 0}}, 1));

    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(md->partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(md->partitions[0].id, model::partition_id(0));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas.size(), 2);
    BOOST_REQUIRE_EQUAL(
      md->partitions[0].replicas[0].node_id, model::node_id(0));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas[0].shard, 0);
    BOOST_REQUIRE_EQUAL(
      md->partitions[0].replicas[1].node_id, model::node_id(1));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas[1].shard, 0);
}

SEASTAR_THREAD_TEST_CASE(
  test_adding_new_partition_to_topic_with_one_that_already_exists) {
    auto cache = create_test_cache();

    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}}, 1));
    cache.update_partition_assignment(
      create_test_assignment("test_topic", 1, {{0, 0}}, 1));

    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(md->partitions.size(), 2);
    BOOST_REQUIRE_EQUAL(md->partitions[0].id, model::partition_id(0));
    BOOST_REQUIRE_EQUAL(md->partitions[1].id, model::partition_id(1));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas.size(), 1);
    BOOST_REQUIRE_EQUAL(md->partitions[1].replicas.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_updating_replica_assignment) {
    auto cache = create_test_cache();

    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}}, 1));
    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{20, 0}}, 1));

    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(md->partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(md->partitions[0].id, model::partition_id(0));
    BOOST_REQUIRE_EQUAL(md->partitions[0].replicas.size(), 1);
    BOOST_REQUIRE_EQUAL(
      md->partitions[0].replicas[0].node_id, model::node_id(20));
}

SEASTAR_THREAD_TEST_CASE(test_updating_partition_leader) {
    auto cache = create_test_cache();

    cache.update_partition_assignment(
      create_test_assignment("test_topic", 0, {{0, 0}}, 1));
    cache.update_partition_leader(
      model::topic("test_topic"), model::partition_id(0), model::node_id(1));

    auto md = cache.get_topic_metadata(model::topic("test_topic"));
    BOOST_REQUIRE_EQUAL(md.has_value(), true);
    BOOST_REQUIRE_EQUAL(
      md->partitions[0].leader_node.value(), model::node_id(1));
}

SEASTAR_THREAD_TEST_CASE(test_updating_brokers_cache) {
    auto cache = create_test_cache();
    cache.update_brokers_cache({
      create_test_broker(1),
      create_test_broker(2),
      create_test_broker(3),
    });
    auto brokers = cache.all_brokers();
    BOOST_REQUIRE_EQUAL(brokers.size(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_getting_single_broker) {
    auto cache = create_test_cache();
    cache.update_brokers_cache({
      create_test_broker(1),
      create_test_broker(2),
      create_test_broker(3),
    });

    auto broker = cache.get_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(broker.has_value(), true);
    BOOST_REQUIRE_EQUAL(broker.value()->id(), model::node_id(1));
}