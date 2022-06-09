#include "cluster/controller_backend.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/testing/thread_test_case.hh>

#include <bits/stdint-uintn.h>
#include <boost/test/tools/old/interface.hpp>

#include <vector>

static const model::node_id current_node(0);

static const model::ntp test_ntp(
  model::ns("test"),
  model::topic_partition(model::topic("test"), model::partition_id(1)));

model::broker_shard make_bs(uint32_t node, uint32_t shard) {
    return model::broker_shard{.node_id = model::node_id(node), .shard = shard};
}

cluster::partition_assignment
make_assignment(std::vector<model::broker_shard> replicas) {
    return cluster::partition_assignment{
      raft::group_id(1), model::partition_id(1), std::move(replicas)};
}

using op_t = cluster::topic_table::delta::op_type;
using delta_t = cluster::topic_table::delta;
using deltas_t = std::vector<cluster::topic_table::delta>;

cluster::topic_table::delta make_delta(
  std::vector<model::broker_shard> replicas,
  int64_t o,
  cluster::topic_table::delta::op_type type) {
    return cluster::topic_table::delta(
      test_ntp, make_assignment(std::move(replicas)), model::offset(o), type);
};

delta_t add_current = make_delta(
  {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)}, 1, op_t::add);

delta_t add_different = make_delta(
  {make_bs(3, 0), make_bs(2, 1), make_bs(1, 0)}, 2, op_t::add);

delta_t delete_current = make_delta(
  {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)}, 3, op_t::del);

delta_t recreate_different = make_delta({make_bs(3, 0)}, 4, op_t::add);

delta_t recreate_current = make_delta({make_bs(0, 0)}, 5, op_t::add);

delta_t update_with_current = make_delta(
  {make_bs(0, 0), make_bs(10, 0)}, 6, op_t::update);

delta_t finish_update_with_current = make_delta(
  {make_bs(0, 0), make_bs(10, 0)}, 7, op_t::update_finished);

delta_t update_without_current = make_delta(
  {make_bs(1, 0), make_bs(10, 0)}, 8, op_t::update);

delta_t finish_update_without_current = make_delta(
  {make_bs(1, 0), make_bs(10, 0)}, 9, op_t::update_finished);

delta_t update_without_current_2 = make_delta(
  {make_bs(10, 0)}, 10, op_t::update);

delta_t finish_update_without_current_2 = make_delta(
  {make_bs(10, 0)}, 11, op_t::update_finished);

delta_t update_with_current_2 = make_delta({make_bs(0, 0)}, 12, op_t::update);

delta_t finish_update_with_current_2 = make_delta(
  {make_bs(0, 0)}, 13, op_t::update_finished);

delta_t final_delete = make_delta(
  {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)}, 100, op_t::del);

SEASTAR_THREAD_TEST_CASE(test_simple_bootstrap) {
    // add topic on current node
    deltas_t d_1{add_current};
    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(d_1));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas.back().offset, add_current.offset);

    // add topic on different node, should include this as this is noop on the
    // current node
    deltas_t d_2{add_different};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_2));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas.back().offset, add_different.offset);

    // add & delete topic
    deltas_t d_3{add_current, delete_current};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_3));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas.back().offset, delete_current.offset);

    // recreate topic on current node
    deltas_t d_4{add_current, delete_current, recreate_current};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_4));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas.back().offset, recreate_current.offset);

    // recreate topic on different node
    deltas_t d_5{add_current, delete_current, recreate_different};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_5));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas.back().offset, recreate_different.offset);
}

SEASTAR_THREAD_TEST_CASE(update_including_current_node) {
    // recreate topic on current node and update it's replica set including
    // current node
    deltas_t d_1{
      add_current,
      delete_current,
      recreate_current,
      update_with_current,
      finish_update_with_current};

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(d_1));

    BOOST_REQUIRE_EQUAL(deltas.size(), 3);
    BOOST_REQUIRE_EQUAL(deltas[0].offset, recreate_current.offset);
    BOOST_REQUIRE_EQUAL(deltas[1].offset, update_with_current.offset);
    BOOST_REQUIRE_EQUAL(deltas[2].offset, finish_update_with_current.offset);
}

SEASTAR_THREAD_TEST_CASE(update_excluding_current_node) {
    // create topic on current node and update it's replica set including
    // current node
    deltas_t all{
      add_current, update_without_current, finish_update_without_current};

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(all));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas[0].offset, finish_update_without_current.offset);
}

SEASTAR_THREAD_TEST_CASE(final_delete_on_current_node) {
    // recreate topic on current node and update it's replica set including
    // current node
    deltas_t d_1{
      add_current,
      delete_current,
      recreate_current,
      update_with_current,
      finish_update_with_current,
      final_delete};

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(d_1));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas[0].offset, final_delete.offset);
}

SEASTAR_THREAD_TEST_CASE(move_back_to_current_node) {
    // create topic on current node and update it's replica set including
    // current node
    deltas_t all{
      add_current,
      update_without_current,
      finish_update_without_current,
      update_without_current_2,
      finish_update_without_current_2,
      update_with_current_2,
      finish_update_with_current_2,
    };

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(all));

    BOOST_REQUIRE_EQUAL(deltas.size(), 2);
    BOOST_REQUIRE_EQUAL(deltas[0].offset, update_with_current_2.offset);
    BOOST_REQUIRE_EQUAL(deltas[1].offset, finish_update_with_current_2.offset);
}

SEASTAR_THREAD_TEST_CASE(move_back_to_current_node_not_finished) {
    // create topic on current node and update it's replica set including
    // current node
    deltas_t all{
      add_current,
      update_without_current,
      finish_update_without_current,
      update_without_current_2,
      finish_update_without_current_2,
      update_with_current_2,
    };

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(all));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(deltas[0].offset, update_with_current_2.offset);
}
