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

using op_t = cluster::partition_operation_type;
using delta_t = cluster::topic_table::delta;
using meta_t = cluster::controller_backend::delta_metadata;
using deltas_t = std::deque<meta_t>;

meta_t make_delta(
  int64_t o,
  op_t type,
  std::vector<model::broker_shard> replicas,
  std::vector<model::broker_shard> previous = {}) {
    switch (type) {
    case op_t::update:
    case op_t::force_update:
        return meta_t(cluster::topic_table_delta::create_cancel_update_delta(
          test_ntp,
          model::revision_id(o),
          cluster::is_forced(type == op_t::force_update),
          make_assignment(std::move(replicas)),
          std::move(previous),
          cluster::replicas_revision_map{}));
    case op_t::cancel_update:
    case op_t::force_cancel_update:
        return meta_t(cluster::topic_table_delta::create_cancel_update_delta(
          test_ntp,
          model::revision_id(o),
          cluster::is_forced(type == op_t::force_cancel_update),
          make_assignment(std::move(replicas)),
          std::move(previous),
          cluster::replicas_revision_map{}));
    case op_t::finish_update:
        return meta_t(cluster::topic_table_delta::create_finish_update_delta(
          test_ntp,
          model::revision_id(o),
          make_assignment(std::move(replicas))));
    case op_t::remove:
        return meta_t(cluster::topic_table_delta::create_remove_partition_delta(
          test_ntp, model::revision_id(o)));
    case op_t::add:
        return meta_t(cluster::topic_table_delta::create_add_partition_delta(
          test_ntp,
          model::revision_id(o),
          make_assignment(std::move(replicas)),
          cluster::replicas_revision_map{}));
    default:
        vassert(false, "not supported operation type: {}", type);
    }
    __builtin_unreachable();
};

meta_t add_current = make_delta(
  1, op_t::add, {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)});

meta_t add_different = make_delta(
  2, op_t::add, {make_bs(3, 0), make_bs(2, 1), make_bs(1, 0)});

meta_t delete_current = make_delta(
  3, op_t::remove, {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)});

meta_t recreate_different = make_delta(4, op_t::add, {make_bs(3, 0)});

meta_t recreate_current = make_delta(5, op_t::add, {make_bs(0, 0)});

meta_t update_with_current = make_delta(
  6,
  op_t::update,
  {make_bs(0, 0), make_bs(10, 0)},
  {make_bs(9, 0), make_bs(10, 0)});

meta_t finish_update_with_current = make_delta(
  7, op_t::finish_update, {make_bs(0, 0), make_bs(10, 0)});

meta_t update_without_current = make_delta(
  8,
  op_t::update,
  {make_bs(1, 0), make_bs(10, 0)},
  {make_bs(9, 0), make_bs(10, 0)});

meta_t finish_update_without_current = make_delta(
  9, op_t::finish_update, {make_bs(1, 0), make_bs(10, 0)});

meta_t update_without_current_2 = make_delta(
  10, op_t::update, {make_bs(10, 0)}, {make_bs(9, 0)});

meta_t finish_update_without_current_2 = make_delta(
  11, op_t::finish_update, {make_bs(10, 0)});

meta_t update_with_current_2 = make_delta(
  12, op_t::update, {make_bs(0, 0)}, {make_bs(1, 0)});

meta_t finish_update_with_current_2 = make_delta(
  13, op_t::finish_update, {make_bs(0, 0)});

meta_t final_delete = make_delta(
  100, op_t::remove, {make_bs(0, 0), make_bs(2, 1), make_bs(1, 0)});

SEASTAR_THREAD_TEST_CASE(test_simple_bootstrap) {
    // add topic on current node
    deltas_t d_1{add_current};
    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(d_1));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(
      deltas.back().delta.revision(), add_current.delta.revision());

    // add topic on different node, should not include this
    deltas_t d_2{add_different};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_2));

    BOOST_REQUIRE_EQUAL(deltas.size(), 0);

    // add & delete topic
    deltas_t d_3{add_current, delete_current};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_3));

    BOOST_REQUIRE_EQUAL(deltas.size(), 0);

    // recreate topic on current node
    deltas_t d_4{add_current, delete_current, recreate_current};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_4));

    BOOST_REQUIRE_EQUAL(deltas.size(), 1);
    BOOST_REQUIRE_EQUAL(
      deltas.back().delta.revision(), recreate_current.delta.revision());

    // recreate topic on different node
    deltas_t d_5{add_current, delete_current, recreate_different};
    deltas = cluster::calculate_bootstrap_deltas(current_node, std::move(d_5));

    BOOST_REQUIRE_EQUAL(deltas.size(), 0);
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
    BOOST_REQUIRE_EQUAL(
      deltas.begin()->delta.revision(), recreate_current.delta.revision());
    BOOST_REQUIRE_EQUAL(
      std::next(deltas.begin())->delta.revision(),
      update_with_current.delta.revision());
    BOOST_REQUIRE_EQUAL(
      std::next(deltas.begin(), 2)->delta.revision(),
      finish_update_with_current.delta.revision());
}

SEASTAR_THREAD_TEST_CASE(update_excluding_current_node) {
    // create topic on current node and update it's replica set including
    // current node
    deltas_t all{
      add_current, update_without_current, finish_update_without_current};

    auto deltas = cluster::calculate_bootstrap_deltas(
      current_node, std::move(all));

    BOOST_REQUIRE_EQUAL(deltas.size(), 0);
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

    BOOST_REQUIRE_EQUAL(deltas.size(), 0);
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
    BOOST_REQUIRE_EQUAL(
      deltas.begin()->delta.revision(), update_with_current_2.delta.revision());
    BOOST_REQUIRE_EQUAL(
      std::next(deltas.begin())->delta.revision(),
      finish_update_with_current_2.delta.revision());
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
    BOOST_REQUIRE_EQUAL(
      deltas.begin()->delta.revision(), update_with_current_2.delta.revision());
}
