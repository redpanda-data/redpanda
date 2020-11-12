// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/metadata.h"
#include "ssx/future-util.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_set.h>
#include <bits/stdint-intn.h>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/tuple/tuple.hpp>
#include <fmt/ostream.h>

#include <algorithm>
#include <memory>

using batches_t = ss::circular_buffer<model::record_batch>;
using batches_ptr_t = ss::lw_shared_ptr<batches_t>;
using foreign_batches_t = ss::foreign_ptr<batches_ptr_t>;

void wait_for_metadata(
  cluster::topic_table& topic_table,
  const std::vector<cluster::topic_result>& results) {
    tests::cooperative_spin_wait_with_timeout(2s, [&results, &topic_table] {
        return std::all_of(
          results.begin(),
          results.end(),
          [&topic_table](const cluster::topic_result& r) {
              return topic_table.get_topic_metadata(r.tp_ns);
          });
    }).get0();
}

bool validate_partiton(
  const model::ntp& ntp,
  const cluster::partition_manager& pm,
  const std::vector<model::broker_shard>& bs) {
    auto partition = pm.get(ntp);

    if (!partition) {
        return false;
    }
    auto cfg = partition->group_configuration();

    if (cfg.old_config()) {
        return false;
    }

    if (cfg.brokers().size() != bs.size()) {
        return false;
    }

    if (cfg.current_config().voters.size() != bs.size()) {
        return false;
    }

    absl::flat_hash_set<model::node_id> unique_ids;

    for (auto& v : cfg.current_config().voters) {
        unique_ids.emplace(v);
    }

    if (unique_ids.size() != bs.size()) {
        return false;
    }

    for (auto& b : bs) {
        unique_ids.emplace(b.node_id);
    }

    return unique_ids.size() == bs.size();
}

template<typename Predicate>
ss::future<> wait_for_all_replicas(
  std::vector<model::broker_shard> replica_set, Predicate p) {
    return tests::cooperative_spin_wait_with_timeout(
      10s, [replica_set = std::move(replica_set), p = std::move(p)]() mutable {
          using partition_ptr = ss::lw_shared_ptr<cluster::partition>;
          return ssx::parallel_transform(
                   replica_set.cbegin(),
                   replica_set.cend(),
                   [p](const model::broker_shard& bs) mutable { return p(bs); })
            .then([](std::vector<bool> res) {
                return std::all_of(
                  std::cbegin(res), std::cend(res), [](bool r) { return r; });
            });
      });
}

bool are_batches_the_same(
  const foreign_batches_t& a, const foreign_batches_t& b) {
    if (a->size() != b->size()) {
        return false;
    }

    auto p = std::mismatch(
      a->begin(),
      a->end(),
      b->begin(),
      b->end(),
      [](
        const model::record_batch& a_batch,
        const model::record_batch& b_batch) { return a_batch == b_batch; });

    return p.first == a->end() && p.second == b->end();
}
class partition_assignment_test_fixture : public cluster_test_fixture {
public:
    partition_assignment_test_fixture() {
        // start 3 nodes
        controllers.push_back(create_controller(nodes[0]));
        controller(0)->start().get0();
        wait_for_leadership(controller(0)->get_partition_leaders().local());
        controllers.push_back(create_controller(nodes[1]));
        controller(1)->start().get0();
        controllers.push_back(create_controller(nodes[2]));
        controller(2)->start().get0();
        // wait for cluster to be stable
        tests::cooperative_spin_wait_with_timeout(2s, [this] {
            return std::all_of(
              controllers.cbegin(),
              controllers.cend(),
              [this](cluster::controller* c) {
                  return c->get_members_table().local().all_broker_ids().size()
                         == nodes.size();
              });
        }).get0();

        ntp = model::ntp(
          model::topic(test_ns), model::topic("tp-1"), model::partition_id(0));
    }

    cluster::topic_configuration
    test_topics_configuration(int replication_factor) {
        return cluster::topic_configuration(
          ntp.ns, ntp.tp.topic, 1, replication_factor);
    }

    cluster::controller* get_leader_controller() {
        auto c = *controllers.begin();
        auto leader = c->get_partition_leaders().local().get_leader(
          cluster::controller_ntp);

        return controllers[(*leader)()];
    }

    ss::future<> wait_for_metadata_update(
      model::ntp ntp, std::vector<model::broker_shard> replica_set) {
        return tests::cooperative_spin_wait_with_timeout(
          2s,
          [this,
           ntp = std::move(ntp),
           replica_set = std::move(replica_set)]() mutable {
              return std::all_of(
                nodes.cbegin(),
                nodes.cbegin(),
                [this,
                 ntp = std::move(ntp),
                 replica_set = std::move(replica_set)](
                  model::node_id nid) mutable {
                    auto& md_cache = get_local_cache(nid);
                    auto md = md_cache.get_topic_metadata(
                      model::topic_namespace_view(ntp));

                    if (!md) {
                        return false;
                    }

                    return true;
                });
          });
    }

    std::vector<model::broker_shard>
    get_replicas(int source_node, const model::ntp& ntp) {
        auto md = controller(source_node)
                    ->get_topics_state()
                    .local()
                    .get_topic_metadata(model::topic_namespace_view(ntp));

        return md->partitions.begin()->replicas;
    }

    void wait_for_replica_set_partitions(
      model::ntp ntp, std::vector<model::broker_shard> replicas) {
        wait_for_all_replicas(
          replicas,
          [this, ntp = std::move(ntp), replicas = std::move(replicas)](
            const model::broker_shard& bs) mutable {
              return get_partition_manager(bs.node_id)
                .invoke_on(
                  bs.shard,
                  [ntp, replicas](const cluster::partition_manager& pm) {
                      return validate_partiton(ntp, pm, replicas);
                  });
          })
          .get0();
    }

    void check_if_ntp_replica_not_exists(
      const model::ntp& ntp, const model::broker_shard& bs) {
        tests::cooperative_spin_wait_with_timeout(5s, [this, bs, ntp] {
            auto& pm = get_partition_manager(bs.node_id);
            return pm.invoke_on(
              bs.shard, [ntp](cluster::partition_manager& pm) {
                  auto p = pm.get(ntp);
                  // partition does not exists
                  return !p;
              });
        }).get0();
    }

    void create_topic(cluster::topic_configuration cfg) {
        auto res = get_leader_controller()
                     ->get_topics_frontend()
                     .local()
                     .autocreate_topics({std::move(cfg)}, 2s)
                     .get0();
        wait_for_metadata(controller(0)->get_topics_state().local(), res);
    }

    ss::future<foreign_batches_t> read_replica_batches(
      model::broker_shard replica, model::ntp ntp, model::offset max_offset) {
        auto& pm = get_partition_manager(replica.node_id);
        return pm.invoke_on(
          replica.shard,
          [ntp = std::move(ntp), max_offset](cluster::partition_manager& pm) {
              auto p = pm.get(ntp);
              if (!p) {
                  return ss::make_ready_future<foreign_batches_t>(
                    ss::make_lw_shared<batches_t>({}));
              }
              storage::log_reader_config cfg(
                model::offset(0), max_offset, ss::default_priority_class());

              return p->make_reader(cfg).then([](model::record_batch_reader r) {
                  return model::consume_reader_to_memory(
                           std::move(r), model::no_timeout)
                    .then([](batches_t batches) {
                        return ss::make_foreign<batches_ptr_t>(
                          ss::make_lw_shared<batches_t>(std::move(batches)));
                    });
              });
          });
    }

    foreign_batches_t replicate_data(model::ntp ntp, int count) {
        // wait for topic to be created
        tests::cooperative_spin_wait_with_timeout(5s, [this, ntp] {
            return get_local_cache(nodes[0])
              .get_leader(ntp, model::no_timeout)
              .then([this, ntp](model::node_id leader_id) {
                  auto shard = get_shard_table(leader_id).shard_for(ntp);
                  if (!shard) {
                      return ss::make_ready_future<bool>(false);
                  }
                  auto& pm = get_partition_manager(leader_id);
                  return pm.invoke_on(
                    *shard, [ntp](cluster::partition_manager& pm) {
                        auto partition = pm.get(ntp);
                        return partition && partition->is_leader();
                    });
              });
        }).get0();

        // at this point we are ready to replicate data
        auto leader_id
          = get_local_cache(nodes[0]).get_leader(ntp, model::no_timeout).get0();
        auto shard = get_shard_table(leader_id).shard_for(ntp);
        auto& pm = get_partition_manager(leader_id);

        return pm
          .invoke_on(
            *shard,
            [count, ntp = std::move(ntp)](cluster::partition_manager& pm) {
                auto batches = storage::test::make_random_batches(
                  model::offset(0), count);
                auto rdr = model::make_memory_record_batch_reader(
                  std::move(batches));

                return pm.get(ntp)
                  ->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .then([&pm, ntp](result<raft::replicate_result> res) {
                      BOOST_REQUIRE(res.has_value());
                      auto p = pm.get(ntp);
                      return p->make_reader(storage::log_reader_config(
                        model::offset(0),
                        p->committed_offset(),
                        ss::default_priority_class()));
                  })
                  .then([](model::record_batch_reader r) {
                      return model::consume_reader_to_memory(
                               std::move(r), model::no_timeout)
                        .then([](batches_t batches) {
                            return ss::make_foreign<batches_ptr_t>(
                              ss::make_lw_shared<batches_t>(
                                std::move(batches)));
                        });
                  });
            })
          .get0();
    }

    void validate_replicas_recovery(
      model::ntp ntp,
      std::vector<model::broker_shard> replicas,
      const foreign_batches_t& reference_batches) {
        BOOST_REQUIRE(!reference_batches->empty());
        auto max_offset = reference_batches->back().last_offset();
        wait_for_all_replicas(
          std::move(replicas),
          [this, ntp = std::move(ntp), &reference_batches, max_offset](
            model::broker_shard bs) {
              return read_replica_batches(bs, ntp, max_offset)
                .then(
                  [this, &reference_batches, bs](foreign_batches_t batches) {
                      return are_batches_the_same(reference_batches, batches);
                  });
          })
          .get0();
    }

    cluster::controller* controller(int id) { return controllers[id]; }

    std::vector<cluster::controller*> controllers;
    std::vector<model::node_id> nodes = {
      model::node_id(0), model::node_id(1), model::node_id(2)};
    model::ntp ntp;
};

/**
 * This test contains following test cases:
 * legend:
 *  [<id>,...] - denotes replica set nodes
 *
 * - growing replica set by one node      [0] -> [0,1]
 * - moving replica to another node       [0] -> [1]
 * - removing node from replica set       [0,1,2] -> [0,1]
 * - moving one of the replicas           [0,1] -> [0,2]
 * - adding multiple replicas             [0] -> [0,1,2]
 *
 */

FIXTURE_TEST(add_broker_to_replica_set, partition_assignment_test_fixture) {
    create_topic(test_topics_configuration(1));

    auto reference_batches = replicate_data(ntp, 10);

    std::vector<model::broker_shard> new_replica_set;
    // add new node to replica set
    new_replica_set = get_replicas(0, ntp);
    new_replica_set.push_back({model::broker_shard{
      .node_id = nodes[2],
      .shard = ss::this_shard_id(),
    }});

    get_leader_controller()
      ->get_topics_frontend()
      .local()
      .move_partition_replicas(ntp, new_replica_set, model::no_timeout)
      .get0();

    // check topics table
    wait_for_metadata_update(ntp, new_replica_set).get0();
    info("caches are up to date");

    // check if replicas are there
    wait_for_replica_set_partitions(ntp, new_replica_set);
    validate_replicas_recovery(ntp, new_replica_set, reference_batches);
}

FIXTURE_TEST(move_single_replica, partition_assignment_test_fixture) {
    create_topic(test_topics_configuration(1));

    auto reference_batches = replicate_data(ntp, 10);

    auto old_replicas = get_replicas(0, ntp);
    auto old_replica = *old_replicas.begin();

    std::vector<model::broker_shard> new_replica_set;
    // add new node to replica set
    new_replica_set.push_back({model::broker_shard{
      .node_id = nodes[2],
      .shard = ss::this_shard_id(),
    }});

    get_leader_controller()
      ->get_topics_frontend()
      .local()
      .move_partition_replicas(ntp, new_replica_set, model::no_timeout)
      .get0();

    // check topics table
    wait_for_metadata_update(ntp, new_replica_set).get0();

    // check if replicas are there
    wait_for_replica_set_partitions(ntp, new_replica_set);

    check_if_ntp_replica_not_exists(ntp, old_replica);
    validate_replicas_recovery(ntp, new_replica_set, reference_batches);
}

FIXTURE_TEST(
  remove_broker_from_replica_set, partition_assignment_test_fixture) {
    // we start with 3 replicas in replica set
    create_topic(test_topics_configuration(3));
    auto reference_batches = replicate_data(ntp, 10);

    std::vector<model::broker_shard> new_replica_set;
    // add new node to replica set
    new_replica_set = get_replicas(0, ntp);
    // pop one of the nodes from replica set
    auto removed_replica = new_replica_set.back();
    new_replica_set.pop_back();
    get_leader_controller()
      ->get_topics_frontend()
      .local()
      .move_partition_replicas(ntp, new_replica_set, model::no_timeout)
      .get0();

    // check topics table
    wait_for_metadata_update(ntp, new_replica_set).get0();
    // check if replicas are there
    wait_for_replica_set_partitions(ntp, new_replica_set);

    check_if_ntp_replica_not_exists(ntp, removed_replica);
    validate_replicas_recovery(ntp, new_replica_set, reference_batches);
}

FIXTURE_TEST(move_one_of_the_replicas, partition_assignment_test_fixture) {
    // we start with 2 replicas in replica set
    create_topic(test_topics_configuration(2));

    auto reference_batches = replicate_data(ntp, 10);

    std::vector<model::broker_shard> new_replica_set;
    // move one of the replicas
    new_replica_set = get_replicas(0, ntp);
    auto old_replica = new_replica_set[1];
    auto current_node = new_replica_set[1].node_id;
    new_replica_set[1].node_id = model::node_id(current_node() + 1 % 3);

    get_leader_controller()
      ->get_topics_frontend()
      .local()
      .move_partition_replicas(ntp, new_replica_set, model::no_timeout)
      .get0();

    // check topics table
    wait_for_metadata_update(ntp, new_replica_set).get0();
    // check if replicas are there
    wait_for_replica_set_partitions(ntp, new_replica_set);

    check_if_ntp_replica_not_exists(ntp, old_replica);
    validate_replicas_recovery(ntp, new_replica_set, reference_batches);
}

FIXTURE_TEST(add_two_nodes_to_replica_set, partition_assignment_test_fixture) {
    // we start with 2 replicas in replica set
    create_topic(test_topics_configuration(1));

    auto reference_batches = replicate_data(ntp, 10);

    std::vector<model::broker_shard> new_replica_set;
    // move one of the replicas
    new_replica_set = get_replicas(0, ntp);

    new_replica_set.push_back(
      model::broker_shard{.node_id = model::node_id(1), .shard = 0});
    new_replica_set.push_back(
      model::broker_shard{.node_id = model::node_id(2), .shard = 0});

    get_leader_controller()
      ->get_topics_frontend()
      .local()
      .move_partition_replicas(ntp, new_replica_set, model::no_timeout)
      .get0();

    // check topics table
    wait_for_metadata_update(ntp, new_replica_set).get0();
    // check if replicas are there
    wait_for_replica_set_partitions(ntp, new_replica_set);

    validate_replicas_recovery(ntp, new_replica_set, reference_batches);
}
