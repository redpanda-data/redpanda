#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "cluster/shard_table.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/topics_frontend.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <absl/container/flat_hash_map.h>
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

bool validate_partiton(
  const model::ntp& ntp,
  const cluster::partition_manager& pm,
  ss::foreign_ptr<ss::lw_shared_ptr<std::vector<model::broker_shard>>> bs) {
    auto partition = pm.get(ntp);

    if (!partition) {
        return false;
    }
    auto cfg = partition->group_configuration();

    if (cfg.old_config()) {
        return false;
    }

    if (cfg.brokers().size() != bs->size()) {
        return false;
    }

    if (cfg.current_config().voters.size() != bs->size()) {
        return false;
    }

    absl::flat_hash_set<model::node_id> unique_ids;

    for (auto& v : cfg.current_config().voters) {
        unique_ids.emplace(v.id());
    }

    if (unique_ids.size() != bs->size()) {
        return false;
    }

    for (auto& b : *bs) {
        unique_ids.emplace(b.node_id);
    }

    return unique_ids.size() == bs->size();
}

template<typename Predicate>
ss::future<> wait_for_all_replicas(
  model::timeout_clock::duration tout,
  std::vector<model::broker_shard> replica_set,
  Predicate p) {
    return tests::cooperative_spin_wait_with_timeout(
      tout, [replica_set = std::move(replica_set), p = std::move(p)]() mutable {
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

struct op {
    virtual ss::future<bool> operator()(cluster::topics_frontend&) = 0;
    virtual ss::sstring message() const;
};

static ss::logger logger("partition_move_test");
class partition_assignment_test_fixture : public cluster_test_fixture {
public:
    using replicas_t = std::vector<model::broker_shard>;
    using history_t = std::vector<replicas_t>;

    partition_assignment_test_fixture() { start_cluster(); }

    ~partition_assignment_test_fixture() {
        controllers.clear();
        for (auto n : nodes) {
            vlog(logger.info, "terminating node: {}", n);
            remove_node_application(n);
            vlog(logger.info, "terminated node: {}", n);
        }
    }

    void start_cluster() {
        // start 3 nodes
        for (auto& n : nodes) {
            auto app = create_node_application(n);
            controllers.push_back(app->controller.get());
        }
        // wait for cluster to be stable
        tests::cooperative_spin_wait_with_timeout(60s, [this] {
            return std::all_of(
              controllers.cbegin(),
              controllers.cend(),
              [this](cluster::controller* c) {
                  return c->get_members_table().local().all_broker_ids().size()
                         == nodes.size();
              });
        }).get0();
    }

    void restart_cluster() {
        BOOST_TEST_MESSAGE("restarting cluster");
        controllers.clear();
        for (auto n : nodes) {
            vlog(logger.info, "stopping stack at node: {}", n);
            remove_node_application(n);
            vlog(logger.info, "stopped stack at node: {}", n);
        }
        start_cluster();
    }

    cluster::topic_configuration test_topics_configuration(
      model::topic_namespace tp_ns, int replication_factor) {
        return cluster::topic_configuration(
          tp_ns.ns, tp_ns.tp, 1, replication_factor);
    }

    cluster::controller* get_leader_controller() {
        auto c = *controllers.begin();
        auto leader = c->get_partition_leaders().local().get_leader(
          model::controller_ntp);

        return controllers[(*leader)()];
    }

    void wait_for_metadata_update(
      model::ntp ntp, const std::vector<model::broker_shard>& replica_set) {
        tests::cooperative_spin_wait_with_timeout(
          5s,
          [this, ntp, &replica_set]() mutable {
              return std::all_of(
                nodes.cbegin(),
                nodes.cend(),
                [this, ntp, &replica_set](model::node_id nid) mutable {
                    auto app = get_node_application(nid);
                    auto md = app->controller->get_topics_state()
                                .local()
                                .get_partition_assignment(ntp);

                    if (!md) {
                        vlog(logger.info, "No metadata for ntp {}", ntp);
                        return false;
                    }
                    vlog(
                      logger.info,
                      "Comparing {} {} to {}",
                      ntp,
                      replica_set,
                      md->replicas);

                    return cluster::are_replica_sets_equal(
                      replica_set, md->replicas);
                });
          })
          .handle_exception([ntp, &replica_set](const std::exception_ptr& e) {
              BOOST_FAIL(fmt::format(
                "Timeout waiting for replica set metadata update, replica set "
                "{} {} ({})",
                ntp,
                replica_set,
                e));
          })
          .get0();
        vlog(
          logger.info,
          "SUCCESS: partition {} metadata are up to date with requested "
          "replica set: {}",
          ntp,
          replica_set);
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
      model::timeout_clock::duration tout,
      model::ntp ntp,
      const std::vector<model::broker_shard>& replicas) {
        print_configuration(ntp).get0();
        wait_for_all_replicas(
          tout,
          replicas,
          [this, ntp, &replicas](const model::broker_shard& bs) mutable {
              auto ptr = ss::make_lw_shared<replicas_t>(replicas);
              auto f_replicas = ss::make_foreign(ptr);
              return get_partition_manager(bs.node_id)
                .invoke_on(
                  bs.shard,
                  [ntp, f_replicas = std::move(f_replicas)](
                    const cluster::partition_manager& pm) mutable {
                      return validate_partiton(ntp, pm, std::move(f_replicas));
                  })
                .finally([ptr] {});
          })
          .handle_exception([&replicas](const std::exception_ptr& e) {
              BOOST_FAIL(fmt::format(
                "Timeout waiting for replica set partitions to be updated {}",
                replicas));
          })
          .get0();

        vlog(
          logger.info,
          "SUCCESS: partition {} are in align with requested "
          "replica set: {}",
          ntp,
          replicas);
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
              .get_leader(ntp, model::timeout_clock::now() + 5s)
              .then([this, ntp](model::node_id leader_id) {
                  auto shard = get_shard_table(leader_id).shard_for(ntp);
                  if (!shard) {
                      return ss::make_ready_future<bool>(false);
                  }
                  auto& pm = get_partition_manager(leader_id);
                  return pm.invoke_on(
                    *shard, [ntp](cluster::partition_manager& pm) {
                        return pm.get(ntp)->is_leader();
                    });
              })
              .handle_exception([](std::exception_ptr) { return false; });
        }).get0();

        auto retries = 10;
        bool stop = false;
        foreign_batches_t ret;
        auto single_retry = [count, ntp](cluster::partition_manager& pm) {
            auto batches = storage::test::make_random_batches(
              model::offset(0), count);
            auto rdr = model::make_memory_record_batch_reader(
              std::move(batches));
            // replicate
            auto f = pm.get(ntp)->replicate(
              std::move(rdr),
              raft::replicate_options(raft::consistency_level::quorum_ack));

            return ss::with_timeout(
                     model::timeout_clock::now() + 2s, std::move(f))
              .then([&pm, ntp](result<raft::replicate_result> res) {
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
                          ss::make_lw_shared<batches_t>(std::move(batches)));
                    });
              });
        };

        while (!ret && retries > 0) {
            try {
                --retries;
                model::node_id leader_id;
                leader_id = get_local_cache(nodes[0])
                              .get_leader(ntp, 1s + model::timeout_clock::now())
                              .get0();

                auto shard = get_shard_table(leader_id).shard_for(ntp);
                auto& pm = get_partition_manager(leader_id);
                ret = pm.invoke_on(*shard, single_retry).get0();
            } catch (...) {
                ss::sleep(1s).get0();
                continue;
            }
        }

        return ret;
    }

    void validate_replicas_recovery(
      model::timeout_clock::duration tout,
      model::ntp ntp,
      const std::vector<model::broker_shard>& replicas,
      const foreign_batches_t& reference_batches) {
        BOOST_REQUIRE(!reference_batches->empty());
        auto max_offset = reference_batches->back().last_offset();
        wait_for_all_replicas(
          tout,
          replicas,
          [this, ntp, &reference_batches, max_offset](model::broker_shard bs) {
              return read_replica_batches(bs, ntp, max_offset)
                .then([this, ntp, &reference_batches, bs](
                        foreign_batches_t batches) {
                    vlog(
                      logger.info,
                      "Comparing {} {} batches {} {}",
                      bs,
                      ntp,
                      reference_batches->size(),
                      batches->size());
                    bool same = are_batches_the_same(
                      reference_batches, batches);
                    if (!same) {
                        for (const auto& i : *reference_batches) {
                            vlog(logger.info, "reference batch {}", i);
                        }
                        for (const auto& i : *batches) {
                            vlog(logger.info, "actual batch {}", i);
                        }
                    }
                    return same;
                });
          })
          .handle_exception([&replicas](const std::exception_ptr&) {
              BOOST_FAIL(fmt::format(
                "Timeout waiting for replica set to be recovered, replicas "
                "{}",
                replicas));
          })
          .get0();
        vlog(
          logger.info,
          "SUCCESS: partition {} log successfully recovered",
          ntp,
          replicas);
    }

    std::vector<model::broker_shard> random_replicas(
      int rf,
      const absl::flat_hash_map<model::node_id, ss::shard_id>&
        current_placement) {
        auto effective_rf = random_generators::get_int(1, rf);
        std::vector<model::broker_shard> replicas;
        replicas.reserve(rf);
        std::vector<model::node_id> available = nodes;
        std::shuffle(
          available.begin(), available.end(), random_generators::internal::gen);
        auto it = available.begin();
        int cnt = 0;

        while (cnt < effective_rf) {
            // either use current shard_id or shard 0
            ss::shard_id shard = random_generators::get_int<ss::shard_id>(
              ss::smp::count - 1);

            replicas.push_back(
              model::broker_shard{.node_id = *it, .shard = shard});
            cnt++;
            it++;
        }
        return replicas;
    }

    void update_replica_shards(
      std::vector<model::broker_shard>& new_replicas,
      const std::vector<model::broker_shard>& current_replicas) {
        for (auto& new_bs : new_replicas) {
            auto it = std::find_if(
              current_replicas.begin(),
              current_replicas.end(),
              [&new_bs](const model::broker_shard& current_bs) {
                  return current_bs.node_id == new_bs.node_id;
              });

            if (it != current_replicas.end()) {
                new_bs.shard = it->shard;
            }
        }
    }

    foreign_batches_t execute_and_validate_history_updates(
      const model::ntp& ntp, history_t& history) {
        auto reference_batches = replicate_data(ntp, 10);
        int cnt = 0;
        auto current = get_replicas(0, ntp);
        std::cout << "history [";
        std::cout << current[0].node_id();
        for (int i = 1; i < current.size(); ++i) {
            std::cout << "," << current[i].node_id();
        }
        std::cout << "]";

        for (const auto& r : history) {
            std::cout << "->[";
            std::cout << r[0].node_id();
            for (int i = 1; i < r.size(); i++) {
                std::cout << "," << r[i].node_id();
            }
            std::cout << "]";
        }
        std::cout << std::endl;
        for (auto& r : history) {
            auto current = get_replicas(0, ntp);
            update_replica_shards(r, current);

            logger.info("update no: {}. started  [{} => {}]", cnt, current, r);
            print_configuration(ntp).get0();
            auto err = std::error_code(cluster::errc::waiting_for_recovery);
            int retry = 0;
            while (err) {
                err = get_leader_controller()
                        ->get_topics_frontend()
                        .local()
                        .move_partition_replicas(
                          ntp, r, model::timeout_clock::now() + 1s)
                        .get0();
                if (err) {
                    logger.info(
                      "update no: {}.  [{} => {}] - failure: {}",
                      cnt,
                      current,
                      r,
                      err);
                    ss::sleep(200ms).get0();
                    retry++;
                }
                if (retry > 100) {
                    BOOST_FAIL("to many retries");
                }
            }

            wait_for_metadata_update(ntp, r);
            wait_for_replica_set_partitions(30s, ntp, r);
            validate_replicas_recovery(30s, ntp, r, reference_batches);
            logger.info(
              "update no: {}. finished  [{} => {}]", cnt++, current, r);
            print_configuration(ntp).get0();
        }
        print_configuration(ntp).get0();

        return reference_batches;
    }

    cluster::controller* controller(int id) { return controllers[id]; }

    ss::future<> print_configuration(const model::ntp& ntp) {
        return ss::do_for_each(nodes, [this, ntp](model::node_id n) {
            return get_partition_manager(n).invoke_on_all(
              [ntp = ntp, n](cluster::partition_manager& pm) {
                  auto p = pm.get(ntp);
                  if (p) {
                      logger.info(
                        "[n: {}] rev: {} o: {}, cfg: {}",
                        n,
                        p->get_revision_id(),
                        p->get_latest_configuration_offset(),
                        p->group_configuration());
                  }
              });
        });
    }

    std::vector<cluster::controller*> controllers;
    // 5 nodes cluster
    std::vector<model::node_id> nodes = {
      model::node_id(0),
      model::node_id(1),
      model::node_id(2),
      model::node_id(3),
      model::node_id(4)};
};

partition_assignment_test_fixture::replicas_t
replica(const std::vector<int>& replicas) {
    std::vector<model::broker_shard> bs;
    bs.reserve(replicas.size());
    for (int r : replicas) {
        bs.push_back(model::broker_shard{
          .node_id = model::node_id(r),
          .shard = random_generators::get_int(ss::smp::count - 1)});
    }

    return bs;
}

partition_assignment_test_fixture::history_t
custom_history(std::vector<std::vector<int>> history) {
    partition_assignment_test_fixture::history_t h;
    h.reserve(history.size());
    for (auto& r : history) {
        h.push_back(replica(r));
    }

    return h;
}
/**
 * This test contains following test cases:
 * legend:
 *  [<id>,...] - denotes replica set nodes
 *
 * - moving replica back and forth between two nodes
 * - random history update
 * - failure recovery test
 * - corner cases tests
 * - test with multiple topics
 */

FIXTURE_TEST(test_moving_back_and_forth, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 1));

    size_t history_size = 10;
    history_t history;
    history.reserve(history_size);
    for (int i = 0; i < history_size; ++i) {
        // migrate between nodes 2 & 3
        history.push_back({model::broker_shard{
          .node_id = model::node_id(i % 2) + 2, .shard = 0}});
    }

    execute_and_validate_history_updates(ntp, history);
}

FIXTURE_TEST(test_moving_to_the_same_node, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 1));

    size_t history_size = 10;
    history_t history;
    history.reserve(history_size);
    for (int i = 0; i < history_size; ++i) {
        history.push_back(
          {model::broker_shard{.node_id = model::node_id(0), .shard = 0}});
    }

    execute_and_validate_history_updates(ntp, history);
}

FIXTURE_TEST(test_moving_cross_cores, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 1));

    size_t history_size = 10;
    history_t history;
    history.reserve(history_size);
    for (int i = 0; i < history_size; ++i) {
        history.push_back({model::broker_shard{
          .node_id = model::node_id(0), .shard = i % ss::smp::count}});
    }

    auto reference_batches = execute_and_validate_history_updates(ntp, history);
    restart_cluster();
    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(corner_cases, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
    auto current = get_replicas(0, ntp);

    auto history = custom_history(
      {{0, 4, 1},
       {0, 4},
       {4},
       {3},
       {3, 2, 4},
       {0, 3},
       {4},
       {2, 1, 4},
       {3},
       {2, 4, 3},
       {3, 2, 1}});

    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
    restart_cluster();
    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(corner_cases_2, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
    auto current = get_replicas(0, ntp);

    auto history = custom_history(
      {{2, 0, 1},
       {0, 2},
       {3, 4, 0},
       {4},
       {3},
       {2, 4},
       {2, 4, 3},
       {0},
       {1},
       {3, 1},
       {0, 4}});

    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
    restart_cluster();
    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(corner_cases_3, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
    auto current = get_replicas(0, ntp);
    auto history = custom_history(
      {{1, 2, 0},
       {2, 4, 3},
       {1, 0},
       {1},
       {0},
       {0, 4},
       {2},
       {3, 1, 0},
       {2, 3, 4},
       {4, 3, 2},
       {3}});

    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
    restart_cluster();
    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
}

/**
 * Create partition with update command in the second partition move, testing
 */
FIXTURE_TEST(corner_cases_4, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
    auto current = get_replicas(0, ntp);
    auto history = custom_history({
      {2, 0, 1},
      {3, 0, 1},
      {3, 0, 4},
      {3, 0, 2},
      {3, 0, 1},
    });

    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
    restart_cluster();
    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(disjoint_qorums, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 1));
    auto current = get_replicas(0, ntp);

    auto history = custom_history(
      {{0}, {1}, {2}, {3}, {4}, {3}, {2}, {1}, {0}});

    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // check after all changes were applied
    wait_for_metadata_update(ntp, history.back());
    // here we have to wait for a long time as the cluster must reach all
    // intermediate states before reaching this point
    wait_for_replica_set_partitions(60s, ntp, history.back());
    validate_replicas_recovery(60s, ntp, history.back(), reference_batches);
    // recovery
    restart_cluster();
    wait_for_metadata_update(ntp, history.back());
    wait_for_replica_set_partitions(20s, ntp, history.back());
    validate_replicas_recovery(20s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(multiple_changes_test, partition_assignment_test_fixture) {
    model::ntp ntp(test_ns, model::topic("t_1"), model::partition_id(0));
    create_topic(test_topics_configuration(
      model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
    auto current = get_replicas(0, ntp);
    size_t history_size = 10;

    absl::flat_hash_map<model::node_id, ss::shard_id> broker_shards;
    broker_shards.reserve(current.size());
    for (auto& bs : current) {
        broker_shards.emplace(bs.node_id, bs.shard);
    }

    history_t history;
    history.reserve(history_size);
    for (int i = 0; i < history_size; ++i) {
        history.push_back(random_replicas(3, broker_shards));
    }
    auto reference_batches = execute_and_validate_history_updates(ntp, history);

    // recovery
    restart_cluster();
    wait_for_metadata_update(ntp, history.back());
    wait_for_replica_set_partitions(120s, ntp, history.back());
    validate_replicas_recovery(20s, ntp, history.back(), reference_batches);
}

FIXTURE_TEST(
  multiple_changes_test_multi_topics, partition_assignment_test_fixture) {
    model::ntp ntp_1(test_ns, model::topic("t_1"), model::partition_id(0));
    model::ntp ntp_2(test_ns, model::topic("t_2"), model::partition_id(0));
    model::ntp ntp_3(test_ns, model::topic("t_3"), model::partition_id(0));
    model::ntp ntp_4(test_ns, model::topic("t_4"), model::partition_id(0));
    model::ntp ntp_5(test_ns, model::topic("t_4"), model::partition_id(1));
    model::ntp ntp_6(test_ns, model::topic("t_4"), model::partition_id(3));

    auto test = [this](model::ntp ntp) {
        return ss::async([this, ntp = std::move(ntp)]() mutable {
            int cnt = 0;

            create_topic(test_topics_configuration(
              model::topic_namespace(ntp.ns, ntp.tp.topic), 3));
            auto current = get_replicas(0, ntp);
            auto reference_batches = replicate_data(ntp, 10);
            size_t history_size = 10;

            absl::flat_hash_map<model::node_id, ss::shard_id> broker_shards;
            broker_shards.reserve(current.size());
            for (auto& bs : current) {
                broker_shards.emplace(bs.node_id, bs.shard);
            }

            history_t history;
            history.reserve(history_size);
            for (int i = 0; i < history_size; ++i) {
                history.push_back(random_replicas(3, broker_shards));
            }

            for (auto& r : history) {
                auto current = get_replicas(0, ntp);
                update_replica_shards(r, current);
                logger.info(
                  "[{}] update no: {}. started  [{} => {}]",
                  ntp,
                  cnt,
                  current,
                  r);
                auto err = std::error_code(raft::errc::not_leader);
                int retry = 0;
                while (err) {
                    err = get_leader_controller()
                            ->get_topics_frontend()
                            .local()
                            .move_partition_replicas(
                              ntp, r, model::timeout_clock::now() + 1s)
                            .get0();
                    if (err) {
                        logger.info(
                          "[{}] update no: {}.  [{} => {}] - failure: {}",
                          ntp,
                          cnt,
                          current,
                          r,
                          err.message());
                        ss::sleep(200ms).get();
                        retry++;
                    }

                    if (retry > 100) {
                        BOOST_FAIL("to many retries");
                    }
                }

                // check topics table

                wait_for_metadata_update(ntp, r);

                // check if replicas are there

                wait_for_replica_set_partitions(60s, ntp, r);

                validate_replicas_recovery(20s, ntp, r, reference_batches);

                print_configuration(ntp).get0();
                logger.info(
                  "[{}] update no: {}. finished  [{} => {}]",
                  ntp,
                  cnt++,
                  current,
                  r);
            }
        });
    };

    std::vector<ss::future<>> all;
    all.push_back(test(ntp_1));
    all.push_back(test(ntp_2));
    all.push_back(test(ntp_3));
    all.push_back(test(ntp_4));

    ss::when_all_succeed(all.begin(), all.end()).get0();
}
