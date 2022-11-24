// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/partition.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"

using batches_t = ss::circular_buffer<model::record_batch>;
using batches_ptr_t = ss::lw_shared_ptr<batches_t>;
using foreign_batches_t = ss::foreign_ptr<batches_ptr_t>;

using namespace std::chrono_literals; // NOLINT

class rebalancing_tests_fixture : public cluster_test_fixture {
public:
    rebalancing_tests_fixture()
      : test_logger("rebalancing-test") {
        set_configuration(
          "partition_autobalancing_mode",
          model::partition_autobalancing_mode::node_add);
    }

    ~rebalancing_tests_fixture() {
        for (auto& [id, _] : apps) {
            remove_node_application(id);
        }
    }

    application* node_application(int id) { return apps[model::node_id(id)]; }

    void start_cluster(int node_count) {
        // start nodes
        for (auto i = 0; i < node_count; ++i) {
            auto nid = model::node_id(i);
            apps.emplace(nid, create_node_application(nid));
        }

        // wait for cluster to be stable
        tests::cooperative_spin_wait_with_timeout(60s, [this, node_count] {
            return std::all_of(
              apps.cbegin(), apps.cend(), [node_count](auto c) {
                  return c.second->controller->get_members_table()
                           .local()
                           .node_count()
                         == node_count;
              });
        }).get0();
    }

    void add_node(int id) {
        auto nid = model::node_id(id);
        apps.emplace(nid, create_node_application(nid));
        set_configuration("disable_metrics", true);
        set_configuration(
          "partition_autobalancing_mode",
          model::partition_autobalancing_mode::node_add);
    }

    cluster::topic_configuration create_topic_cfg(
      ss::sstring topic, int partitions, int replication_factor) {
        model::topic_namespace tp_ns(
          model::kafka_namespace, model::topic(std::move(topic)));

        return cluster::topic_configuration(
          tp_ns.ns, tp_ns.tp, partitions, replication_factor);
    }

    std::optional<application*> get_leader_node_application() {
        auto it = std::find_if(apps.begin(), apps.end(), [](auto& app) {
            auto partition = app.second->partition_manager.local().get(
              model::controller_ntp);
            return partition && partition->is_elected_leader();
        });

        if (it != apps.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    std::vector<model::broker_shard>
    get_replicas(int source_node, const model::ntp& ntp) {
        auto md = node_application(source_node)
                    ->controller->get_topics_state()
                    .local()
                    .get_topic_metadata(model::topic_namespace_view(ntp));

        return md->get_assignments().begin()->replicas;
    }

    void create_topic(cluster::topic_configuration cfg) {
        auto res = (*get_leader_node_application())
                     ->controller->get_topics_frontend()
                     .local()
                     .autocreate_topics({std::move(cfg)}, 2s)
                     .get0();
        wait_for_metadata(
          node_application(0)->controller->get_topics_state().local(), res);
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
            return get_local_cache(model::node_id(0))
              .get_leader(ntp, model::timeout_clock::now() + 5s)
              .then([this, ntp](model::node_id leader_id) {
                  auto shard = get_shard_table(leader_id).shard_for(ntp);
                  if (!shard) {
                      return ss::make_ready_future<bool>(false);
                  }
                  auto& pm = get_partition_manager(leader_id);
                  return pm.invoke_on(
                    *shard, [ntp](cluster::partition_manager& pm) {
                        return pm.get(ntp)->is_elected_leader();
                    });
              })
              .handle_exception([](std::exception_ptr) { return false; });
        }).get0();

        auto retries = 10;
        foreign_batches_t ret;
        auto single_retry = [count, ntp](cluster::partition_manager& pm) {
            auto batches = model::test::make_random_batches(
              model::offset(0), count);
            auto rdr = model::make_memory_record_batch_reader(
              std::move(batches));
            // replicate
            auto f = pm.get(ntp)->raft()->replicate(
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
                leader_id = get_local_cache(model::node_id(0))
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

    void populate_all_topics_with_data() {
        auto md = get_local_cache(model::node_id(0)).all_topics_metadata();
        for (auto& [tp_ns, topic_metadata] : md) {
            for (auto& p : topic_metadata.get_assignments()) {
                model::ntp ntp(tp_ns.ns, tp_ns.tp, p.id);
                replicate_data(ntp, 10);
            }
        }
    }

    ss::future<> wait_for_node_decommissioned(int node) {
        test_logger.info("waiting for node {}", node);
        model::node_id id(node);
        return tests::cooperative_spin_wait_with_timeout(90s, [this, id] {
            auto leader = get_leader_node_application();
            if (!leader) {
                return ss::make_ready_future<bool>(false);
            }
            auto ids
              = (*leader)->controller->get_members_table().local().node_ids();
            test_logger.info("current brokers: {}", ids);
            return ss::make_ready_future<bool>(
              std::find(ids.begin(), ids.end(), id) == ids.end());
        });
    }
    ss::logger test_logger;
    absl::node_hash_map<model::node_id, application*> apps;
};
