// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "vlog.h"

#include <seastar/testing/thread_test_case.hh>

static ss::logger logger("balancer_sim");

class partition_balancer_sim_fixture {
public:
    void add_node(model::node_id id, size_t total_size) {
        vassert(!_nodes.contains(id), "duplicate node id: {}", id);

        model::broker broker(
          id,
          net::unresolved_address{},
          net::unresolved_address{},
          std::nullopt,
          model::broker_properties{
            .cores = 4,
            .available_memory_gb = 2,
            .available_disk_gb = uint32_t(total_size / 1_GiB)});

        _workers.members.local().apply(
          model::offset{}, cluster::add_node_cmd(id, broker));
        _workers.allocator.local().register_node(create_allocation_node(id, 4));

        // TODO: add some random used space
        _nodes.emplace(
          id, node_state{.id = id, .total = total_size, .used = 0});
    }

    const auto& nodes() const { return _nodes; }

    void add_topic(
      const ss::sstring& name,
      int partitions,
      int16_t replication_factor,
      size_t size) {
        auto tp_ns = model::topic_namespace(test_ns, model::topic(name));
        auto topic_conf = _workers.make_tp_configuration(
          name, partitions, replication_factor);
        _workers.dispatch_topic_command(
          cluster::create_topic_cmd(tp_ns, topic_conf));
        for (const auto& as : topic_conf.assignments) {
            model::ntp ntp{tp_ns.ns, tp_ns.tp, as.id};
            for (const auto& bs : as.replicas) {
                auto& node = _nodes.at(bs.node_id);
                node.replica_sizes[ntp] = size; // TODO: add size jitter
                node.used += size;
            }
        }
    }

    void set_decommissioning(model::node_id id) {
        _workers.set_decommissioning(id);
    }

    void tick() {
        auto hr = create_health_report();
        populate_node_status_table();

        auto planner = make_planner();

        {
            ss::abort_source as;
            auto plan_data = planner.plan_actions(hr, as).get();

            logger.info(
              "tick, action counts: reassignments: {}, cancellations: {}, "
              "failed: {}",
              plan_data.reassignments.size(),
              plan_data.cancellations.size(),
              plan_data.failed_actions_count);

            for (const auto& reassignment : plan_data.reassignments) {
                dispatch_move(
                  reassignment.ntp, reassignment.allocated.replicas());
            }
            for (const auto& ntp : plan_data.cancellations) {
                dispatch_cancel(ntp);
            }
        }

        // TODO: more realistic movement
        for (const auto& ntp :
             _workers.table.local().all_updates_in_progress()) {
            finish_update(ntp);
        }
    }

    void print_state() const {
        for (const auto& [id, node] : nodes()) {
            logger.info(
              "node id: {}, used: {:.4} GiB ({}%), num replicas: {}",
              id,
              double(node.used) / 1_GiB,
              (node.used * 100) / node.total,
              node.replica_sizes.size());
        }
    }

    void print_allocator() const {
        const auto& state = _workers.allocator.local().state();
        for (const auto& [id, node] : state.allocation_nodes()) {
            logger.info(
              "alloc node id: {}, allocated: {}, target: {}",
              id,
              node->allocated_partitions(),
              node->final_partitions());
        }
    }

private:
    cluster::cluster_health_report create_health_report() const {
        cluster::cluster_health_report report;
        for (const auto& [id, state] : _nodes) {
            report.node_reports.push_back(state.get_health_report());
        }
        return report;
    }

    void populate_node_status_table() {
        std::vector<cluster::node_status> status_updates;
        for (const auto& [id, state] : _nodes) {
            auto last_seen = raft::clock_type::now();
            // TODO: add ability to add unavailable nodes
            status_updates.push_back(cluster::node_status{
              .node_id = id,
              .last_seen = last_seen,
            });
        }

        _workers.node_status_table
          .invoke_on_all([status_updates](cluster::node_status_table& nts) {
              nts.update_peers(status_updates);
          })
          .get();
    }

    cluster::partition_balancer_planner make_planner(
      model::partition_autobalancing_mode mode
      = model::partition_autobalancing_mode::continuous) {
        return cluster::partition_balancer_planner(
          cluster::planner_config{
            .mode = mode,
            .soft_max_disk_usage_ratio = 0.8,
            .hard_max_disk_usage_ratio = 0.95,
            .movement_disk_size_batch = 5_GiB,
            .max_concurrent_actions = max_concurrent_actions,
            .node_availability_timeout_sec = std::chrono::minutes(1),
            .segment_fallocation_step = 16_MiB,
            .node_responsiveness_timeout = std::chrono::seconds(10)},
          _workers.state.local(),
          _workers.allocator.local());
    }

    void dispatch_move(
      model::ntp ntp, std::vector<model::broker_shard> new_replicas) {
        _workers.dispatch_topic_command(cluster::move_partition_replicas_cmd(
          std::move(ntp), std::move(new_replicas)));
    }

    void dispatch_cancel(model::ntp ntp) {
        cluster::cancel_moving_partition_replicas_cmd cmd{
          std::move(ntp),
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{false}}};
        _workers.dispatch_topic_command(std::move(cmd));
    }

    void finish_update(model::ntp ntp) {
        auto cur_assignment = _workers.table.local().get_partition_assignment(
          ntp);
        BOOST_REQUIRE(cur_assignment);

        cluster::finish_moving_partition_replicas_cmd cmd{
          ntp, cur_assignment->replicas};

        _workers.dispatch_topic_command(std::move(cmd));

        size_t size = max_partition_size(ntp);

        absl::flat_hash_set<model::node_id> cur_replicas;
        for (const auto& bs : cur_assignment->replicas) {
            cur_replicas.insert(bs.node_id);
        }
        for (auto& [id, node] : _nodes) {
            if (cur_replicas.contains(id)) {
                size_t old = std::exchange(node.replica_sizes[ntp], size);
                node.used += size - old;
            } else {
                auto it = node.replica_sizes.find(ntp);
                if (it != node.replica_sizes.end()) {
                    node.used -= it->second;
                    node.replica_sizes.erase(it);
                }
            }
        }
    }

    size_t max_partition_size(const model::ntp& ntp) const {
        size_t size = 0;
        for (const auto& [id, node] : _nodes) {
            auto it = node.replica_sizes.find(ntp);
            if (it != node.replica_sizes.end()) {
                size = std::max(size, it->second);
            }
        }
        return size;
    }

    struct node_state {
        model::node_id id;
        size_t total = 0;
        size_t used = 0;
        absl::flat_hash_map<model::ntp, size_t> replica_sizes;

        cluster::node_health_report get_health_report() const {
            cluster::node_health_report report;
            storage::disk node_disk{.free = total - used, .total = total};
            report.id = id;
            report.local_state.set_disk(node_disk);

            absl::flat_hash_map<
              model::topic_namespace,
              ss::chunked_fifo<cluster::partition_status>>
              topic2partitions;
            for (const auto& [ntp, size] : replica_sizes) {
                topic2partitions[model::topic_namespace(ntp.ns, ntp.tp.topic)]
                  .push_back(cluster::partition_status{
                    .id = ntp.tp.partition, .size_bytes = size});
            }

            for (auto& [topic, partitions] : topic2partitions) {
                report.topics.push_back(
                  cluster::topic_status(topic, std::move(partitions)));
            }

            return report;
        }
    };

    absl::btree_map<model::node_id, node_state> _nodes;
    controller_workers _workers;
};

FIXTURE_TEST(test_decommission, partition_balancer_sim_fixture) {
    for (size_t i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 500, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    print_state();
    for (size_t i = 0; i < 1000; ++i) {
        if (nodes().at(model::node_id{0}).replica_sizes.empty()) {
            break;
        }
        tick();
        print_state();
    }

    BOOST_REQUIRE_EQUAL(nodes().at(model::node_id{0}).replica_sizes.size(), 0);
}
