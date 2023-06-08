// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/testing/thread_test_case.hh>

static ss::logger logger("balancer_sim");

static constexpr size_t produce_batch_size = 1_MiB;

// Recovery rate magic numbers. Recovery bandwidth is not directly controllable,
// but it is related to the tick length: node bandwidth allows exactly 1 batch
// per tick. For 100MiB/s bandwidth this gives us 5ms ticks.
static constexpr size_t recovery_batch_size = 512_KiB;
static constexpr size_t recovery_throttle_burst = 100_MiB;
static constexpr size_t recovery_throttle_ticks_between_refills = 10;

/// If a value is a sum of a large number of random variables that with some
/// probability add item_size to the value and the result has a mean of `mean`
/// (example: producing to a topic, randomly choosing the partition for each
/// produced batch), the distribution will be approximately gaussian with stddev
/// sqrt(mean number of items). We use this function to calculate partition size
/// jitter.
static size_t add_sqrt_jitter(size_t mean, size_t item_size) {
    std::normal_distribution<> dist(0, sqrt(double(mean) / item_size));
    auto jitter = std::min(
      int(mean), int(item_size * dist(random_generators::internal::gen)));
    return mean + jitter;
}

class partition_balancer_sim_fixture {
public:
    void add_node(model::node_id id, size_t total_size, uint32_t n_cores = 4) {
        vassert(!_nodes.contains(id), "duplicate node id: {}", id);

        model::broker broker(
          id,
          net::unresolved_address{},
          net::unresolved_address{},
          std::nullopt,
          model::broker_properties{
            .cores = n_cores,
            .available_memory_gb = 2,
            .available_disk_gb = uint32_t(total_size / 1_GiB)});

        _workers.members.local().apply(
          model::offset{}, cluster::add_node_cmd(id, broker));
        _workers.allocator.local().register_node(
          std::make_unique<cluster::allocation_node>(
            id,
            n_cores,
            config::mock_binding<uint32_t>(1000),
            config::mock_binding<uint32_t>(0)));

        // add some random initial used space
        size_t initial_used = random_generators::get_int(
          3 * total_size / 100, 5 * total_size / 100);

        _nodes.emplace(
          id, node_state{.id = id, .total = total_size, .used = initial_used});
    }

    const auto& nodes() const { return _nodes; }

    const cluster::allocation_state::underlying_t& allocation_nodes() const {
        return _workers.allocator.local().state().allocation_nodes();
    }

    const cluster::topic_table& topics() const {
        return _workers.table.local();
    }

    void add_topic(
      const ss::sstring& name,
      int partitions,
      int16_t replication_factor,
      size_t mean_partition_size) {
        auto tp_ns = model::topic_namespace(test_ns, model::topic(name));
        auto topic_conf = _workers.make_tp_configuration(
          name, partitions, replication_factor);
        _workers.dispatch_topic_command(
          cluster::create_topic_cmd(tp_ns, topic_conf));
        for (const auto& as : topic_conf.assignments) {
            model::ntp ntp{tp_ns.ns, tp_ns.tp, as.id};
            auto size = add_sqrt_jitter(
              mean_partition_size, produce_batch_size);
            auto partition = ss::make_lw_shared<partition_state>(ntp, size);
            _partitions.emplace(ntp, partition);

            for (const auto& bs : as.replicas) {
                auto& node = _nodes.at(bs.node_id);
                node.replicas[ntp] = replica{
                  .partition = partition, .local_size = size};
                node.used += size;
            }

            elect_leader(ntp);
        }
    }

    void set_decommissioning(model::node_id id) {
        _workers.set_decommissioning(id);
    }

    void add_node_to_rebalance(model::node_id id) {
        _workers.state.local().add_node_to_rebalance(id);
    }

    size_t cur_tick() const { return _cur_tick; }

    void tick() {
        // refill the bandwidth
        for (auto& [id, node] : _nodes) {
            node.ticks_since_refill += 1;
            if (
              node.ticks_since_refill
              >= recovery_throttle_ticks_between_refills) {
                node.ticks_since_refill = 0;

                if (node.bandwidth_left <= recovery_throttle_burst) {
                    node.bandwidth_left
                      += recovery_throttle_ticks_between_refills
                         * recovery_batch_size;
                }
            }
        }

        // gather all active recovery streams
        absl::flat_hash_map<model::node_id, std::vector<recovery_stream>>
          node2pending_rs;
        for (const auto& ntp :
             _workers.table.local().all_updates_in_progress()) {
            auto part = _partitions.at(ntp);
            if (!part->leader) {
                continue;
            }

            auto learners = get_learners(ntp);
            for (const auto& id : learners) {
                node2pending_rs[*part->leader].push_back(
                  recovery_stream{.ntp = ntp, .from = *part->leader, .to = id});
            }
        }

        // perform recovery for lucky streams that have won the bandwidth
        // lottery.
        for (auto& [node_id, recovery_streams] : node2pending_rs) {
            auto& node = _nodes.at(node_id);

            std::shuffle(
              recovery_streams.begin(),
              recovery_streams.end(),
              random_generators::internal::gen);

            for (const auto& rs : recovery_streams) {
                if (node.bandwidth_left >= recovery_batch_size) {
                    perform_recovery_step(rs);
                    node.bandwidth_left -= recovery_batch_size;
                } else {
                    break;
                }
            }
        }

        // finish the updates that are ready
        for (const auto& ntp :
             _workers.table.local().all_updates_in_progress()) {
            maybe_finish_update(ntp);
        }

        _cur_tick += 1;
    }

    void run_balancer() {
        auto hr = create_health_report();
        populate_node_status_table();

        auto planner = make_planner();

        {
            ss::abort_source as;
            auto plan_data = planner.plan_actions(hr, as).get();

            logger.info(
              "planned action counts: reassignments: {}, cancellations: {}, "
              "failed: {}",
              plan_data.reassignments.size(),
              plan_data.cancellations.size(),
              plan_data.failed_actions_count);

            _last_run_in_progress_updates
              = _workers.table.local().updates_in_progress().size()
                + plan_data.reassignments.size()
                + plan_data.cancellations.size();

            for (const auto& reassignment : plan_data.reassignments) {
                dispatch_move(
                  reassignment.ntp, reassignment.allocated.replicas());
            }
            for (const auto& ntp : plan_data.cancellations) {
                dispatch_cancel(ntp);
            }
        }
    }

    size_t last_run_in_progress_updates() const {
        return _last_run_in_progress_updates;
    }

    void print_state() const {
        logger.info(
          "TICK {}: {} nodes, {} partitions, {} updates in progress",
          _cur_tick,
          _nodes.size(),
          _partitions.size(),
          _workers.table.local().updates_in_progress().size());
        for (const auto& [id, node] : nodes()) {
            logger.info(
              "node id: {}, used: {:.4} GiB ({}%), num replicas: {} (final: "
              "{})",
              id,
              double(node.used) / 1_GiB,
              (node.used * 100) / node.total,
              node.replicas.size(),
              allocation_nodes().at(id)->final_partitions());
        }
    }

    void print_replica_map() const {
        for (const auto& t : topics().topics_map()) {
            for (const auto& a : t.second.get_assignments()) {
                auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
                std::vector<model::node_id> replicas;
                for (const auto& bs : a.replicas) {
                    replicas.push_back(bs.node_id);
                }
                std::sort(replicas.begin(), replicas.end());
                logger.info("ntp {}: {}", ntp, replicas);
            }
        }
    }

    void validate_even_replica_distribution() {
        static constexpr double max_skew = 0.01;

        absl::flat_hash_map<model::node_id, size_t> node2replicas;
        size_t total_replicas = 0;
        size_t total_capacity = 0;
        for (auto& [id, n] : allocation_nodes()) {
            node2replicas[id] = n->allocated_partitions();
            total_replicas += n->allocated_partitions();
            total_capacity += n->max_capacity();
        }

        for (auto& [id, replicas] : node2replicas) {
            size_t capacity = allocation_nodes().at(id)->max_capacity();
            auto expected = floor(
              double(total_replicas) * capacity / total_capacity);
            logger.info(
              "node {} has {} replicas, expected: {}", id, replicas, expected);
            BOOST_REQUIRE_GE(replicas, expected - ceil(max_skew * expected));
            BOOST_REQUIRE_LE(replicas, expected + ceil(max_skew * expected));
        }
    }

    bool should_schedule_balancer_run() const {
        auto current_in_progress
          = _workers.table.local().updates_in_progress().size();
        return current_in_progress == 0
               || double(current_in_progress)
                    < 0.8 * double(_last_run_in_progress_updates);
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

    std::vector<model::node_id> get_voters(const model::ntp& ntp) const {
        std::vector<model::node_id> ret;
        for (const auto& [id, node] : _nodes) {
            auto it = node.replicas.find(ntp);
            if (
              it != node.replicas.end()
              && it->second.local_size == it->second.partition->size) {
                ret.push_back(id);
            }
        }
        return ret;
    }

    std::vector<model::node_id> get_learners(const model::ntp& ntp) const {
        std::vector<model::node_id> ret;
        for (const auto& [id, node] : _nodes) {
            auto it = node.replicas.find(ntp);
            if (
              it != node.replicas.end()
              && it->second.local_size < it->second.partition->size) {
                ret.push_back(id);
            }
        }
        return ret;
    }

    void elect_leader(const model::ntp& ntp) {
        auto voters = get_voters(ntp);
        _partitions.at(ntp)->leader = random_generators::random_choice(voters);
    }

    void dispatch_move(
      model::ntp ntp, std::vector<model::broker_shard> new_replicas) {
        _workers.dispatch_topic_command(
          cluster::move_partition_replicas_cmd(ntp, new_replicas));

        auto partition = _partitions.at(ntp);
        for (const auto& bs : new_replicas) {
            auto& node = _nodes.at(bs.node_id);
            node.replicas.emplace(
              ntp, replica{.partition = partition, .local_size = 0});
        }
    }

    void dispatch_cancel(model::ntp ntp) {
        cluster::cancel_moving_partition_replicas_cmd cmd{
          std::move(ntp),
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{false}}};
        _workers.dispatch_topic_command(std::move(cmd));
    }

    struct recovery_stream {
        model::ntp ntp;
        model::node_id from;
        model::node_id to;

        friend bool operator==(const recovery_stream&, const recovery_stream&)
          = default;
    };

    void perform_recovery_step(const recovery_stream& rs) {
        auto& dest_node = _nodes.at(rs.to);
        auto& dest_replica = dest_node.replicas.at(rs.ntp);
        const auto& partition = *dest_replica.partition;
        auto step = std::min(
          recovery_batch_size, partition.size - dest_replica.local_size);
        dest_replica.local_size += step;
        dest_node.used += step;
    }

    bool maybe_finish_update(const model::ntp& ntp) {
        const auto& partition = *_partitions.at(ntp);
        if (!partition.leader) {
            // can't finish anything for a leaderless partition
            return false;
        }

        const auto& cur_update
          = _workers.table.local().updates_in_progress().at(ntp);

        bool all_replicas_recovered = true;
        for (const auto& bs : cur_update.get_target_replicas()) {
            const auto& node = _nodes.at(bs.node_id);
            if (node.replicas.at(ntp).local_size < partition.size) {
                // some nodes are still learners
                all_replicas_recovered = false;
                break;
            }
        }

        // dispatch the finish command

        switch (cur_update.get_state()) {
        case cluster::reconfiguration_state::in_progress:
            if (!all_replicas_recovered) {
                return false;
            }

            _workers.dispatch_topic_command(
              cluster::finish_moving_partition_replicas_cmd{
                ntp, cur_update.get_target_replicas()});
            break;
        case cluster::reconfiguration_state::cancelled:
            if (all_replicas_recovered) {
                _workers.dispatch_topic_command(
                  cluster::revert_cancel_partition_move_cmd{
                    0,
                    cluster::revert_cancel_partition_move_cmd_data{
                      .ntp = ntp}});
            } else {
                _workers.dispatch_topic_command(
                  cluster::finish_moving_partition_replicas_cmd{
                    ntp, cur_update.get_previous_replicas()});
            }
            break;
        default:
            // other states can't appear because they can only be created
            // manually, not by the balancer.
            BOOST_REQUIRE(false);
        }

        // remove excess replicas

        auto cur_assignment = _workers.table.local().get_partition_assignment(
          ntp);
        BOOST_REQUIRE(cur_assignment);

        absl::flat_hash_set<model::node_id> cur_replicas;
        for (const auto& bs : cur_assignment->replicas) {
            cur_replicas.insert(bs.node_id);
        }

        for (auto& [id, node] : _nodes) {
            if (!cur_replicas.contains(id)) {
                auto it = node.replicas.find(ntp);
                if (it != node.replicas.end()) {
                    node.used -= it->second.local_size;
                    node.replicas.erase(it);
                }
            }
        }

        // for the case when the old leader is not in the new replica set.
        elect_leader(ntp);

        return true;
    }

    struct partition_state {
        partition_state(model::ntp ntp, size_t size)
          : ntp(std::move(ntp))
          , size(size) {}

        model::ntp ntp;
        size_t size = 0;
        std::optional<model::node_id> leader;

        using ptr_t = ss::lw_shared_ptr<partition_state>;
    };

    struct replica {
        partition_state::ptr_t partition;
        size_t local_size = 0;
    };

    struct node_state {
        model::node_id id;
        size_t total = 0;
        size_t used = 0;
        absl::flat_hash_map<model::ntp, replica> replicas;
        size_t bandwidth_left = recovery_throttle_burst;
        size_t ticks_since_refill = 0;

        cluster::node_health_report get_health_report() const {
            cluster::node_health_report report;
            storage::disk node_disk{.free = total - used, .total = total};
            report.id = id;
            report.local_state.set_disk(node_disk);

            absl::flat_hash_map<
              model::topic_namespace,
              ss::chunked_fifo<cluster::partition_status>>
              topic2partitions;
            for (const auto& [ntp, repl] : replicas) {
                topic2partitions[model::topic_namespace(ntp.ns, ntp.tp.topic)]
                  .push_back(cluster::partition_status{
                    .id = ntp.tp.partition, .size_bytes = repl.local_size});
            }

            for (auto& [topic, partitions] : topic2partitions) {
                report.topics.push_back(
                  cluster::topic_status(topic, std::move(partitions)));
            }

            return report;
        }
    };

    absl::btree_map<model::node_id, node_state> _nodes;
    absl::flat_hash_map<model::ntp, partition_state::ptr_t> _partitions;
    size_t _cur_tick = 0;
    size_t _last_run_in_progress_updates = 0;
    controller_workers _workers;
};

FIXTURE_TEST(test_decommission, partition_balancer_sim_fixture) {
    for (size_t i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 100, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    print_state();
    for (size_t i = 0; i < 10000; ++i) {
        if (nodes().at(model::node_id{0}).replicas.empty()) {
            logger.info("finished in {} ticks", i);
            break;
        }

        tick();
        if (should_schedule_balancer_run()) {
            print_state();
            run_balancer();
        }
    }

    logger.info("finished after {} ticks", cur_tick());
    print_state();

    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_two_decommissions, partition_balancer_sim_fixture) {
    for (size_t i = 0; i < 5; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 200, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    size_t start_node_0_replicas
      = nodes().at(model::node_id{0}).replicas.size();

    print_state();
    for (size_t i = 0; i < 20000; ++i) {
        if (
          nodes().at(model::node_id{0}).replicas.empty()
          && nodes().at(model::node_id{1}).replicas.empty()) {
            break;
        }

        if (
          !allocation_nodes().at(model::node_id{1})->is_decommissioned()
          && allocation_nodes().at(model::node_id{0})->final_partitions()()
               < start_node_0_replicas / 2) {
            logger.info(
              "start decommissioning node 1 after {} ticks", cur_tick());
            set_decommissioning(model::node_id{1});
            print_state();
        }

        tick();
        if (should_schedule_balancer_run()) {
            print_state();
            run_balancer();
        }
    }

    logger.info("finished after {} ticks", cur_tick());
    print_state();

    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{1})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_counts_rebalancing, partition_balancer_sim_fixture) {
    for (size_t i = 0; i < 3; ++i) {
        add_node(model::node_id{i}, 100_GiB, 4);
    }
    add_topic("mytopic", 200, 3, 100_MiB);
    add_node(model::node_id{3}, 100_GiB, 4);
    add_node_to_rebalance(model::node_id{3});
    add_node(model::node_id{4}, 100_GiB, 8);
    add_node_to_rebalance(model::node_id{4});

    print_state();

    for (size_t i = 0; i < 30000; ++i) {
        tick();
        if (should_schedule_balancer_run()) {
            print_state();
            run_balancer();

            if (last_run_in_progress_updates() == 0) {
                break;
            }
        }
    }

    validate_even_replica_distribution();
}
