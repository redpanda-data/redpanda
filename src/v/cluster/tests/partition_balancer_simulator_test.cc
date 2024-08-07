// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cluster/health_monitor_types.h"
#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/testing/thread_test_case.hh>

#include <cstddef>

static ss::logger logger("balancer_sim");

static constexpr size_t produce_batch_size = 1_MiB;

// Recovery rate magic numbers. Recovery bandwidth is not directly controllable,
// but it is related to the tick length: node bandwidth allows exactly 1 batch
// per tick. For 100MiB/s bandwidth this gives us 5ms ticks.
static constexpr size_t recovery_batch_size = 512_KiB;
static constexpr size_t recovery_throttle_burst = 100_MiB;
static constexpr size_t recovery_throttle_ticks_between_refills = 10;

class partition_balancer_sim_fixture {
public:
    void add_node(
      model::node_id id,
      size_t total_size,
      uint32_t n_cores = 4,
      std::optional<model::rack_id> rack = std::nullopt) {
        vassert(!_nodes.contains(id), "duplicate node id: {}", id);

        model::broker broker(
          id,
          net::unresolved_address{},
          net::unresolved_address{},
          rack,
          model::broker_properties{
            .cores = n_cores,
            .available_memory_gb = 2,
            .available_disk_gb = uint32_t(total_size / 1_GiB),
            .available_memory_bytes = 2 * 1_GiB});

        BOOST_REQUIRE(!_workers.members.local().apply(
          model::offset{}, cluster::add_node_cmd(id, broker)));
        _workers.allocator.local().register_node(
          std::make_unique<cluster::allocation_node>(
            id,
            n_cores,
            config::mock_binding<uint32_t>(1000),
            config::mock_binding<uint32_t>(0),
            config::mock_binding<std::vector<ss::sstring>>({})));

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
      size_t mean_partition_size,
      std::optional<double> stddev = std::nullopt) {
        auto tp_ns = model::topic_namespace(test_ns, model::topic(name));
        auto topic_conf = _workers.make_tp_configuration(
          name, partitions, replication_factor);
        _workers.dispatch_topic_command(
          cluster::create_topic_cmd(tp_ns, topic_conf));

        if (!stddev) {
            /// If a value is a sum of a large number of random variables that
            /// with some probability add item_size to the value and the result
            /// has a mean of `mean` (example: producing to a topic, randomly
            /// choosing the partition for each produced batch), the
            /// distribution will be approximately gaussian with stddev
            /// sqrt(mean number of items).
            stddev = produce_batch_size
                     * sqrt(double(mean_partition_size) / produce_batch_size);
        }

        std::normal_distribution<> dist(0, 1);
        for (const auto& as : topic_conf.assignments) {
            model::ntp ntp{tp_ns.ns, tp_ns.tp, as.id};
            auto jitter = std::max(
              -int64_t(mean_partition_size),
              int64_t(*stddev * dist(random_generators::internal::gen)));
            auto size = mean_partition_size + jitter;
            auto partition = ss::make_lw_shared<partition_state>(ntp, size);
            _partitions.emplace(ntp, partition);

            for (const auto& bs : as.replicas) {
                auto& node = _nodes.at(bs.node_id);
                node.replicas[ntp] = replica{
                  .partition = partition, .local_size = size};
                node.used += size;
            }

            elect_leader(ntp);

            logger.info(
              "added ntp {}, replicas {}, size {}",
              ntp,
              as.replicas,
              human::bytes(size));
        }
        _total_replicas += static_cast<size_t>(replication_factor) * partitions;
    }

    void set_decommissioning(model::node_id id) {
        _workers.set_decommissioning(id);
    }

    void add_node_to_rebalance(model::node_id id) {
        _workers.state.local().add_node_to_rebalance(id);
    }

    size_t cur_tick() const { return _cur_tick; }

    // returns true if all partition movements stopped
    bool run_to_completion(
      size_t max_balancer_actions, std::function<void()> tick_cb = [] {}) {
        print_state();

        size_t num_actions = 0;
        while (num_actions < max_balancer_actions) {
            tick();
            tick_cb();

            if (should_schedule_balancer_run()) {
                num_actions += run_balancer();
                print_state();

                if (last_run_in_progress_updates() == 0) {
                    logger.info("finished after {} ticks", cur_tick());
                    return true;
                }
            }
        }

        print_state();
        return false;
    }

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

    // return the number of scheduled actions
    size_t run_balancer() {
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

            size_t actions_count = plan_data.reassignments.size()
                                   + plan_data.cancellations.size();

            _last_run_in_progress_updates
              = _workers.table.local().updates_in_progress().size()
                + actions_count;

            for (const auto& reassignment : plan_data.reassignments) {
                dispatch_move(
                  reassignment.ntp, reassignment.allocated.replicas());
            }
            for (const auto& ntp : plan_data.cancellations) {
                dispatch_cancel(ntp);
            }

            return actions_count;
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
            for (const auto& [_, a] : t.second.get_assignments()) {
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
            auto expected_min = expected - ceil(max_skew * expected);
            auto expected_max = expected + ceil(max_skew * expected);
            if (replicas < expected_min || replicas > expected_max) {
                print_replica_map();
                BOOST_REQUIRE_MESSAGE(
                  false,
                  "node " << id << ": unexpected replicas count: " << replicas
                          << "(expected interval: [" << expected_min << ", "
                          << expected_max << "]");
            }
        }
    }

    void validate_even_topic_distribution() {
        size_t total_capacity = 0;
        for (auto& [id, n] : allocation_nodes()) {
            total_capacity += n->max_capacity();
        }

        absl::node_hash_map<
          model::topic_namespace,
          absl::flat_hash_map<model::node_id, size_t>>
          topic_replica_distribution;

        absl::node_hash_map<model::topic_namespace, size_t>
          total_topic_replicas;

        for (auto& [tp_ns, topic_md] :
             _workers.table.local().all_topics_metadata()) {
            for (auto& [_, p_as] : topic_md.get_assignments()) {
                total_topic_replicas[tp_ns] += p_as.replicas.size();
                for (auto& r : p_as.replicas) {
                    topic_replica_distribution[tp_ns][r.node_id]++;
                }
            }
        }

        for (auto& [tp, node_replicas] : topic_replica_distribution) {
            if (total_topic_replicas[tp] < nodes().size()) {
                continue;
            }

            double total_replicas = static_cast<double>(
              total_topic_replicas[tp]);
            for (auto& [id, alloc_node] : allocation_nodes()) {
                auto it = node_replicas.find(id);
                const auto replicas_on_node = it == node_replicas.end()
                                                ? 0
                                                : it->second;

                auto expected = ceil(
                  total_replicas * alloc_node->max_capacity() / total_capacity);

                logger.info(
                  "topic {} has {} replicas on {}, expected: {}, "
                  "total replicas: {}",
                  tp,
                  replicas_on_node,
                  id,
                  expected,
                  total_replicas);

                static constexpr double max_skew = 0.03;
                auto expected_min = expected - ceil(max_skew * expected);
                auto expected_max = expected + ceil(max_skew * expected);
                BOOST_CHECK_MESSAGE(
                  replicas_on_node >= expected_min
                    && replicas_on_node <= expected_max,
                  "topic " << tp.tp() << ": unexpected replicas count on node "
                           << id);
            }
        }
    }

    /// Validate that all possible replica pairings are represented
    /// approximately equally in topic partition assignments.
    void validate_topic_replica_pair_frequencies(const ss::sstring& topic) {
        do_validate_replica_pair_frequencies(topic);
    }

    /// Validate that all possible replica pairings are represented
    /// approximately equally in overall partition assignments, as well as for
    /// each topic assignments.
    void validate_replica_pair_frequencies() {
        for (const auto& [tp_ns, topic_md] :
             _workers.table.local().all_topics_metadata()) {
            do_validate_replica_pair_frequencies(tp_ns.tp());
        }
        do_validate_replica_pair_frequencies(std::nullopt);
    }

    bool should_schedule_balancer_run() const {
        auto current_in_progress
          = _workers.table.local().updates_in_progress().size();
        return current_in_progress == 0
               || double(current_in_progress)
                    < 0.8 * double(_last_run_in_progress_updates);
    }

    size_t total_replicas() const { return _total_replicas; }

private:
    void
    do_validate_replica_pair_frequencies(std::optional<ss::sstring> topic) {
        absl::flat_hash_map<
          model::node_id,
          absl::flat_hash_map<model::node_id, size_t>>
          pair_freqs;
        size_t total_pairs = 0;
        for (const auto& [tp_ns, topic_md] :
             _workers.table.local().all_topics_metadata()) {
            if (topic && tp_ns.tp() != topic) {
                continue;
            }

            for (const auto& [_, p_as] : topic_md.get_assignments()) {
                for (const auto& r1 : p_as.replicas) {
                    for (const auto& r2 : p_as.replicas) {
                        if (r1.node_id != r2.node_id) {
                            pair_freqs[r1.node_id][r2.node_id] += 1;
                            total_pairs += 1;
                        }
                    }
                }
            }
        }

        size_t node_count = allocation_nodes().size();
        double expected_freq = double(total_pairs)
                               / (node_count * (node_count - 1));

        // Generous boundaries to allow for fluctuations. But they will catch
        // pathological cases.
        double expected_min = expected_freq - sqrt(expected_freq) * 3;
        double expected_max = expected_freq + sqrt(expected_freq) * 3;

        logger.info(
          "validating replica pair frequencies, topic filter: {}, "
          "expected: {:.4} (interval: [{:.4}, {:.4}])",
          topic,
          expected_freq,
          expected_min,
          expected_max);
        std::optional<std::pair<model::node_id, model::node_id>> offending_pair;
        for (const auto& [id1, _] : allocation_nodes()) {
            for (const auto& [id2, _] : allocation_nodes()) {
                if (id1 == id2) {
                    continue;
                }
                size_t freq = pair_freqs[id1][id2];
                logger.info("node pair {} - {} frequency: {}", id1, id2, freq);
                if (freq < expected_min || freq > expected_max) {
                    offending_pair = {id1, id2};
                }
            }
        }
        BOOST_REQUIRE_MESSAGE(
          !offending_pair,
          "validation failed, offending pair: " << offending_pair->first << ", "
                                                << offending_pair->second);
    }

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
            .max_concurrent_actions = 50,
            .node_availability_timeout_sec = std::chrono::minutes(1),
            .segment_fallocation_step = 16_MiB,
            .node_responsiveness_timeout = std::chrono::seconds(10),
            .topic_aware = true,
          },
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

        cluster::node_health_report_ptr get_health_report() const {
            cluster::node::local_state local_state;
            storage::disk node_disk{.free = total - used, .total = total};
            local_state.set_disk(node_disk);
            local_state.log_data_size = {
              .data_target_size = total,
              .data_current_size = used,
              .data_reclaimable_size = 0};

            absl::flat_hash_map<
              model::topic_namespace,
              cluster::partition_statuses_t>
              topic2partitions;
            for (const auto& [ntp, repl] : replicas) {
                topic2partitions[model::topic_namespace(ntp.ns, ntp.tp.topic)]
                  .push_back(cluster::partition_status{
                    .id = ntp.tp.partition, .size_bytes = repl.local_size});
            }

            chunked_vector<cluster::topic_status> topics;
            for (auto& [topic, partitions] : topic2partitions) {
                topics.push_back(
                  cluster::topic_status(topic, std::move(partitions)));
            }

            return ss::make_foreign(
              ss::make_lw_shared<const cluster::node_health_report>(
                id, local_state, std::move(topics), std::nullopt));
        }
    };

    absl::btree_map<model::node_id, node_state> _nodes;
    absl::flat_hash_map<model::ntp, partition_state::ptr_t> _partitions;
    size_t _cur_tick = 0;
    size_t _last_run_in_progress_updates = 0;
    controller_workers _workers;
    size_t _total_replicas = 0;
};

FIXTURE_TEST(test_decommission, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 100, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    BOOST_REQUIRE(run_to_completion(100));
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_two_decommissions, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 5; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 200, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    size_t start_node_0_replicas
      = nodes().at(model::node_id{0}).replicas.size();
    auto tick_cb = [&] {
        if (
          !allocation_nodes().at(model::node_id{1})->is_decommissioned()
          && allocation_nodes().at(model::node_id{0})->final_partitions()()
               < start_node_0_replicas / 2) {
            logger.info(
              "start decommissioning node 1 after {} ticks", cur_tick());
            set_decommissioning(model::node_id{1});
            print_state();
        }
    };

    BOOST_REQUIRE(run_to_completion(500, tick_cb));
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{1})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_counts_rebalancing, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(model::node_id{i}, 1000_GiB, 4);
    }

    for (int i = 0; i < 10; ++i) {
        add_topic(
          ssx::sformat("topic_{}", i),
          random_generators::get_int(20, 100),
          3,
          100_MiB);
    }

    add_node(model::node_id{3}, 1000_GiB, 4);
    add_node_to_rebalance(model::node_id{3});
    add_node(model::node_id{4}, 1000_GiB, 8);
    add_node_to_rebalance(model::node_id{4});

    BOOST_REQUIRE(run_to_completion(2000));
    validate_even_replica_distribution();
    validate_even_topic_distribution();
}

FIXTURE_TEST(
  test_heterogeneous_racks_full_disk, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(
          model::node_id{i},
          1000_GiB,
          4,
          model::rack_id{ssx::sformat("rack_{}", i)});
    }
    add_node(model::node_id{3}, 1000_GiB, 4, model::rack_id{"rack_0"});

    add_topic("topic_1", 50, 3, 18_GiB);

    // Nodes 0 and 3 are in rack_0 and 1 and 2 are each in racks of their own.
    // We expect 1 and 2 to go over 80% disk limit and the balancer to fix this
    // (even though some rack constraint violations are introduced).

    BOOST_REQUIRE(run_to_completion(100));
    for (const auto& [id, node] : nodes()) {
        BOOST_REQUIRE(double(node.used) / node.total < 0.8);
    }
}

FIXTURE_TEST(test_smol, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }

    add_topic("topic_1", 3, 3, 1_GiB, 100_MiB);
    add_topic("topic_2", 3, 3, 1_GiB, 100_MiB);

    for (model::node_id::type i = 4; i < 6; ++i) {
        add_node(model::node_id{i}, 100_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(10));
}

FIXTURE_TEST(test_heterogeneous_topics, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 9; ++i) {
        add_node(model::node_id{i}, 300_GiB);
    }

    // Add 2 topics with drastically different partition sizes.
    // We expect the result to be nevertheless balanced thanks to topic-aware
    // balancing.
    add_topic("topic_1", 200, 3, 2_GiB, 200_MiB);
    add_topic("topic_2", 800, 3, 10_MiB, 1_MiB);

    for (model::node_id::type i = 9; i < 12; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));

    validate_even_topic_distribution();
    validate_even_replica_distribution();
}

FIXTURE_TEST(test_many_topics, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }

    for (size_t i = 0; i < 100; ++i) {
        // Many topics, each with just a few partitions - this is the hard case
        // for topic-aware balancing. We expect overall replica distribution to
        // be even anyway.

        auto n_partitions = random_generators::get_int(2, 5);

        // take mean topic partition sizes from a bimodal distribution - 20% of
        // the topics will be big.
        auto partition_size = random_generators::get_int(0, 4) == 0 ? 1_GiB
                                                                    : 10_MiB;

        add_topic(
          ssx::sformat("topic_{}", i),
          n_partitions,
          3,
          partition_size,
          partition_size / 10);
    }

    for (model::node_id::type i = 4; i < 6; ++i) {
        add_node(model::node_id{i}, 100_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));

    validate_even_replica_distribution();
}

FIXTURE_TEST(test_replica_pair_frequency, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(model::node_id{i}, 300_GiB);
    }
    add_topic("topic_1", 150, 3, 1_GiB);

    for (model::node_id::type i = 3; i < 6; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    add_topic("topic_2", 150, 3, 1_GiB);
    logger.info("topic_2 created");
    validate_topic_replica_pair_frequencies("topic_2");

    BOOST_REQUIRE(run_to_completion(1000));
    logger.info("first rebalance finished");
    validate_even_replica_distribution();
    validate_replica_pair_frequencies();

    for (model::node_id::type i = 6; i < 9; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));
    logger.info("second rebalance finished");
    validate_even_replica_distribution();
    validate_replica_pair_frequencies();
}

FIXTURE_TEST(test_mixed_replication_factors, partition_balancer_sim_fixture) {
    add_node(model::node_id{0}, 100_GiB, 1);
    add_node(model::node_id{1}, 400_GiB, 1);
    add_node(model::node_id{2}, 100_GiB, 1);
    /**
     * Corner case from real deployment with the following configuration of
     * topics:
     * - partitions:  1, rf: 3, topic_count:  96
     * - partitions:  5, rf: 1, topic_count: 118
     * - partitions: 16, rf: 3, topic_count:   1
     * - partitions:  5, rf: 3, topic_count:  36
     * - partitions: 25, rf: 3, topic_count:  16
     * - partitions:  3, rf: 3, topic_count:   2
     * - partitions:  1, rf: 1, topic_count:  37
     * - partitions: 10, rf: 1, topic_count:   1
     */
    for (int i = 0; i < 96; ++i) {
        add_topic(fmt::format("topic_1_3_{}", i), 1, 3, 10_MiB);
    }
    for (int i = 0; i < 118; ++i) {
        add_topic(fmt::format("topic_5_1_{}", i), 5, 1, 10_MiB);
    }

    add_topic("topic_10_1", 10, 1, 10_MiB);
    for (int i = 0; i < 36; ++i) {
        add_topic(fmt::format("topic_5_3_{}", i), 5, 3, 10_MiB);
    }
    for (int i = 0; i < 16; ++i) {
        add_topic(fmt::format("topic_25_1_{}", i), 25, 3, 10_MiB);
    }
    for (int i = 0; i < 2; ++i) {
        add_topic(fmt::format("topic_3_3_{}", i), 3, 3, 10_MiB);
    }

    for (int i = 0; i < 37; ++i) {
        add_topic(fmt::format("topic_1_1_{}", i), 3, 3, 10_MiB);
    }

    add_node(model::node_id{4}, 300_GiB, 1);
    add_node_to_rebalance(model::node_id{4});
    BOOST_REQUIRE(run_to_completion(total_replicas() * 3));
    set_decommissioning(model::node_id{4});
    BOOST_REQUIRE(run_to_completion(total_replicas() * 3));
}
