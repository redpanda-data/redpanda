#pragma once

#include "cluster/tests/utils.h"
#include "cluster/topic_table.h"
#include "test_utils/fixture.h"
#include "units.h"

static std::unique_ptr<cluster::allocation_node>
create_allocation_node(model::node_id nid, uint32_t cores) {
    return std::make_unique<cluster::allocation_node>(
      nid, cores, std::unordered_map<ss::sstring, ss::sstring>{});
}

static void validate_delta(
  const cluster::topic_table::delta& d,
  int new_topics,
  int new_partitions,
  int removed_topics,
  int removed_partitions) {
    BOOST_REQUIRE_EQUAL(d.partitions.additions.size(), new_partitions);
    BOOST_REQUIRE_EQUAL(d.partitions.deletions.size(), removed_partitions);
    BOOST_REQUIRE_EQUAL(d.topics.additions.size(), new_topics);
    BOOST_REQUIRE_EQUAL(d.topics.deletions.size(), removed_topics);
}

struct topic_table_fixture {
    topic_table_fixture() {
        table.start().get0();
        allocator.start_single(raft::group_id(0)).get0();
        allocator.local().register_node(
          create_allocation_node(model::node_id(1), 8));
        allocator.local().register_node(
          create_allocation_node(model::node_id(2), 12));
        allocator.local().register_node(
          create_allocation_node(model::node_id(3), 4));
    }

    ~topic_table_fixture() {
        table.stop().get0();
        allocator.stop().get0();
        as.request_abort();
    }

    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        auto pas = allocator.local().allocate(cfg).value().get_assignments();

        return cluster::topic_configuration_assignment(cfg, std::move(pas));
    }

    cluster::create_topic_cmd make_create_topic_cmd(
      const ss::sstring& name, int partitions, int replication_factor) {
        return cluster::create_topic_cmd(
          make_tp_ns(name),
          make_tp_configuration(name, partitions, replication_factor));
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return model::topic_namespace(test_ns, model::topic(tp));
    }

    void create_topics() {
        auto cmd_1 = make_create_topic_cmd("test_tp_1", 1, 3);
        cmd_1.value.cfg.compaction_strategy
          = model::compaction_strategy::offset;
        cmd_1.value.cfg.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        cmd_1.value.cfg.compression = model::compression::lz4;
        cmd_1.value.cfg.retention_bytes = tristate(std::make_optional(2_GiB));
        cmd_1.value.cfg.retention_duration = tristate(
          std::make_optional(std::chrono::milliseconds(3600000)));
        auto cmd_2 = make_create_topic_cmd("test_tp_2", 12, 3);
        auto cmd_3 = make_create_topic_cmd("test_tp_3", 8, 1);

        auto res_1
          = table.local().apply(std::move(cmd_1), model::offset(0)).get0();
        auto res_2
          = table.local().apply(std::move(cmd_2), model::offset(0)).get0();
        auto res_3
          = table.local().apply(std::move(cmd_3), model::offset(0)).get0();

        BOOST_REQUIRE_EQUAL(res_1, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_2, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_3, cluster::errc::success);
    }

    size_t total_capacity() {
        size_t total = 0;
        for (const auto& [id, n] : allocator.local().allocation_nodes()) {
            total += n->partition_capacity();
        }
        return total;
    }

    ss::sharded<cluster::partition_allocator> allocator;
    ss::sharded<cluster::topic_table> table;
    ss::abort_source as;
};
