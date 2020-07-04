
#include "cluster/commands.h"
#include "cluster/partition_allocator.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "test_utils/fixture.h"
#include "units.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/variant_utils.hh>

#include <bits/stdint-intn.h>
#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;

std::unique_ptr<cluster::allocation_node>
create_allocation_node(model::node_id nid, uint32_t cores) {
    return std::make_unique<cluster::allocation_node>(
      nid, cores, std::unordered_map<ss::sstring, ss::sstring>{});
}

struct cmd_test_fixture {
    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        cfg.segment_size = 100_MiB;
        cfg.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        cfg.compression = model::compression::gzip;

        auto pas = allocate(cfg);

        return cluster::topic_configuration_assignment(cfg, std::move(pas));
    }

    cluster::create_topic_cmd make_create_topic_cmd(
      const ss::sstring& name, int partitions, int replication_factor) {
        return cluster::create_topic_cmd(
          make_tp_ns(name),
          make_tp_configuration(name, partitions, replication_factor));
    }

    cluster::delete_topic_cmd make_delete_topic_cmd(const ss::sstring& name) {
        return cluster::delete_topic_cmd(make_tp_ns(name), make_tp_ns(name));
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return model::topic_namespace(test_ns, model::topic(tp));
    }

    std::vector<cluster::partition_assignment>
    allocate(const cluster::topic_configuration& cfg) {
        std::vector<cluster::partition_assignment> ret;
        ret.reserve(cfg.partition_count);
        for (int i = 0; i < cfg.partition_count; i++) {
            ret.push_back(cluster::partition_assignment{
              raft::group_id(0), model::partition_id(i), {}});
        }
        return {};
    }
};

FIXTURE_TEST(test_create_topic_cmd_serialization, cmd_test_fixture) {
    auto cmd = make_create_topic_cmd("test_tp", 2, 3);

    auto batch = cluster::serialize_cmd(cmd).get0();
    auto deser = cluster::deserialize(
                   std::move(batch),
                   cluster::make_commands_list<cluster::create_topic_cmd>())
                   .get0();
    ss::visit(deser, [&cmd](cluster::create_topic_cmd c) {
        BOOST_REQUIRE_EQUAL(c.key.tp, cmd.key.tp);
        BOOST_REQUIRE_EQUAL(c.value.cfg.compression, cmd.value.cfg.compression);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.cleanup_policy_bitflags,
          cmd.value.cfg.cleanup_policy_bitflags);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.segment_size, cmd.value.cfg.segment_size);
        BOOST_REQUIRE_EQUAL(c.value.cfg.compression, cmd.value.cfg.compression);
        BOOST_REQUIRE_EQUAL(
          c.value.assignments.size(), cmd.value.assignments.size());
    });
}

FIXTURE_TEST(test_delete_topic_cmd_serialization, cmd_test_fixture) {
    auto cmd = make_delete_topic_cmd("test_tp");

    auto batch = cluster::serialize_cmd(cmd).get0();
    auto deser = cluster::deserialize(
                   std::move(batch),
                   cluster::make_commands_list<cluster::delete_topic_cmd>())
                   .get0();
    ss::visit(deser, [&cmd](cluster::delete_topic_cmd c) {
        BOOST_REQUIRE_EQUAL(c.key.tp, cmd.key.tp);
        BOOST_REQUIRE_EQUAL(c.value, cmd.value);
    });
}
