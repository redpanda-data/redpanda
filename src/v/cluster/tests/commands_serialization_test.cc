// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/commands.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "test_utils/fixture.h"
#include "topic_table_fixture.h"
#include "units.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/variant_utils.hh>

#include <bits/stdint-intn.h>
#include <boost/test/tools/old/interface.hpp>

#include <vector>

using namespace std::chrono_literals;

namespace {

// Fake command type used to test serde-only types.
static constexpr int8_t fake_serde_only_cmd_type = 0;
struct fake_serde_only_key
  : serde::envelope<
      fake_serde_only_key,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    ss::sstring str;
};
struct fake_serde_only_val
  : serde::envelope<
      fake_serde_only_val,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    ss::sstring str;
};
using fake_serde_only_cmd = cluster::controller_command<
  fake_serde_only_key,
  fake_serde_only_val,
  fake_serde_only_cmd_type,
  model::record_batch_type::ghost_batch>;

} // anonymous namespace

struct cmd_test_fixture {
    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        cfg.properties.segment_size = 100_MiB;
        cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        cfg.properties.compression = model::compression::gzip;

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

    cluster::move_partition_replicas_cmd make_move_partition_replicas_cmd(
      model::ntp ntp, std::vector<model::broker_shard> replica_set) {
        return cluster::move_partition_replicas_cmd(
          std::move(ntp), std::move(replica_set));
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return model::topic_namespace(test_ns, model::topic(tp));
    }

    std::vector<cluster::partition_assignment>
    allocate(const cluster::topic_configuration& cfg) {
        std::vector<cluster::partition_assignment> ret;
        ret.reserve(cfg.partition_count);
        for (int i = 0; i < cfg.partition_count; i++) {
            ret.emplace_back(
              raft::group_id(0),
              model::partition_id(i),
              std::vector<model::broker_shard>{});
        }
        return {};
    }
};

FIXTURE_TEST(test_serde_only_cmd, cmd_test_fixture) {
    fake_serde_only_key k;
    fake_serde_only_val v;
    k.str = "foo";
    v.str = "bar";
    fake_serde_only_cmd cmd(std::move(k), std::move(v));
    auto batch = cluster::serde_serialize_cmd(cmd);
    auto deser = cluster::deserialize(
                   std::move(batch),
                   cluster::make_commands_list<fake_serde_only_cmd>())
                   .get0();
    ss::visit(deser, [&cmd](fake_serde_only_cmd c) {
        BOOST_REQUIRE_EQUAL(c.key.str, cmd.key.str);
        BOOST_REQUIRE_EQUAL(c.value.str, cmd.value.str);
    });
}

FIXTURE_TEST(test_create_topic_cmd_serialization, cmd_test_fixture) {
    auto cmd = make_create_topic_cmd("test_tp", 2, 3);

    auto batch = cluster::serialize_cmd(cmd).get0();
    auto deser = cluster::deserialize(
                   std::move(batch),
                   cluster::make_commands_list<cluster::create_topic_cmd>())
                   .get0();
    ss::visit(deser, [&cmd](cluster::create_topic_cmd c) {
        BOOST_REQUIRE_EQUAL(c.key.tp, cmd.key.tp);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.properties.compression,
          cmd.value.cfg.properties.compression);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.properties.cleanup_policy_bitflags,
          cmd.value.cfg.properties.cleanup_policy_bitflags);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.properties.segment_size,
          cmd.value.cfg.properties.segment_size);
        BOOST_REQUIRE_EQUAL(
          c.value.cfg.properties.compression,
          cmd.value.cfg.properties.compression);
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

FIXTURE_TEST(test_move_partition_replicass_command, cmd_test_fixture) {
    auto ntp = model::ntp(test_ns, model::topic("tp"), model::partition_id(20));
    std::vector<model::broker_shard> replicas{
      model::broker_shard{.node_id = model::node_id(1), .shard = 1},
      model::broker_shard{.node_id = model::node_id(2), .shard = 8},
      model::broker_shard{.node_id = model::node_id(3), .shard = 3},
    };
    auto cmd = make_move_partition_replicas_cmd(ntp, replicas);

    auto batch = cluster::serialize_cmd(cmd).get0();
    auto deser
      = cluster::deserialize(
          std::move(batch),
          cluster::make_commands_list<cluster::move_partition_replicas_cmd>())
          .get0();

    ss::visit(deser, [&cmd](cluster::move_partition_replicas_cmd c) {
        BOOST_REQUIRE_EQUAL(c.key, cmd.key);
        for (int i = 0; i < cmd.value.size(); ++i) {
            BOOST_REQUIRE_EQUAL(c.value[i].node_id, cmd.value[i].node_id);
            BOOST_REQUIRE_EQUAL(c.value[i].shard, cmd.value[i].shard);
        }
    });
}
