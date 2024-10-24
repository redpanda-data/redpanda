// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/topic_table_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>

using namespace std::chrono_literals;

static void validate_topic_deltas(
  const std::vector<cluster::topic_table::topic_delta>& d,
  int new_topics,
  int removed_topics) {
    size_t additions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::topic_delta& d) {
          return d.type == cluster::topic_table_topic_delta_type::added;
      });
    size_t deletions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::topic_delta& d) {
          return d.type == cluster::topic_table_topic_delta_type::removed;
      });
    BOOST_REQUIRE_EQUAL(additions, new_topics);
    BOOST_REQUIRE_EQUAL(deletions, removed_topics);
}

static void validate_ntp_deltas(
  const std::vector<cluster::topic_table::ntp_delta>& d,
  int new_partitions,
  int removed_partitions) {
    size_t additions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::ntp_delta& d) {
          return d.type == cluster::topic_table_ntp_delta_type::added;
      });
    size_t deletions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::ntp_delta& d) {
          return d.type == cluster::topic_table_ntp_delta_type::removed;
      });
    BOOST_REQUIRE_EQUAL(additions, new_partitions);
    BOOST_REQUIRE_EQUAL(deletions, removed_partitions);
}

FIXTURE_TEST(test_happy_path_create, topic_table_fixture) {
    std::vector<cluster::topic_table_topic_delta> topic_deltas;
    table.local().register_topic_delta_notification([&](const auto& d) {
        topic_deltas.insert(topic_deltas.end(), d.begin(), d.end());
    });

    std::vector<cluster::topic_table_ntp_delta> deltas;
    table.local().register_ntp_delta_notification(
      [&](const auto& d) { deltas.insert(deltas.end(), d.begin(), d.end()); });

    create_topics();
    auto& md = table.local().all_topics_metadata();

    BOOST_REQUIRE_EQUAL(md.size(), 3);

    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_1")));
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_2")));
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_3")));

    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_2"))->second.get_assignments().size(), 12);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_3"))->second.get_assignments().size(), 8);

    validate_topic_deltas(topic_deltas, 3, 0);
    validate_ntp_deltas(deltas, 21, 0);
}

FIXTURE_TEST(test_happy_path_delete, topic_table_fixture) {
    create_topics();

    std::vector<cluster::topic_table_topic_delta> topic_deltas;
    table.local().register_topic_delta_notification([&](const auto& d) {
        topic_deltas.insert(topic_deltas.end(), d.begin(), d.end());
    });

    std::vector<cluster::topic_table_ntp_delta> deltas;
    table.local().register_ntp_delta_notification(
      [&](const auto& d) { deltas.insert(deltas.end(), d.begin(), d.end()); });

    BOOST_REQUIRE(!table.local()
                     .apply(
                       cluster::delete_topic_cmd(
                         make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2")),
                       model::offset(0))
                     .get());
    BOOST_REQUIRE(!table.local()
                     .apply(
                       cluster::delete_topic_cmd(
                         make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3")),
                       model::offset(0))
                     .get());

    auto& md = table.local().all_topics_metadata();
    BOOST_REQUIRE_EQUAL(md.size(), 1);
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_1")));

    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);

    validate_topic_deltas(topic_deltas, 0, 2);
    validate_ntp_deltas(deltas, 0, 20);
}

FIXTURE_TEST(test_conflicts, topic_table_fixture) {
    create_topics();

    std::vector<cluster::topic_table_topic_delta> topic_deltas;
    table.local().register_topic_delta_notification([&](const auto& d) {
        topic_deltas.insert(topic_deltas.end(), d.begin(), d.end());
    });

    std::vector<cluster::topic_table_ntp_delta> deltas;
    table.local().register_ntp_delta_notification(
      [&](const auto& d) { deltas.insert(deltas.end(), d.begin(), d.end()); });

    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("not_exists"), make_tp_ns("not_exists")),
                     model::offset(0))
                   .get();
    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::topic_not_exists);

    auto res_2 = table.local()
                   .apply(
                     make_create_topic_cmd("test_tp_1", 2, 3), model::offset(0))
                   .get();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::topic_already_exists);
    BOOST_REQUIRE_EQUAL(topic_deltas.size(), 0);
    BOOST_REQUIRE_EQUAL(deltas.size(), 0);
}

FIXTURE_TEST(get_getting_config, topic_table_fixture) {
    create_topics();
    auto cfg = table.local().get_topic_cfg(make_tp_ns("test_tp_1"));
    BOOST_REQUIRE(cfg.has_value());
    auto v = cfg.value();
    BOOST_REQUIRE_EQUAL(
      v.properties.compaction_strategy, model::compaction_strategy::offset);

    BOOST_REQUIRE_EQUAL(
      v.properties.cleanup_policy_bitflags,
      model::cleanup_policy_bitflags::compaction);
    BOOST_REQUIRE_EQUAL(v.properties.compression, model::compression::lz4);
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_bytes, tristate(std::make_optional(2_GiB)));
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_duration,
      tristate(std::make_optional(std::chrono::milliseconds(3600000))));
}

FIXTURE_TEST(test_adding_partition, topic_table_fixture) {
    create_topics();

    std::vector<cluster::topic_table_topic_delta> topic_deltas;
    table.local().register_topic_delta_notification([&](const auto& d) {
        topic_deltas.insert(topic_deltas.end(), d.begin(), d.end());
    });

    std::vector<cluster::topic_table_ntp_delta> deltas;
    table.local().register_ntp_delta_notification(
      [&](const auto& d) { deltas.insert(deltas.end(), d.begin(), d.end()); });

    cluster::create_partitions_configuration cfg(make_tp_ns("test_tp_2"), 3);
    ss::chunked_fifo<cluster::partition_assignment> p_as;
    p_as.push_back(cluster::partition_assignment{
      raft::group_id(10),
      model::partition_id(0),
      {model::broker_shard{model::node_id(0), 0},
       model::broker_shard{model::node_id(1), 1},
       model::broker_shard{model::node_id(2), 2}},
    });
    p_as.push_back(cluster::partition_assignment{
      raft::group_id(11),
      model::partition_id(1),
      {model::broker_shard{model::node_id(0), 0},
       model::broker_shard{model::node_id(1), 1},
       model::broker_shard{model::node_id(2), 2}},
    });
    p_as.push_back(cluster::partition_assignment{
      raft::group_id(12),
      model::partition_id(2),
      {model::broker_shard{model::node_id(0), 0},
       model::broker_shard{model::node_id(1), 1},
       model::broker_shard{model::node_id(2), 2}},
    });
    cluster::create_partitions_configuration_assignment pca(
      std::move(cfg), std::move(p_as));

    auto result = table.local()
                    .apply(
                      cluster::create_partition_cmd(
                        make_tp_ns("test_tp_2"), std::move(pca)),
                      model::offset(0))
                    .get();
    BOOST_REQUIRE(!result);

    auto md = table.local().get_topic_metadata(make_tp_ns("test_tp_2"));

    BOOST_REQUIRE_EQUAL(md->get_assignments().size(), 15);
    // require 3 partition additions
    BOOST_REQUIRE_EQUAL(topic_deltas.size(), 0);
    validate_ntp_deltas(deltas, 3, 0);
}

void validate_brokers_revisions(
  const model::ntp& ntp,
  const cluster::topic_table::underlying_t& all_metadata,
  const absl::flat_hash_map<model::node_id, model::revision_id>&
    expected_revisions) {
    // first check if initial revisions of brokers are valid

    auto tp_it = all_metadata.find(model::topic_namespace_view(ntp));
    BOOST_REQUIRE(tp_it != all_metadata.end());

    auto p_it = tp_it->second.metadata.get_assignments().find(ntp.tp.partition);
    BOOST_REQUIRE(p_it != tp_it->second.metadata.get_assignments().end());

    auto p_meta_it = tp_it->second.partitions.find(ntp.tp.partition);
    BOOST_REQUIRE(p_meta_it != tp_it->second.partitions.end());
    const auto& revisions = p_meta_it->second.replicas_revisions;

    for (auto& bs : p_it->second.replicas) {
        fmt::print("replica: {}\n", bs);
    }
    for (auto& bs : expected_revisions) {
        fmt::print("expected_rev: {} = {}\n", bs.first, bs.second);
    }

    for (const auto& [node, rev] : revisions) {
        fmt::print("current_rev: {} = {}\n", node, rev);
    }
    BOOST_REQUIRE_EQUAL(expected_revisions.size(), revisions.size());
    for (auto& [id, rev] : revisions) {
        auto ex = expected_revisions.find(id);

        BOOST_REQUIRE(ex != expected_revisions.end());

        fmt::print("Checking {} == {}\n", rev, ex->second);
        BOOST_REQUIRE_EQUAL(rev, ex->second);
    }
}

void validate_command_revisions(
  const model::ntp& ntp,
  const cluster::topic_table& topics,
  model::revision_id exp_last_update_finished,
  model::revision_id exp_cur_update,
  model::revision_id exp_last_cmd) {
    auto tp_it = topics.all_topics_metadata().find(
      model::topic_namespace_view(ntp));
    BOOST_REQUIRE(tp_it != topics.all_topics_metadata().end());

    auto p_meta_it = tp_it->second.partitions.find(ntp.tp.partition);
    BOOST_REQUIRE(p_meta_it != tp_it->second.partitions.end());

    BOOST_REQUIRE_EQUAL(
      p_meta_it->second.last_update_finished_revision,
      exp_last_update_finished);

    model::revision_id cur_update;
    model::revision_id last_cmd;
    auto in_progress_it = topics.updates_in_progress().find(ntp);
    if (in_progress_it != topics.updates_in_progress().end()) {
        cur_update = in_progress_it->second.get_update_revision();
        last_cmd = in_progress_it->second.get_last_cmd_revision();
    }

    BOOST_REQUIRE_EQUAL(cur_update, exp_cur_update);
    BOOST_REQUIRE_EQUAL(last_cmd, exp_last_cmd);
};

template<typename Cmd, typename Key, typename Val>
void apply_cmd(
  cluster::topic_table& table, Key k, Val v, model::offset revision) {
    auto ec = table.apply(Cmd(std::move(k), std::move(v)), revision).get();

    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);
}

FIXTURE_TEST(test_tracking_broker_and_command_revisions, topic_table_fixture) {
    auto& topics = table.local();
    static const model::node_id n_0(0);
    static const model::node_id n_1(1);
    static const model::node_id n_2(2);
    static const model::node_id n_3(3);
    static const model::node_id n_4(4);

    using rev_map_t = absl::flat_hash_map<model::node_id, model::revision_id>;

    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic("test_tp"));

    model::ntp ntp_0(tp_ns.ns, tp_ns.tp, model::partition_id(0));

    cluster::topic_configuration_assignment cfg(
      cluster::topic_configuration(tp_ns.ns, tp_ns.tp, 1, 3), {});

    std::vector<model::broker_shard> replicas;
    replicas.reserve(3);
    for (auto n = 0; n < 3; ++n) {
        replicas.push_back(
          model::broker_shard{.node_id = model::node_id(n), .shard = 0});
    }

    cfg.assignments.emplace_back(
      raft::group_id(0), ntp_0.tp.partition, std::move(replicas));

    apply_cmd<cluster::create_topic_cmd>(
      topics, tp_ns, std::move(cfg), model::offset(10));

    auto& topic_metadata = table.local().all_topics_metadata();

    // first check if initial revisions of brokers are valid
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(10)},
        {n_2, model::revision_id(10)}});

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{10},
      model::revision_id{},
      model::revision_id{});

    // move one of the topic partitions to new replica set
    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0},
        model::broker_shard{n_3, 0}, // new broker
      },
      model::offset(11));

    // validate that revisions are unchanged
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(10)},
        {n_2, model::revision_id(10)}});

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{10},
      model::revision_id{11},
      model::revision_id{11});

    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0},
        model::broker_shard{n_3, 0}, // new broker
      },
      model::offset(12));

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{12},
      model::revision_id{},
      model::revision_id{});

    // validate that the new broker was added with the updated_version
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(10)},
        {n_3, model::revision_id(11)}});

    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0}, // new broker
        model::broker_shard{n_3, 0},
      },
      model::offset(13));

    // finish before validating
    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0}, // new broker
        model::broker_shard{n_3, 0},
      },
      model::offset(14));

    // validate that new broker was added with updated revision
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    // x-core move should not update replica revisions
    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0},
        model::broker_shard{n_3, 1}, // updated shard
      },
      model::offset(15));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{14},
      model::revision_id{15},
      model::revision_id{15});

    // finish before validating
    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0},
        model::broker_shard{n_3, 1}, // updated shard
      },
      model::offset(16));

    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0}, // new broker
        model::broker_shard{n_3, 2}, // shard updated
      },
      model::offset(17));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    // cancel should revert replica revisions to previous state
    apply_cmd<cluster::cancel_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      cluster::cancel_moving_partition_replicas_cmd_data(
        cluster::force_abort_update::no),
      model::offset(18));
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{16},
      model::revision_id{17},
      model::revision_id{18});

    // force cancel should not change the revision tracking
    apply_cmd<cluster::cancel_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      cluster::cancel_moving_partition_replicas_cmd_data(
        cluster::force_abort_update::yes),
      model::offset(19));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    validate_command_revisions(
      ntp_0,
      table.local(),
      model::revision_id{16},
      model::revision_id{17},
      model::revision_id{19});
}

FIXTURE_TEST(test_topic_with_schema_id_validation_ops, topic_table_fixture) {
    auto& topics = table.local();

    // Create a topic with defaults
    auto create = make_create_topic_cmd("test_schema_id_validation", 1, 1);
    auto tp_ns = create.value.cfg.tp_ns;
    auto ec = topics.apply(create, model::offset{10}).get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);

    auto cfg = topics.get_topic_cfg(tp_ns);
    BOOST_REQUIRE(cfg.has_value());
    BOOST_REQUIRE(!cfg->properties.record_key_schema_id_validation.has_value());

    // enable record_key_schema_id_validation
    cluster::incremental_topic_updates update;
    update.record_key_schema_id_validation.op
      = cluster::incremental_update_operation::set;
    update.record_key_schema_id_validation.value.emplace(true);
    ec = topics
           .apply(
             cluster::update_topic_properties_cmd{tp_ns, update},
             model::offset{11})
           .get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);
    cfg = topics.get_topic_cfg(tp_ns);
    BOOST_REQUIRE(cfg.has_value());
    BOOST_REQUIRE_EQUAL(cfg->properties.record_key_schema_id_validation, true);

    // remove record_key_schema_id_validation
    update.record_key_schema_id_validation.op
      = cluster::incremental_update_operation::remove;
    update.record_key_schema_id_validation.value.emplace(true);
    ec = topics
           .apply(
             cluster::update_topic_properties_cmd{tp_ns, update},
             model::offset{11})
           .get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);
    cfg = topics.get_topic_cfg(tp_ns);
    BOOST_REQUIRE(cfg.has_value());
    BOOST_REQUIRE(!cfg->properties.record_key_schema_id_validation.has_value());

    // Ensure that an invalid update cmd does not get persisted in the topic
    // table.
    // Sanity check before starting.
    BOOST_REQUIRE(!cfg->properties.record_key_schema_id_validation.has_value());
    BOOST_REQUIRE(
      !cfg->properties.record_key_schema_id_validation_compat.has_value());

    update.record_key_schema_id_validation.op
      = cluster::incremental_update_operation::set;
    update.record_key_schema_id_validation.value.emplace(true);

    update.record_key_schema_id_validation_compat.op
      = cluster::incremental_update_operation::set;
    update.record_key_schema_id_validation_compat.value.emplace(false);
    ec = topics
           .apply(
             cluster::update_topic_properties_cmd{tp_ns, update},
             model::offset{11})
           .get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::topic_invalid_config);
    cfg = topics.get_topic_cfg(tp_ns);
    BOOST_REQUIRE(cfg.has_value());

    // Properties from invalid configuration should not have been persisted.
    BOOST_REQUIRE(!cfg->properties.record_key_schema_id_validation.has_value());
    BOOST_REQUIRE(
      !cfg->properties.record_key_schema_id_validation_compat.has_value());
}

FIXTURE_TEST(test_topic_table_iterator_basic, topic_table_fixture) {
    create_topics();

    const auto& topics = table.local();

    auto find_tp_ns = [&](const model::topic_namespace& tp_ns) {
        return std::find_if(
          topics.topics_iterator_begin(),
          topics.topics_iterator_end(),
          [&](const auto& it) { return it.first == tp_ns; });
    };

    auto end = topics.topics_iterator_end();
    BOOST_REQUIRE(find_tp_ns(make_tp_ns("test_tp_1")) != end);
    BOOST_REQUIRE(find_tp_ns(make_tp_ns("test_tp_2")) != end);
    BOOST_REQUIRE(find_tp_ns(make_tp_ns("test_tp_3")) != end);
    BOOST_REQUIRE(find_tp_ns(make_tp_ns("abcdef")) == end);
}

FIXTURE_TEST(test_topic_table_iterator_invalidation, topic_table_fixture) {
    create_topics();
    const auto& topics = table.local();

    auto it = topics.topics_iterator_begin();
    BOOST_REQUIRE(it != topics.topics_iterator_end());
    BOOST_REQUIRE_NO_THROW((void)it->first);
    add_random_topic(); // invalidates iterator
    BOOST_REQUIRE_THROW((void)it->first, iterator_stability_violation);
}
