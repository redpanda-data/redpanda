/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/tests/archival_service_fixture.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/state_machine.h"

static ss::logger logger{"datalake-test-logger"};
namespace datalake::tests {

class datalake_cluster_test_fixture
  : public cluster_test_fixture
  , public s3_imposter_fixture {
public:
    datalake_cluster_test_fixture() { set_expectations_and_listen({}); }

    ~datalake_cluster_test_fixture() {
        for (auto id : instance_ids()) {
            remove_node_application(id);
        }
    }

    void add_node() {
        static constexpr int kafka_port_base = 9092;
        static constexpr int rpc_port_base = 11000;
        static constexpr int proxy_port_base = 8082;
        static constexpr int schema_reg_port_base = 8081;

        auto [s3_conf, a_conf, cs_conf] = get_cloud_storage_configurations(
          httpd_host_name, httpd_port_number());

        create_node_application(
          next_node_id(),
          kafka_port_base,
          rpc_port_base,
          proxy_port_base,
          schema_reg_port_base,
          configure_node_id::yes,
          empty_seed_starts_cluster::yes,
          s3_conf,
          std::move(*a_conf),
          cs_conf,
          false,
          /*iceberg_enabled=*/true);
    }

    ss::future<> create_iceberg_topic(
      model::topic topic, int num_partitions = 1, int16_t num_replicas = 3) {
        cluster::topic_properties props;
        props.iceberg_mode = model::iceberg_mode::key_value;
        return cluster_test_fixture::create_topic(
          {model::kafka_namespace, topic},
          num_partitions,
          num_replicas,
          std::move(props));
    }

    ss::sharded<datalake::coordinator::frontend>&
    coordinator_frontend(model::node_id id) {
        return instance(id)->app.datalake_coordinator_frontend();
    }

    ss::future<unchecked<
      chunked_vector<datalake::coordinator::translated_offset_range>,
      datalake::coordinator::errc>>
    translated_files_for_partition(const model::ntp& ntp) {
        chunked_vector<datalake::coordinator::translated_offset_range> result;
        auto& fe = coordinator_frontend(model::node_id{0});
        auto coordinator_partition = fe.local().coordinator_partition(
          ntp.tp.topic);
        if (!coordinator_partition) {
            co_return datalake::coordinator::errc::coordinator_topic_not_exists;
        }
        auto c_ntp = model::ntp{
          model::datalake_coordinator_nt.ns,
          model::datalake_coordinator_nt.tp,
          coordinator_partition.value()};
        auto [_, partition] = get_leader(c_ntp);
        if (!partition) {
            co_return datalake::coordinator::errc::not_leader;
        }
        auto stm = partition->raft()
                     ->stm_manager()
                     ->get<datalake::coordinator::coordinator_stm>();
        if (!stm) {
            co_return datalake::coordinator::errc::not_leader;
        }
        const auto& state = stm->state().topic_to_state;
        auto it = state.find(ntp.tp.topic);
        if (
          it == state.end()
          || !it->second.pid_to_pending_files.contains(ntp.tp.partition)) {
            co_return result;
        }
        for (auto& range : it->second.pid_to_pending_files.at(ntp.tp.partition)
                             .pending_entries) {
            result.push_back(range.data.copy());
        }
        co_return result;
    }

    ss::future<> validate_translated_files(const model::ntp& ntp) {
        // Wait until all all the data is translated.
        auto [fixture, partition] = get_leader(ntp);
        if (!partition) {
            throw std::runtime_error("leader not found during validation");
        }
        auto topic_revision = partition->get_topic_revision_id();
        const auto& ot = partition->get_offset_translator_state();
        auto max_offset = kafka::prev_offset(model::offset_cast(
          ot->from_log_offset(partition->last_stable_offset())));
        auto& fe = coordinator_frontend(fixture->app.controller->self());
        coordinator::fetch_latest_translated_offset_request request;
        request.tp = ntp.tp;
        request.topic_revision = topic_revision;
        vlog(logger.info, "Waiting for last added offet: {}", max_offset);
        co_await ::tests::cooperative_spin_wait_with_timeout(20s, [&] {
            return fe.local().fetch_latest_translated_offset(request).then(
              [max_offset](
                coordinator::fetch_latest_translated_offset_reply resp) {
                  vlog(
                    logger.trace,
                    "Waiting for last added offet: {}, current: {}",
                    max_offset,
                    resp);
                  return resp.last_added_offset
                         && resp.last_added_offset.value() >= max_offset;
              });
        });
        // validate the continuity of the offset space.
        auto files = co_await translated_files_for_partition(ntp);
        if (files.has_error() || files.value().empty()) {
            throw std::runtime_error("No translated files found");
        }
        vlog(logger.info, "Translated files: {}", files.value());
        std::optional<kafka::offset> last_end;
        for (auto& f : files.value()) {
            BOOST_REQUIRE(
              !last_end
              || f.last_offset == kafka::next_offset(last_end.value()));
            last_end = f.last_offset;
        }
    }
};

} // namespace datalake::tests
