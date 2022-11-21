/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc_cluster_fixture.h"

#include "coproc/tests/utils/event_publisher_utils.h"

coproc_cluster_fixture::coproc_cluster_fixture() noexcept
  : cluster_test_fixture()
  , coproc_api_fixture() {
    set_configuration("enable_coproc", true);
}

ss::future<>
coproc_cluster_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::script_id> ids;
    std::vector<ss::future<>> wait;
    std::transform(
      copros.cbegin(),
      copros.cend(),
      std::back_inserter(ids),
      [](const deploy& d) { return coproc::script_id(d.id); });
    co_await coproc_api_fixture::enable_coprocessors(std::move(copros));
    auto node_ids = get_node_ids();
    co_await ss::parallel_for_each(
      node_ids, [this, ids](const model::node_id& node_id) {
          application* app = get_node_application(node_id);
          return ss::parallel_for_each(ids, [app](coproc::script_id id) {
              return coproc::wasm::wait_for_copro(
                app->coprocessing->get_pacemaker(), id);
          });
      });
}

application* coproc_cluster_fixture::create_node_application(
  model::node_id node_id,
  int kafka_port,
  int rpc_port,
  int proxy_port,
  int schema_reg_port,
  int coproc_supervisor_port) {
    application* app = cluster_test_fixture::create_node_application(
      node_id,
      kafka_port,
      rpc_port,
      proxy_port,
      schema_reg_port,
      coproc_supervisor_port);
    _instances.emplace(
      node_id,
      std::make_unique<supervisor_test_fixture>(
        coproc_supervisor_port + node_id()));
    return app;
}

std::vector<model::node_id> coproc_cluster_fixture::get_node_ids() {
    std::vector<model::node_id> ids;
    std::transform(
      _instances.cbegin(),
      _instances.cend(),
      std::back_inserter(ids),
      [](const auto& p) { return p.first; });
    return ids;
}
