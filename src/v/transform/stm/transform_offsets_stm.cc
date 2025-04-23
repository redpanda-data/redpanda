/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/stm/transform_offsets_stm.h"

#include "model/metadata.h"

#include <seastar/util/log.hh>

namespace transform {

namespace {
// NOLINTNEXTLINE
static ss::logger log{"transform/stm"};
} // namespace

bool transform_offsets_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    const auto& ntp = cfg.ntp();
    return ntp.ns == model::kafka_internal_namespace
           && ntp.tp.topic == model::transform_offsets_topic;
}

void transform_offsets_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config& cfg) {
    const auto tcfg = cfg.initial_topic_cfg;
    vassert(
      tcfg != nullptr,
      "When creating transform stm the topic configuration must exist");

    builder.create_stm<transform_offsets_stm_t>(
      tcfg->partition_count, log, raft);
}

} // namespace transform
