
/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/units.h"
#include "bytes/random.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "test_utils/randoms.h"
#include "utils/tests/randoms.h"

namespace model {
inline model::topic_namespace random_topic_namespace() {
    return {
      tests::random_named_string<model::ns>(),
      tests::random_named_string<model::topic>()};
}

inline model::ntp random_ntp() {
    return {
      tests::random_named_string<model::ns>(),
      tests::random_named_string<model::topic>(),
      tests::random_named_int<model::partition_id>()};
}

inline model::cleanup_policy_bitflags random_cleanup_policy() {
    return random_generators::random_choice(
      std::vector<model::cleanup_policy_bitflags>{
        model::cleanup_policy_bitflags::none,
        model::cleanup_policy_bitflags::compaction,
        model::cleanup_policy_bitflags::deletion});
}

inline model::compaction_strategy random_compaction_strategy() {
    return random_generators::random_choice(
      std::vector<model::compaction_strategy>{
        model::compaction_strategy::header,
        model::compaction_strategy::offset,
        model::compaction_strategy::timestamp});
}

inline model::compression random_compression() {
    return random_generators::random_choice(std::vector<model::compression>{
      model::compression::gzip,
      model::compression::zstd,
      model::compression::none,
      model::compression::producer,
      model::compression::snappy});
}

inline model::shadow_indexing_mode random_shadow_indexing_mode() {
    return random_generators::random_choice(
      std::vector<model::shadow_indexing_mode>{
        model::shadow_indexing_mode::archival,
        model::shadow_indexing_mode::drop_archival,
        model::shadow_indexing_mode::drop_fetch,
        model::shadow_indexing_mode::drop_full,
        model::shadow_indexing_mode::disabled,
        model::shadow_indexing_mode::full,
      });
}

inline model::timestamp_type random_timestamp_type() {
    return random_generators::random_choice(std::vector<model::timestamp_type>{
      model::timestamp_type::append_time, model::timestamp_type::create_time});
}

inline model::broker_endpoint random_broker_endpoint() {
    return {
      random_generators::gen_alphanum_string(
        random_generators::get_int(1, 100)),
      tests::random_net_address(),
    };
}

inline model::broker_properties random_broker_properties() {
    std::vector<ss::sstring> mount_paths;
    for (int i = 0; i < random_generators::get_int(10); i++) {
        mount_paths.push_back(random_generators::gen_alphanum_string(
          random_generators::get_int(1, 100)));
    }
    std::unordered_map<ss::sstring, ss::sstring> etc_props;
    for (int i = 0; i < random_generators::get_int(10); i++) {
        etc_props.emplace(
          random_generators::gen_alphanum_string(
            random_generators::get_int(1, 100)),
          random_generators::gen_alphanum_string(
            random_generators::get_int(1, 100)));
    }
    uint64_t memory_bytes = random_generators::get_int<uint64_t>(
      1, 100 * 1_GiB);

    return {
      .cores = random_generators::get_int<uint32_t>(1, 100),
      .available_memory_gb = static_cast<uint32_t>(memory_bytes >> 30),
      .available_disk_gb = random_generators::get_int<uint32_t>(1, 100),
      .mount_paths = std::move(mount_paths),
      .etc_props = std::move(etc_props),
      .available_memory_bytes = memory_bytes,
    };
}

inline model::broker random_broker(
  model::node_id node_id,
  model::broker_properties broker_properties = random_broker_properties()) {
    std::vector<model::broker_endpoint> kafka_advertised_listeners;
    for (int i = 0; i < random_generators::get_int(10); i++) {
        kafka_advertised_listeners.push_back(random_broker_endpoint());
    }

    std::optional<model::rack_id> rack;
    if (tests::random_bool()) {
        rack = tests::random_named_string<model::rack_id>();
    }

    return {
      node_id,
      std::move(kafka_advertised_listeners),
      tests::random_net_address(),
      rack,
      broker_properties,
    };
}

inline model::broker
random_broker(int32_t id_low_bound, int32_t id_upper_bound) {
    return random_broker(
      model::node_id(random_generators::get_int(id_low_bound, id_upper_bound)));
}

inline model::broker random_broker(
  int32_t id_low_bound,
  int32_t id_upper_bound,
  model::broker_properties broker_properties) {
    return random_broker(
      model::node_id(random_generators::get_int(id_low_bound, id_upper_bound)),
      broker_properties);
}

inline model::broker random_broker() {
    return random_broker(tests::random_named_int<model::node_id>());
}

inline model::membership_state random_membership_state() {
    return membership_state(random_generators::get_int<int8_t>(
      static_cast<int8_t>(model::membership_state::active),
      static_cast<int8_t>(model::membership_state::removed)));
}

inline model::producer_identity random_producer_identity() {
    return {
      random_generators::get_int<int64_t>(),
      random_generators::get_int<int16_t>()};
}

inline model::offset random_offset() {
    return tests::random_named_int<model::offset>();
}

inline model::tx_range random_tx_range() {
    return {random_producer_identity(), random_offset(), random_offset()};
}

inline model::broker_shard random_broker_shard() {
    return {
      tests::random_named_int<model::node_id>(),
      random_generators::get_int(std::numeric_limits<uint32_t>::max()),
    };
}

} // namespace model

namespace tests {

inline model::record_batch_type random_batch_type() {
    return random_generators::random_choice(
      std::vector<model::record_batch_type>{
        model::record_batch_type::raft_data,
        model::record_batch_type::raft_configuration,
        model::record_batch_type::controller,
        model::record_batch_type::kvstore,
        model::record_batch_type::checkpoint,
        model::record_batch_type::topic_management_cmd,
        model::record_batch_type::ghost_batch,
        model::record_batch_type::id_allocator,
        model::record_batch_type::tx_prepare,
        model::record_batch_type::tx_fence,
        model::record_batch_type::tm_update,
        model::record_batch_type::user_management_cmd,
        model::record_batch_type::acl_management_cmd,
        model::record_batch_type::group_prepare_tx,
        model::record_batch_type::group_commit_tx,
        model::record_batch_type::group_abort_tx,
        model::record_batch_type::node_management_cmd,
        model::record_batch_type::data_policy_management_cmd,
        model::record_batch_type::archival_metadata,
        model::record_batch_type::cluster_config_cmd,
        model::record_batch_type::feature_update,
      });
}

} // namespace tests
