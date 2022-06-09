
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
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "test_utils/randoms.h"

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
} // namespace model
