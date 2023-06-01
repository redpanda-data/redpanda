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

#include "bytes/iobuf.h"
#include "compat/generator.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <cstdint>

namespace compat {

template<>
struct instance_generator<model::compression> {
    static model::compression random() {
        return random_generators::random_choice(
          {model::compression::gzip,
           model::compression::lz4,
           model::compression::none,
           model::compression::producer,
           model::compression::snappy,
           model::compression::zstd});
    }

    static std::vector<model::compression> limits() { return {}; }
};

template<>
struct instance_generator<model::cleanup_policy_bitflags> {
    static model::cleanup_policy_bitflags random() {
        return random_generators::random_choice(
          {model::cleanup_policy_bitflags::none,
           model::cleanup_policy_bitflags::deletion,
           model::cleanup_policy_bitflags::compaction});
    }

    static std::vector<model::cleanup_policy_bitflags> limits() { return {}; }
};

template<>
struct instance_generator<model::compaction_strategy> {
    static model::compaction_strategy random() {
        return random_generators::random_choice(
          {model::compaction_strategy::offset,
           model::compaction_strategy::header,
           model::compaction_strategy::timestamp});
    }

    static std::vector<model::compaction_strategy> limits() { return {}; }
};

template<>
struct instance_generator<model::shadow_indexing_mode> {
    static model::shadow_indexing_mode random() {
        return random_generators::random_choice({
          model::shadow_indexing_mode::disabled,
          model::shadow_indexing_mode::archival,
          model::shadow_indexing_mode::fetch,
          model::shadow_indexing_mode::full,
          model::shadow_indexing_mode::drop_archival,
          model::shadow_indexing_mode::drop_fetch,
          model::shadow_indexing_mode::drop_full,
        });
    }

    static std::vector<model::shadow_indexing_mode> limits() { return {}; }
};

template<>
struct instance_generator<model::timestamp_type> {
    static model::timestamp_type random() {
        return random_generators::random_choice(
          {model::timestamp_type::append_time,
           model::timestamp_type::create_time});
    }
    static std::vector<model::timestamp_type> limits() { return {}; }
};

template<>
struct instance_generator<model::partition_metadata> {
    static model::partition_metadata random() {
        auto pm = model::partition_metadata();
        pm.id = tests::random_named_int<model::partition_id>();
        pm.replicas = tests::random_vector(
          [] { return model::random_broker_shard(); });
        pm.leader_node = tests::random_optional(
          [] { return tests::random_named_int<model::node_id>(); });
        return pm;
    }
    static std::vector<model::partition_metadata> limits() { return {}; }
};

template<>
struct instance_generator<model::topic_metadata> {
    static model::topic_metadata random() {
        auto tm = model::topic_metadata();
        tm.tp_ns = model::random_topic_namespace();
        tm.partitions = tests::random_vector([] {
            return instance_generator<model::partition_metadata>::random();
        });
        return tm;
    }
    static std::vector<model::topic_metadata> limits() { return {}; }
};

template<>
struct instance_generator<model::record_batch_header> {
    static model::record_batch_header random() {
        model::record_batch_header h;
        h.header_crc = random_generators::get_int(
          std::numeric_limits<uint32_t>::min(),
          std::numeric_limits<uint32_t>::max());

        h.size_bytes = random_generators::get_int(
          std::numeric_limits<int32_t>::min(),
          std::numeric_limits<int32_t>::max());
        h.base_offset = tests::random_named_int<model::offset>();
        h.type = tests::random_batch_type();
        h.crc = random_generators::get_int(
          std::numeric_limits<int32_t>::min(),
          std::numeric_limits<int32_t>::max());
        h.attrs = model::record_batch_attributes(
          random_generators::get_int<int16_t>(0, 256));
        h.last_offset_delta = random_generators::get_int(0, 10);
        h.first_timestamp = model::timestamp(
          random_generators::get_int<int64_t>(
            0, std::numeric_limits<int64_t>::max()));
        h.max_timestamp = model::timestamp(random_generators::get_int<int64_t>(
          0, std::numeric_limits<int64_t>::max()));
        h.producer_id = random_generators::get_int(
          random_generators::get_int<int64_t>(
            0, std::numeric_limits<int64_t>::max()));
        h.producer_epoch = random_generators::get_int(
          random_generators::get_int<int16_t>(
            0, std::numeric_limits<int16_t>::max()));
        h.base_sequence = random_generators::get_int(
          0, std::numeric_limits<int32_t>::max());
        h.record_count = random_generators::get_int(
          0, std::numeric_limits<int32_t>::max());
        return h;
    }
    static std::vector<model::record_batch_header> limits() { return {}; }
};

template<>
struct instance_generator<model::record_batch> {
    static model::record_batch random() {
        return model::test::make_random_batch(model::test::record_batch_spec{
          .allow_compression = true,
          .count = 1,
          .bt = tests::random_batch_type(),
          .enable_idempotence = tests::random_bool(),
          .producer_id = random_generators::get_int(
            0, std::numeric_limits<int32_t>::max()),
          .producer_epoch = random_generators::get_int<int16_t>(
            0, std::numeric_limits<int16_t>::max()),
          .base_sequence = random_generators::get_int(
            0, std::numeric_limits<int32_t>::max()),
          .is_transactional = tests::random_bool()});
    }

    static std::vector<model::record_batch> limits() { return {}; }
};

} // namespace compat
