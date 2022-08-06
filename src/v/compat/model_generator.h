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

#include "compat/generator.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "random/generators.h"

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

} // namespace compat
