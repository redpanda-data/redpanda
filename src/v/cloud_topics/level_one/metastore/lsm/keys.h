/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// Defines keys to be used for identifying state in an kv-store-backed
// implementation of the L1 metastore.

enum class row_type : uint8_t {
    metadata = 0,
    extent = 1,
    term_start = 2,
    compaction = 3,
    object = 4,
};

// clang-format off
// Key: row_type | topic_id                         | partition_id
// Ex:  00       | 0a1b2c3d4e5f67890a1b2c3d4e5f6789 | 00000001
// clang-format on
struct metadata_row_key {
    static std::optional<metadata_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&);

    model::topic_id_partition tidp;
};

// clang-format off
// Key: row_type | topic_id                         | partition_id | base_offset
// Ex:  01       | 0a1b2c3d4e5f67890a1b2c3d4e5f6789 | 00000001     | 0000000000000064
// clang-format on
struct extent_row_key {
    static std::optional<extent_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&, kafka::offset);

    model::topic_id_partition tidp;
    kafka::offset base_offset;
};

// clang-format off
// Key: row_type | topic_id                         | partition_id | term_id
// Ex:  02       | 0a1b2c3d4e5f67890a1b2c3d4e5f6789 | 00000001     | 0000000000000005
// clang-format on
struct term_row_key {
    static std::optional<term_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&, model::term_id);

    model::topic_id_partition tidp;
    model::term_id term;
};

// clang-format off
// Key: row_type | topic_id                         | partition_id
// Ex:  03       | 0a1b2c3d4e5f67890a1b2c3d4e5f6789 | 00000001
// clang-format on
struct compaction_row_key {
    static std::optional<compaction_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&);

    model::topic_id_partition tidp;
};

// clang-format off
// Key: row_type | object_id
// Ex:  04       | fedcba9876543210fedcba9876543210
// clang-format on
struct object_row_key {
    static std::optional<object_row_key> decode(std::string_view);
    static ss::sstring encode(const object_id&);

    object_id oid;
};

} // namespace cloud_topics::l1
