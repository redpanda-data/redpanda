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

enum class row_type : uint8_t {
    metadata = 0,
    extent = 1,
    term_start = 2,
    compaction = 3,
    object = 4,
};

struct metadata_row_key {
    static std::optional<metadata_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&);
    ss::sstring encode() const { return encode(tidp); }

    model::topic_id_partition tidp;
};

struct extent_row_key {
    static std::optional<extent_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&, kafka::offset);
    ss::sstring encode() const { return encode(tidp, base_offset); }

    model::topic_id_partition tidp;
    kafka::offset base_offset;
};

struct term_row_key {
    static std::optional<term_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&, model::term_id);
    ss::sstring encode() const { return encode(tidp, term); }

    model::topic_id_partition tidp;
    model::term_id term;
};

struct compaction_row_key {
    static std::optional<compaction_row_key> decode(std::string_view);
    static ss::sstring encode(const model::topic_id_partition&);
    ss::sstring encode() const { return encode(tidp); }

    model::topic_id_partition tidp;
};

struct object_row_key {
    static std::optional<object_row_key> decode(std::string_view);
    static ss::sstring encode(const object_id&);
    ss::sstring encode() const { return encode(oid); }

    object_id oid;
};

} // namespace cloud_topics::l1
