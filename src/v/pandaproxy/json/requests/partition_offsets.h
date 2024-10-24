/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "json/encodings.h"
#include "json/types.h"
#include "model/fundamental.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/types.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

struct topic_partition_offset {
    model::topic topic;
    model::partition_id partition;
    model::offset offset;
    bool operator<(const topic_partition_offset& other) const {
        return std::tie(topic, partition)
               < std::tie(other.topic, other.partition);
    };
};

template<typename Encoding = ::json::UTF8<>>
class partition_offsets_request_handler final : public base_handler<Encoding> {
private:
    enum class state {
        empty = 0,
        partitions,
        tpo,
        topic,
        partition,
        offset,
    };

    state state = state::empty;

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = std::vector<topic_partition_offset>;
    rjson_parse_result result;

    bool Int(int i) {
        switch (state) {
        case state::partition:
            result.back().partition = model::partition_id(i);
            state = state::tpo;
            return true;
        case state::offset:
            result.back().offset = model::offset(i);
            state = state::tpo;
            return true;
        default:
            return false;
        }
    }

    bool Uint(unsigned u) {
        switch (state) {
        case state::partition:
            result.back().partition = model::partition_id(u);
            state = state::tpo;
            return true;
        case state::offset:
            result.back().offset = model::offset(u);
            state = state::tpo;
            return true;
        default:
            return false;
        }
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        if (state == state::topic) {
            result.back().topic = model::topic(ss::sstring(str, len));
            state = state::tpo;
            return true;
        }
        return false;
    }

    bool Key(const char* str, ::json::SizeType len, bool) {
        auto key = std::string_view(str, len);
        if (state == state::empty && key == "partitions") {
            state = state::partitions;
            return true;
        }
        if (state == state::tpo) {
            if (key == "topic") {
                state = state::topic;
            } else if (key == "partition") {
                state = state::partition;
            } else if (key == "offset") {
                state = state::offset;
            } else {
                return false;
            }
            return true;
        }
        return false;
    }

    bool StartObject() {
        if (state == state::empty) {
            return true;
        }
        if (state == state::partitions) {
            result.emplace_back();
            state = state::tpo;
            return true;
        }
        return false;
    }

    bool EndObject(::json::SizeType size) {
        if (state == state::tpo) {
            state = state::partitions;
            return size == 3;
        }
        if (state == state::partitions) {
            state = state::empty;
            return true;
        }
        return false;
    }

    bool StartArray() { return state == state::partitions; }

    bool EndArray(::json::SizeType) { return state == state::partitions; }
};

} // namespace pandaproxy::json
