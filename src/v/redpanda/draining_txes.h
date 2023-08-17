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

#include "cluster/tx_hash_ranges.h"
#include "json/encodings.h"
#include "json/types.h"
#include "kafka/protocol/types.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

template<typename Encoding = ::json::UTF8<>>
class mark_transactions_draining_request_handler final
  : public base_handler<Encoding> {
private:
    enum class state {
        empty = 0,
        repartitioning_id,
        transactional_ids,
        hash_ranges,
        hash_range,
        from,
        to,
    };

    state state = state::empty;

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = cluster::draining_txs;
    rjson_parse_result result;

    bool Int64(int64_t i) {
        if (state == state::repartitioning_id) {
            result.id = cluster::repartitioning_id(i);
            return true;
        }
        return false;
    }

    bool Int(int64_t i) {
        if (state == state::repartitioning_id) {
            result.id = cluster::repartitioning_id(i);
            return true;
        }
        return false;
    }

    bool Uint(unsigned i) {
        if (state == state::repartitioning_id) {
            result.id = cluster::repartitioning_id(i);
            return true;
        }
        if (state == state::from) {
            result.ranges.ranges.back().first = uint32_t(i);
            state = state::hash_range;
            return true;
        }
        if (state == state::to) {
            result.ranges.ranges.back().last = uint32_t(i);
            state = state::hash_range;
            return true;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        if (state == state::transactional_ids) {
            result.transactions.insert(
              kafka::transactional_id(ss::sstring(str, len)));
            return true;
        }
        return false;
    }

    bool Key(const char* str, ::json::SizeType len, bool) {
        auto key = std::string_view(str, len);
        if (state == state::empty && key == "repartitioning_id") {
            state = state::repartitioning_id;
            return true;
        }
        if (state == state::repartitioning_id && key == "transactional_ids") {
            state = state::transactional_ids;
            return true;
        }
        if (
          (state == state::repartitioning_id
           || state == state::transactional_ids)
          && key == "hash_ranges") {
            state = state::hash_ranges;
            return true;
        }
        if (state == state::hash_range) {
            if (key == "from") {
                state = state::from;
            } else if (key == "to") {
                state = state::to;
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
        if (state == state::hash_ranges) {
            result.ranges.ranges.push_back(cluster::tx_hash_range());
            state = state::hash_range;
            return true;
        }
        return false;
    }

    bool EndObject(::json::SizeType size) {
        if (state == state::hash_range) {
            state = state::hash_ranges;
            return size == 2;
        }
        if (state == state::hash_ranges || state == state::transactional_ids) {
            state = state::empty;
            return true;
        }
        return false;
    }

    bool StartArray() {
        return state == state::transactional_ids || state == state::hash_ranges;
    }

    bool EndArray(::json::SizeType) {
        return state == state::transactional_ids || state == state::hash_ranges;
    }
};

} // namespace pandaproxy::json
