/*
 * Copyright 2020 Vectorized, Inc.
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
#include "json/json.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "pandaproxy/json/iobuf.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::json {

struct record {
    model::partition_id id;
    std::optional<iobuf> key;
    std::optional<iobuf> value;
};

template<typename Encoding = rapidjson::UTF8<>>
class produce_request_handler {
private:
    enum class state {
        empty = 0,
        records,
        record,
        partition,
        key,
        value,
    };

    serialization_format _fmt = serialization_format::none;
    state state = state::empty;

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = std::vector<pandaproxy::json::record>;
    rjson_parse_result result;

    explicit produce_request_handler(serialization_format fmt)
      : _fmt(fmt) {}

    bool Null() { return false; }
    bool Bool(bool) { return false; }
    bool Int64(int64_t) { return false; }
    bool Uint64(uint64_t) { return false; }
    bool Double(double) { return false; }
    bool RawNumber(const Ch*, rapidjson::SizeType, bool) { return false; }

    bool Int(int i) {
        if (state == state::partition) {
            result.back().id = model::partition_id(i);
            state = state::record;
            return true;
        }
        return false;
    }

    bool Uint(unsigned u) {
        if (state == state::partition) {
            result.back().id = model::partition_id(u);
            state = state::record;
            return true;
        }
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        if (state == state::key) {
            auto [res, buf] = rjson_parse_impl<iobuf>(_fmt)(
              std::string_view(str, len));
            if (res) {
                result.back().key = std::move(buf);
            }
            state = state::record;
            return res;
        } else if (state == state::value) {
            auto [res, buf] = rjson_parse_impl<iobuf>(_fmt)(
              std::string_view(str, len));
            if (res) {
                result.back().value = std::move(buf);
            }
            state = state::record;
            return res;
        }
        return false;
    }

    bool Key(const char* str, rapidjson::SizeType len, bool) {
        auto key = std::string_view(str, len);
        if (state == state::empty && key == "records") {
            state = state::records;
            return true;
        }
        if (state == state::record) {
            if (key == "partition") {
                state = state::partition;
            } else if (key == "key") {
                state = state::key;
            } else if (key == "value") {
                state = state::value;
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
        if (state == state::records) {
            result.push_back(pandaproxy::json::record{});
            state = state::record;
            return true;
        }
        return false;
    }

    bool EndObject(rapidjson::SizeType) {
        if (state == state::record) {
            state = state::records;
            return true;
        }
        if (state == state::records) {
            state = state::empty;
            return true;
        }
        return false;
    }

    bool StartArray() { return state == state::records; }

    bool EndArray(rapidjson::SizeType) { return state == state::records; }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const kafka::produce_response::partition& v) {
    w.StartObject();
    w.Key("partition");
    w.Int(v.id);
    if (v.error != kafka::error_code::none) {
        w.Key("error_code");
        ::json::rjson_serialize(w, v.error);
    }
    w.Key("offset");
    w.Int64(v.base_offset);
    w.EndObject();
}

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const kafka::produce_response::topic& v) {
    w.StartObject();
    w.Key("offsets");
    w.StartArray();
    for (auto const& p : v.partitions) {
        rjson_serialize(w, p);
    }
    w.EndArray();
    w.EndObject();
}

} // namespace pandaproxy::json
