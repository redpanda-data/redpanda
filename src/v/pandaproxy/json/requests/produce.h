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

#include "bytes/iobuf.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "json/types.h"
#include "json/writer.h"
#include "kafka/client/types.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"
#include "tristate.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

template<typename Encoding = ::json::UTF8<>>
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

    using json_writer = ::json::Writer<::json::StringBuffer>;

    // If we're parsing json_v2, and the field is key or value (implied by
    // _json_writer being set), then forward calls to json_writer.
    // Return json parsing success, or no value if the above is not true.
    template<typename MemFunc, typename... Args>
    tristate<bool> maybe_json(MemFunc mem_func, Args&&... args) {
        if (_fmt != serialization_format::json_v2 || !_json_writer) {
            return tristate<bool>();
        }
        auto res = std::invoke(
          mem_func, *_json_writer, std::forward<Args>(args)...);
        if (_json_writer->IsComplete()) {
            iobuf buf;
            buf.append(_buf.GetString(), _buf.GetSize());
            switch (state) {
            case state::key:
                result.back().key.emplace(std::move(buf));
                break;
            case state::value:
                result.back().value.emplace(std::move(buf));
                break;
            default:
                return tristate<bool>(false);
            }
            _buf.Clear();
            _json_writer.reset();
            state = state::record;
        }
        return tristate<bool>(res);
    }

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = std::vector<kafka::client::record_essence>;
    rjson_parse_result result;

    explicit produce_request_handler(serialization_format fmt)
      : _fmt(fmt) {}

    bool Null() {
        if (auto res = maybe_json(&json_writer::Null);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }
    bool Bool(bool b) {
        if (auto res = maybe_json(&json_writer::Bool, b);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }
    bool Int64(int64_t v) {
        if (auto res = maybe_json(&json_writer::Int64, v);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }
    bool Uint64(uint64_t v) {
        if (auto res = maybe_json(&json_writer::Uint64, v);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }
    bool Double(double v) {
        if (auto res = maybe_json(&json_writer::Double, v);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }
    bool RawNumber(const Ch* str, ::json::SizeType len, bool b) {
        if (auto res = maybe_json(&json_writer::RawNumber, str, len, b);
            res.has_optional_value()) {
            return res.value();
        }
        return false;
    }

    bool Int(int i) {
        if (auto res = maybe_json(&json_writer::Int, i);
            res.has_optional_value()) {
            return res.value();
        }
        if (state == state::partition) {
            result.back().partition_id = model::partition_id(i);
            state = state::record;
            return true;
        }
        return false;
    }

    bool Uint(unsigned u) {
        if (auto res = maybe_json(&json_writer::Uint, u);
            res.has_optional_value()) {
            return res.value();
        }
        if (state == state::partition) {
            result.back().partition_id = model::partition_id(u);
            state = state::record;
            return true;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool b) {
        if (auto res = maybe_json<bool (json_writer::*)(
              const Ch*, ::json::SizeType, bool)>(
              &json_writer::String, str, len, b);
            res.has_optional_value()) {
            return res.value();
        }
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

    bool Key(const Ch* str, ::json::SizeType len, bool b) {
        if (auto res = maybe_json<bool (json_writer::*)(
              const Ch*, ::json::SizeType, bool)>(
              &json_writer::Key, str, len, b);
            res.has_optional_value()) {
            return res.value();
        }
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
                if (_fmt == serialization_format::json_v2) {
                    _json_writer.emplace(_buf);
                }
            } else if (key == "value") {
                state = state::value;
                if (_fmt == serialization_format::json_v2) {
                    _json_writer.emplace(_buf);
                }
            } else {
                return false;
            }
            return true;
        }
        return false;
    }

    bool StartObject() {
        if (auto res = maybe_json(&json_writer::StartObject);
            res.has_optional_value()) {
            return res.value();
        }
        if (state == state::empty) {
            return true;
        }
        if (state == state::records) {
            result.push_back(kafka::client::record_essence{});
            state = state::record;
            return true;
        }
        return false;
    }

    bool EndObject(::json::SizeType s) {
        if (auto res = maybe_json(&json_writer::EndObject, s);
            res.has_optional_value()) {
            return res.value();
        }
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

    bool StartArray() {
        if (auto res = maybe_json(&json_writer::StartArray);
            res.has_optional_value()) {
            return res.value();
        }
        return state == state::records;
    }

    bool EndArray(::json::SizeType s) {
        if (auto res = maybe_json(&json_writer::EndArray, s);
            res.has_optional_value()) {
            return res.value();
        }
        return state == state::records;
    }

private:
    ::json::StringBuffer _buf;
    std::optional<json_writer> _json_writer;
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
  const kafka::produce_response::partition& v) {
    w.StartObject();
    w.Key("partition");
    w.Int(v.partition_index);
    if (v.error_code != kafka::error_code::none) {
        w.Key("error_code");
        ::json::rjson_serialize(w, v.error_code);
    }
    w.Key("offset");
    w.Int64(v.base_offset);
    w.EndObject();
}

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
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
