/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "json/reader.h"
#include "json/stringbuffer.h"
#include "json/types.h"
#include "json/writer.h"
#include "pandaproxy/json/rjson_parse.h"

struct latency_data {
    uint64_t job_id;
    uint64_t send_time;
    uint64_t rcv_time;
    ss::sstring data;
};

template<typename encoding = ::json::UTF8<>>
class latency_data_handler final
  : public pandaproxy::json::base_handler<encoding> {
private:
    enum class state { empty = 0, lat_data, job_id, send_time, rcv_time, data };

    state _state = state::empty;

public:
    using Ch = typename encoding::Ch;
    using rjson_parse_result = latency_data;
    rjson_parse_result result;

    bool String(const Ch* str, ::json::SizeType len, bool) {
        if (_state != state::data) {
            return false;
        }

        result.data = ss::sstring(str, len);
        _state = state::lat_data;
        return true;
    }

    bool Int(int i) {
        switch (_state) {
        case state::job_id:
            result.job_id = i;
            _state = state::lat_data;
            return true;
        case state::send_time:
            result.send_time = i;
            _state = state::lat_data;
            return true;
        case state::rcv_time:
            result.rcv_time = i;
            _state = state::lat_data;
            return true;
        default:
            return false;
        }
    }

    bool Int64(int64_t i) {
        switch (_state) {
        case state::job_id:
            result.job_id = i;
            _state = state::lat_data;
            return true;
        case state::send_time:
            result.send_time = i;
            _state = state::lat_data;
            return true;
        case state::rcv_time:
            result.rcv_time = i;
            _state = state::lat_data;
            return true;
        default:
            return false;
        }
    }

    bool Uint64(uint64_t u) {
        switch (_state) {
        case state::job_id:
            result.job_id = u;
            _state = state::lat_data;
            return true;
        case state::send_time:
            result.send_time = u;
            _state = state::lat_data;
            return true;
        case state::rcv_time:
            result.rcv_time = u;
            _state = state::lat_data;
            return true;
        default:
            return false;
        }
    }

    bool Uint(unsigned u) {
        switch (_state) {
        case state::job_id:
            result.job_id = u;
            _state = state::lat_data;
            return true;
        case state::send_time:
            result.send_time = u;
            _state = state::lat_data;
            return true;
        case state::rcv_time:
            result.rcv_time = u;
            _state = state::lat_data;
            return true;
        default:
            return false;
        }
    }

    bool Key(const char* str, ::json::SizeType len, bool) {
        auto key = std::string_view(str, len);

        if (_state == state::lat_data) {
            if (key == "job_id") {
                _state = state::job_id;
            } else if (key == "send_time") {
                _state = state::send_time;
            } else if (key == "rcv_time") {
                _state = state::rcv_time;
            } else if (key == "data") {
                _state = state::data;
            } else {
                return false;
            }
            return true;
        }

        return false;
    }

    bool StartObject() {
        if (_state == state::empty) {
            _state = state::lat_data;
            return true;
        }

        return false;
    }

    bool EndObject(::json::SizeType) {
        if (_state == state::lat_data) {
            _state = state::empty;
            return true;
        }
        return false;
    }

private:
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w, const latency_data& data) {
    w.StartObject();
    w.Key("job_id");
    w.Uint64(data.job_id);
    w.Key("send_time");
    w.Uint64(data.send_time);
    w.Key("rcv_time");
    w.Uint64(data.rcv_time);
    w.Key("data");
    w.String(data.data);
    w.EndObject();
}
