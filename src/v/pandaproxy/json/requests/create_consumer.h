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
#include "json/types.h"
#include "json/writer.h"
#include "kafka/protocol/join_group.h"
#include "pandaproxy/json/rjson_parse.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <string_view>

namespace pandaproxy::json {

struct create_consumer_request {
    kafka::member_id name{kafka::no_member};
    ss::sstring format{"binary"};
    ss::sstring auto_offset_reset{"earliest"};
    ss::sstring auto_commit_enable{"false"};
    ss::sstring fetch_min_bytes;
    ss::sstring consumer_request_timeout_ms;
};

template<typename Encoding = ::json::UTF8<>>
class create_consumer_request_handler final : public base_handler<Encoding> {
private:
    enum class state {
        empty = 0,
        name,
        format,
        auto_offset_reset,
        auto_commit_enable,
        fetch_min_bytes,
        timeout_ms
    };

    state _state = state::empty;

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = create_consumer_request;
    rjson_parse_result result;

    bool String(const Ch* str, ::json::SizeType len, bool) {
        switch (_state) {
        case state::empty:
            return false;
        case state::name:
            result.name = kafka::member_id{ss::sstring{str, len}};
            break;
        case state::format:
            result.format = {str, len};
            break;
        case state::auto_offset_reset:
            result.auto_offset_reset = {str, len};
            break;
        case state::auto_commit_enable:
            result.auto_commit_enable = {str, len};
            break;
        case state::fetch_min_bytes:
            result.fetch_min_bytes = {str, len};
            break;
        case state::timeout_ms:
            result.consumer_request_timeout_ms = {str, len};
            break;
        }
        return true;
    }

    bool Key(const char* str, ::json::SizeType len, bool) {
        _state = string_switch<state>({str, len})
                   .match("name", state::name)
                   .match("format", state::format)
                   .match("auto.offset.reset", state::auto_offset_reset)
                   .match("auto.commit.enable", state::auto_commit_enable)
                   .match("fetch.min.bytes", state::fetch_min_bytes)
                   .match("consumer.request.timeout.ms", state::timeout_ms)
                   .default_match(state::empty);

        return _state != state::empty;
    }

    bool StartObject() { return _state == state::empty; }

    bool EndObject(::json::SizeType) { return _state != state::empty; }
};

struct create_consumer_response {
    kafka::member_id instance_id;
    ss::sstring base_uri;
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const create_consumer_response& res) {
    w.StartObject();
    w.Key("instance_id");
    w.String(res.instance_id());
    w.Key("base_uri");
    w.String(res.base_uri);
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class create_consumer_response_handler final : public base_handler<Encoding> {
private:
    enum class state { empty = 0, instance_id, base_uri };

    state _state = state::empty;

public:
    using Ch = typename Encoding::Ch;
    using rjson_parse_result = create_consumer_response;
    rjson_parse_result result;

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto str_view{std::string_view{str, len}};
        switch (_state) {
        case state::empty:
            return false;
        case state::instance_id:
            result.instance_id = kafka::member_id{ss::sstring{str_view}};
            break;
        case state::base_uri:
            result.base_uri = {str, len};
            break;
        }
        return true;
    }

    bool Key(const char* str, ::json::SizeType len, bool) {
        _state = string_switch<state>({str, len})
                   .match("instance_id", state::instance_id)
                   .match("base_uri", state::base_uri)
                   .default_match(state::empty);

        return _state != state::empty;
    }

    bool StartObject() { return _state == state::empty; }

    bool EndObject(::json::SizeType) { return _state != state::empty; }
};

} // namespace pandaproxy::json
