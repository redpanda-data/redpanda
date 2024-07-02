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
#include "bytes/iobuf_parser.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/json/types.h"
#include "utils/base64.h"

#include <seastar/core/loop.hh>

#include <optional>

namespace pandaproxy::json {

template<>
class rjson_parse_impl<iobuf> {
public:
    explicit rjson_parse_impl(serialization_format fmt)
      : _fmt(fmt) {}

    std::pair<bool, std::optional<iobuf>> operator()(std::string_view v) {
        switch (_fmt) {
        case serialization_format::none:
            [[fallthrough]];
        case serialization_format::binary_v2:
            return decode_base64(v);
        case serialization_format::unsupported:
            [[fallthrough]];
        default:
            return {false, std::nullopt};
        }
    }

    inline std::pair<bool, std::optional<iobuf>>
    decode_base64(std::string_view v) {
        try {
            auto decoded = base64_to_bytes(v);
            return {true, bytes_to_iobuf(decoded)};
        } catch (const base64_decoder_exception&) {
            return {false, std::nullopt};
        }
    };

private:
    serialization_format _fmt;
};

template<>
class rjson_serialize_impl<iobuf> {
public:
    explicit rjson_serialize_impl(serialization_format fmt)
      : _fmt(fmt) {}

    template<typename Buffer>
    bool operator()(::json::Writer<Buffer>& w, iobuf buf) {
        switch (_fmt) {
        case serialization_format::none:
            [[fallthrough]];
        case serialization_format::binary_v2:
            return encode_base64(w, std::move(buf));
        case serialization_format::json_v2:
            return encode_json(w, std::move(buf));
        case serialization_format::unsupported:
            [[fallthrough]];
        default:
            return false;
        }
    }

    template<typename Buffer>
    bool encode_base64(::json::Writer<Buffer>& w, iobuf buf) {
        if (buf.empty()) {
            return w.Null();
        }
        // TODO Ben: Implement custom OutputStream to prevent this linearization
        return w.String(iobuf_to_base64(buf));
    };

    template<typename Buffer>
    bool encode_json(::json::Writer<Buffer>& w, iobuf buf) {
        if (buf.empty()) {
            return w.Null();
        }
        iobuf_parser p{std::move(buf)};
        auto str = p.read_string(p.bytes_left());
        static_assert(str.padding(), "StringStream requires null termination");
        ::json::Reader reader;
        ::json::StringStream ss{str.c_str()};
        return reader.Parse(ss, w);
    };

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
