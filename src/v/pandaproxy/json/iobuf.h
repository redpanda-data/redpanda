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
#include "bytes/iobuf_parser.h"
#include "json/json.h"
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

    bool operator()(rapidjson::Writer<rapidjson::StringBuffer>& w, iobuf buf) {
        switch (_fmt) {
        case serialization_format::none:
            [[fallthrough]];
        case serialization_format::binary_v2:
            return encode_base64(w, std::move(buf));
        case serialization_format::unsupported:
            [[fallthrough]];
        default:
            return false;
        }
    }

    bool
    encode_base64(rapidjson::Writer<rapidjson::StringBuffer>& w, iobuf buf) {
        if (buf.empty()) {
            return w.String("", 0);
        }
        // TODO Ben: Implement custom OutputStream to prevent this linearization
        return w.String(iobuf_to_base64(buf));
    };

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
