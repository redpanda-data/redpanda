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

#include <seastar/core/loop.hh>

#include <libbase64.h>
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
        std::vector<char> decoded(v.length());
        size_t out_len{};
        if (
          0
          == base64_decode(v.data(), v.length(), decoded.data(), &out_len, 0)) {
            return {false, std::nullopt};
        }
        auto buf = std::make_optional<iobuf>();
        buf->append(decoded.data(), out_len);
        return {true, std::move(buf)};
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

        base64_state state{};
        base64_stream_encode_init(&state, 0);
        // Required length is ceil(4n/3) rounded up to 4 bytes
        size_t out_len = (((4 * buf.size_bytes()) / 3) + 3) & ~0x3U;
        // TODO Ben: Implement custom OutputStream to prevent this linearization
        ss::sstring out(ss::sstring::initialized_later{}, out_len);
        auto out_ptr = out.data();
        iobuf::iterator_consumer(buf.cbegin(), buf.cend())
          .consume(
            buf.size_bytes(),
            [&state, &out_ptr, &out_len](const char* src, size_t sz) {
                base64_stream_encode(&state, src, sz, out_ptr, &out_len);
                out_ptr += out_len;
                return ss::stop_iteration::no;
            });
        base64_stream_encode_final(&state, out_ptr, &out_len);
        out_ptr += out_len;
        size_t written_len = std::distance(out.data(), out_ptr);
        vassert(written_len <= out.size(), "buffer overrun in encode_base64");
        return w.String(out.data(), std::distance(out.data(), out_ptr));
    };

private:
    serialization_format _fmt;
};

} // namespace pandaproxy::json
