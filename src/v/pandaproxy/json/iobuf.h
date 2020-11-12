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
#include "pandaproxy/json/types.h"
#include "pandaproxy/types.h"

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

} // namespace pandaproxy::json
