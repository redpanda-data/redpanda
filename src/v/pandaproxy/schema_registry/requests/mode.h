/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/types.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

struct mode_req_rep {
    static constexpr std::string_view field_name = "mode";
    mode mode{mode::read_write};
};

template<typename Encoding = ::json::UTF8<>>
class mode_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        mode,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = mode_req_rep;
    rjson_parse_result result;

    explicit mode_handler()
      : json::base_handler<Encoding>{json::serialization_format::none}
      , result() {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::object && sv == mode_req_rep::field_name) {
            _state = state::mode;
            return true;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::mode) {
            auto s = from_string_view<mode>(sv);
            if (s.has_value() && s.value() != mode::import) {
                result.mode = *s;
                _state = state::object;
            } else {
                auto code = error_code::mode_invalid;
                throw as_exception(
                  error_info{code, make_error_code(code).message()});
            }
            return s.has_value();
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(::json::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::mode_req_rep& res) {
    w.StartObject();
    w.Key(mode_req_rep::field_name.data());
    ::json::rjson_serialize(w, to_string_view(res.mode));
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
