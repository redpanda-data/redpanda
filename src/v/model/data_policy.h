/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/json.h"
#include "seastarx.h"
#include "serde/serde.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <type_traits>
#include <utility>

namespace model {

class data_policy_exeption final : public std::exception {
public:
    explicit data_policy_exeption(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

// TODO: add to manifest, test
struct data_policy {
    data_policy() = default;

    data_policy(const ss::sstring& s) {
        rapidjson::Document json_string;
        json_string.Parse(s);

        if (json_string.HasParseError()) {
            throw data_policy_exeption(
              fmt::format("Parse json error for string: {}", s));
        }

        function_name = get_string_field("function_name", json_string);
        script_name = get_string_field("script_name", json_string);
    }

    ss::sstring to_json() const {
        rapidjson::StringBuffer cfg_sb;
        rapidjson::Writer<rapidjson::StringBuffer> w(cfg_sb);

        w.StartObject();

        w.Key("function_name");
        json::rjson_serialize(w, function_name);

        w.Key("script_name");
        json::rjson_serialize(w, script_name);

        w.EndObject();

        return cfg_sb.GetString();
    }

    void serde_read(iobuf_parser& in, const serde::header&) {
        function_name = serde::read<ss::sstring>(in);
        script_name = serde::read<ss::sstring>(in);
    }

    void serde_write(iobuf& out) {
        serde::write(out, function_name);
        serde::write(out, script_name);
    }

    bool operator==(const data_policy& other) const {
        return function_name == other.function_name
               && script_name == other.script_name;
    }

    ss::sstring function_name;
    ss::sstring script_name;
    // More fields in future(acls, geo, ...)

private:
    static inline ss::sstring
    get_string_field(std::string_view key, rapidjson::Document& json) {
        if (!json.HasMember(key.data())) {
            throw data_policy_exeption(fmt::format("Can not find key {}", key));
        }
        if (!json[key.data()].IsString()) {
            throw data_policy_exeption(
              fmt::format("Value for key {} is not string", key));
        }
        return json[key.data()].GetString();
    }
};

inline std::ostream&
operator<<(std::ostream& os, const data_policy& datapolicy) {
    os << "function_name: " << datapolicy.function_name
       << " , script_name: " << datapolicy.script_name;
    return os;
}

inline std::istream& operator>>(std::istream& i, data_policy& datapolicy) {
    std::string s;
    std::getline(i, s);

    datapolicy = data_policy(s);
    return i;
}

} // namespace model
