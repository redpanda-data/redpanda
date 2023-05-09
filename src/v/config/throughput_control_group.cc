/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "throughput_control_group.h"

#include "config/convert.h"
#include "ssx/sformat.h"
#include "utils/to_string.h"
#include "utils/utf8.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>

using namespace std::string_literals;

namespace {

template<typename T>
requires std::same_as<T, ss::sstring> || std::same_as<T, std::string>
bool contains_control_characters(const T& s) {
    const auto& ctype = std::use_facet<std::ctype<char>>(
      std::locale::classic());
    const char* const end = s.c_str() + s.length();
    return ctype.scan_is(std::ctype_base::cntrl, s.c_str(), end) != end;
}

} // namespace

namespace config {

namespace ids {
constexpr const char* name = "name";
constexpr const char* client_id = "client_id";
constexpr const char* tp_limit_node_in = "throughput_limit_node_in_bps";
constexpr const char* tp_limit_node_out = "throughput_limit_node_out_bps";
} // namespace ids

static const ss::sstring noname(1, '\0');

throughput_control_group::throughput_control_group() = default;
throughput_control_group::throughput_control_group(
  throughput_control_group&&) noexcept
  = default;
throughput_control_group&
throughput_control_group::operator=(throughput_control_group&&) noexcept
  = default;
throughput_control_group::~throughput_control_group() noexcept = default;

throughput_control_group::throughput_control_group(
  const throughput_control_group& other)
  : name(other.name)
  , client_id_re(
      other.client_id_re
        ? std::make_unique<re2::RE2>(other.client_id_re->pattern())
        : std::unique_ptr<re2::RE2>{})
  , throughput_limit_node_in_bps(other.throughput_limit_node_in_bps)
  , throughput_limit_node_out_bps(other.throughput_limit_node_out_bps) {}

throughput_control_group&
throughput_control_group::operator=(const throughput_control_group& other) {
    return *this = throughput_control_group(other);
}

std::ostream&
operator<<(std::ostream& os, const throughput_control_group& tcg) {
    fmt::print(
      os,
      "{{group_name: {}, client_id: {}, throughput_limit_node_in_bps: {}, "
      "throughput_limit_node_out_bps: {}}}",
      tcg.name,
      tcg.client_id_re ? tcg.client_id_re->pattern() : ""s,
      tcg.throughput_limit_node_in_bps,
      tcg.throughput_limit_node_out_bps);
    return os;
}

bool throughput_control_group::match_client_id(
  const ss::sstring& client_id) const {
    if (!client_id_re) {
        // omitted match criterion means "always match"
        return true;
    }
    return re2::RE2::FullMatch(
      re2::StringPiece(client_id.c_str(), client_id.length()), *client_id_re);
}

bool throughput_control_group::is_noname() const noexcept {
    return name == noname;
}

ss::sstring throughput_control_group::validate() const {
    if (unlikely(name != noname && contains_control_character(name))) {
        return "Group name contains invalid character";
    }
    if (unlikely(client_id_re && !client_id_re->ok())) {
        return ss::format("Invalid client_id regex. {}", client_id_re->error());
    }
    return {};
}

} // namespace config

namespace YAML {

Node convert<config::throughput_control_group>::encode(const type& tcg) {
    Node node;
    if (tcg.name != config::noname) {
        node[config::ids::name] = tcg.name;
    }
    if (tcg.client_id_re) {
        node[config::ids::client_id] = tcg.client_id_re->pattern();
    }
    return node;
}

bool convert<config::throughput_control_group>::decode(
  const Node& node, type& tcg) {
    config::throughput_control_group res;
    if (const auto& n = node[config::ids::name]; n) {
        res.name = n.as<ss::sstring>();
        if (contains_control_characters(res.name)) {
            return false;
        }
    } else {
        res.name = config::noname;
    }
    if (const auto& n = node[config::ids::client_id]; n) {
        const auto s = n.as<std::string>();
        if (contains_control_characters(s)) {
            return false;
        }
        res.client_id_re = std::make_unique<re2::RE2>(s);
        if (!res.client_id_re->ok()) {
            return false;
        }
    } else {
        res.client_id_re.reset();
    }
    if (const auto& n = node[config::ids::tp_limit_node_in]; n) {
        // only the no-limit option is supported yet
        return false;
    } else {
        res.throughput_limit_node_in_bps = std::nullopt;
    }
    if (const auto& n = node[config::ids::tp_limit_node_out]; n) {
        // only the no-limit option is supported yet
        return false;
    } else {
        res.throughput_limit_node_out_bps = std::nullopt;
    }

    tcg = std::move(res);
    return true;
}

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::throughput_control_group& tcg) {
    w.StartObject();
    if (tcg.name != config::noname) {
        w.Key(config::ids::name);
        w.String(tcg.name);
    }
    if (tcg.client_id_re) {
        w.Key(config::ids::client_id);
        w.String(tcg.client_id_re->pattern());
    }
    w.EndObject();
}

} // namespace json
