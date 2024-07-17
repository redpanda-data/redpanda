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

#include "config/throughput_control_group.h"

#include "config/convert.h"
#include "ssx/sformat.h"
#include "strings/utf8.h"
#include "utils/functional.h"
#include "utils/to_string.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <yaml-cpp/node/node.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>

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

constexpr char selector_prefix = '+';
constexpr std::string_view selector_empty = "empty";

static const ss::sstring noname(1, '\0');

struct copyable_RE2 : re2::RE2 {
    copyable_RE2(const copyable_RE2& other)
      : RE2(other.pattern()) {}
    copyable_RE2& operator=(const copyable_RE2&) = delete;
    copyable_RE2(copyable_RE2&&) noexcept = delete;
    copyable_RE2& operator=(copyable_RE2&&) noexcept = delete;
    explicit copyable_RE2(const std::string& s)
      : RE2(s) {}
    friend std::ostream& operator<<(std::ostream& os, const copyable_RE2& re) {
        fmt::print(os, "{}", re.pattern());
        return os;
    }
};

struct client_id_matcher_type {
    // nullopt stands for the empty client_id value
    std::optional<copyable_RE2> v;
    client_id_matcher_type() = default;
    explicit client_id_matcher_type(const copyable_RE2& d)
      : v(d) {}
    friend std::ostream& operator<<(
      std::ostream& os, const std::unique_ptr<client_id_matcher_type>& mt) {
        if (mt) {
            fmt::print(os, "{{v: {}}}", mt->v);
        } else {
            fmt::print(os, "null");
        }
        return os;
    }
};

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
  , client_id_matcher(
      other.client_id_matcher
        ? std::make_unique<client_id_matcher_type>(*other.client_id_matcher)
        : std::unique_ptr<client_id_matcher_type>{})
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
      tcg.is_noname() ? ""s : fmt::format("{{{}}}", tcg.name),
      tcg.client_id_matcher,
      tcg.throughput_limit_node_in_bps,
      tcg.throughput_limit_node_out_bps);
    return os;
}

bool throughput_control_group::match_client_id(
  const std::optional<std::string_view> client_id) const {
    if (!client_id_matcher) {
        // omitted match criterion means "always match"
        return true;
    }
    if (!client_id_matcher->v) {
        // empty client_id match
        // only missing client_id matches the empty
        return !client_id;
    }
    // regex match
    // missing client_id never matches a re
    return client_id
           && re2::RE2::FullMatch(
             re2::StringPiece(*client_id), *client_id_matcher->v);
}

bool throughput_control_group::is_noname() const noexcept {
    return name == noname;
}

ss::sstring throughput_control_group::validate() const {
    if (unlikely(name != noname && contains_control_character(name))) {
        return "Group name contains invalid character";
    }
    if (!client_id_matcher) {
        // omitted value always ok
        return {};
    }
    if (!client_id_matcher->v) {
        // empty match always ok
        return {};
    }
    // regex match: check if the regex is valid
    if (likely(client_id_matcher->v->ok())) {
        return {};
    } else {
        return ss::format(
          "Invalid client_id regex. {}", client_id_matcher->v->error());
    }
}

} // namespace config

namespace YAML {

Node convert<config::throughput_control_group>::encode(const type& tcg) {
    using namespace config;
    Node node;

    if (tcg.name != noname) {
        node[ids::name] = tcg.name;
    }

    if (tcg.client_id_matcher) {
        YAML::Node client_id_node = node[ids::client_id];
        if (!tcg.client_id_matcher->v) {
            // the empty match
            client_id_node = fmt::format(
              "{}{}", selector_prefix, selector_empty);
        } else {
            // regex
            client_id_node = tcg.client_id_matcher->v->pattern();
        }
    }

    return node;
}

bool convert<config::throughput_control_group>::decode(
  const Node& node, type& tcg) {
    using namespace config;
    throughput_control_group res;

    if (const auto& n = node[ids::name]; n) {
        res.name = n.as<ss::sstring>();
        if (contains_control_characters(res.name)) {
            return false;
        }
    } else {
        res.name = noname;
    }

    if (const auto& n = node[ids::client_id]; n) {
        const auto s = n.as<std::string>();
        if (contains_control_characters(s)) {
            return false;
        }
        if (!s.empty() && s[0] == selector_prefix) {
            // an explicit selector
            const std::string_view selector_name(
              std::next(s.cbegin()), s.cend());
            if (selector_name == selector_empty) {
                res.client_id_matcher
                  = std::make_unique<client_id_matcher_type>();
            } else {
                return false;
            }
        } else {
            // a regex
            const copyable_RE2 re{s};
            if (!re.ok()) {
                return false;
            }
            res.client_id_matcher = std::make_unique<client_id_matcher_type>(
              re);
        }
    } else {
        // nothing
        res.client_id_matcher.reset();
    }

    if (const auto& n = node[ids::tp_limit_node_in]; n) {
        // only the no-limit option is supported yet
        return false;
    } else {
        res.throughput_limit_node_in_bps = std::nullopt;
    }

    if (const auto& n = node[ids::tp_limit_node_out]; n) {
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
    using namespace config;
    w.StartObject();

    if (tcg.name != noname) {
        w.Key(ids::name);
        w.String(tcg.name);
    }

    if (tcg.client_id_matcher) {
        w.Key(ids::client_id);
        if (!tcg.client_id_matcher->v) {
            // empty match
            w.String(fmt::format("{}{}", selector_prefix, selector_empty));
        } else {
            // regex match
            w.String(tcg.client_id_matcher->v->pattern());
        }
    }

    w.EndObject();
}

} // namespace json
