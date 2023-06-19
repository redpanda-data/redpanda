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
#include "security/acl.h"
#include "ssx/sformat.h"
#include "utils/functional.h"
#include "utils/to_string.h"
#include "utils/utf8.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <yaml-cpp/node/detail/iterator_fwd.h>
#include <yaml-cpp/node/node.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
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
constexpr const char* principals = "principals";
constexpr const char* user = "user";
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

    friend bool operator==(const copyable_RE2& lhs, const copyable_RE2& rhs) {
        return lhs.pattern() == rhs.pattern();
    }
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

    friend bool
    operator==(const client_id_matcher_type&, const client_id_matcher_type&)
      = default;
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
  , acl_principals(other.acl_principals)
  , throughput_limit_node_in_bps(other.throughput_limit_node_in_bps)
  , throughput_limit_node_out_bps(other.throughput_limit_node_out_bps) {}

throughput_control_group&
throughput_control_group::operator=(const throughput_control_group& other) {
    return *this = throughput_control_group(other);
}

bool operator==(
  const throughput_control_group& lhs, const throughput_control_group& rhs) {
    return lhs.name == rhs.name
           && (lhs.client_id_matcher == rhs.client_id_matcher
                 || (lhs.client_id_matcher && rhs.client_id_matcher
                     && *lhs.client_id_matcher == *rhs.client_id_matcher))
           && lhs.acl_principals == rhs.acl_principals
           && lhs.throughput_limit_node_in_bps == rhs.throughput_limit_node_in_bps
           && lhs.throughput_limit_node_out_bps == rhs.throughput_limit_node_out_bps;
}

std::ostream&
operator<<(std::ostream& os, const throughput_control_group& tcg) {
    fmt::print(
      os,
      "{{group_name: {}, client_id_matcher: {}, acl_principals: {}, "
      "throughput_limit_node_in_bps: {}, throughput_limit_node_out_bps: {}}}",
      tcg.is_noname() ? ""s : fmt::format("{{{}}}", tcg.name),
      tcg.client_id_matcher,
      tcg.acl_principals,
      tcg.throughput_limit_node_in_bps,
      tcg.throughput_limit_node_out_bps);
    return os;
}

bool throughput_control_group::match_client_id(
  const std::optional<std::string_view> client_id_to_match) const {
    if (!client_id_matcher) {
        // omitted match criterion means "always match"
        return true;
    }
    if (!client_id_matcher->v) {
        // empty client_id match
        // only missing client_id matches the empty
        return !client_id_to_match;
    }
    // regex match
    // missing client_id never matches a re
    return client_id_to_match
           && re2::RE2::FullMatch(
             re2::StringPiece(*client_id_to_match), *client_id_matcher->v);
}

bool throughput_control_group::match_acl_principal(
  const security::acl_principal* const principal_to_match) const {
    if (!acl_principals) {
        // omitted match criterion means "always match"
        return true;
    }
    if (!principal_to_match) {
        // no other way to match unauthenticated case
        return false;
    }
    return std::any_of(
      acl_principals->begin(),
      acl_principals->end(),
      [principal_to_match](const security::acl_principal& p) {
          return (p == *principal_to_match)
                 || (p.wildcard() && p.type() == principal_to_match->type());
      });
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

template<>
struct convert<security::acl_principal> {
    using type = security::acl_principal;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

Node convert<security::acl_principal>::encode(const type& principal) {
    using namespace config;
    Node node;

    switch (principal.type()) {
    case security::principal_type::user: {
        node[ids::user] = principal.name();
        break;
    }
    case security::principal_type::ephemeral_user: {
        // not supported yet, invalid config produced if it appears
        break;
    }
    }

    return node;
}

bool convert<security::acl_principal>::decode(
  const Node& node, type& principal) {
    using namespace config;

    // always a single element map
    if (node.IsNull() || !node.IsMap()) {
        return false;
    }
    if (node.size() != 1) {
        return false;
    }
    const detail::iterator_value& el = *node.begin();
    const auto el_name = el.first.as<ss::sstring>();
    auto el_value = el.second.as<ss::sstring>();
    if (
      contains_control_characters(el_name)
      || contains_control_characters(el_value)) {
        return false;
    }

    if (el_name == ids::user) {
        principal = security::acl_principal(
          security::principal_type::user, std::move(el_value));
        return true;
    }
    return false;
}

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

    if (tcg.acl_principals) {
        YAML::Node principals_node = node[ids::principals];
        for (const security::acl_principal& p : *tcg.acl_principals) {
            principals_node.push_back(
              convert<security::acl_principal>::encode(p));
        }
    }

    return node;
}

bool convert<config::throughput_control_group>::decode(
  const Node& node, type& tcg) {
    using namespace config;
    throughput_control_group res;

    // name
    if (const auto& n = node[ids::name]; n) {
        res.name = n.as<ss::sstring>();
        if (contains_control_characters(res.name)) {
            return false;
        }
    } else {
        res.name = noname;
    }

    // client_id
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

    // principals
    if (const auto& n = node[ids::principals]; n) {
        std::vector<security::acl_principal> res_principals;
        if (!n.IsNull()) {
            if (!n.IsSequence()) {
                return false;
            }
            res_principals.reserve(n.size());
            for (const_iterator i = n.begin(); i != n.end(); ++i) {
                if (!convert<security::acl_principal>::decode(
                      *i, res_principals.emplace_back())) {
                    return false;
                }
            }
        }
        res.acl_principals = std::move(res_principals);
    } else {
        res.acl_principals = std::nullopt;
    }

    // tp_limit_node_in/out
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
  const security::acl_principal& principal) {
    using namespace config;
    w.StartObject();

    switch (principal.type()) {
    case security::principal_type::user: {
        w.Key(ids::user);
        w.String(principal.name());
        break;
    }
    case security::principal_type::ephemeral_user: {
        // not supported yet, invalid config produced if it appears
        break;
    }
    }

    w.EndObject();
}

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

    if (tcg.acl_principals) {
        w.Key(ids::principals);
        if (tcg.acl_principals->empty()) {
            w.Null();
        } else {
            w.StartArray();
            for (const security::acl_principal& p : *tcg.acl_principals) {
                rjson_serialize(w, p);
            }
            w.EndArray(tcg.acl_principals->size());
        }
    }

    w.EndObject();
}

} // namespace json
