// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <array>
#include <string_view>
#include <vector>

namespace config {

inline constexpr std::string_view gssapi{"GSSAPI"};
inline constexpr std::string_view scram{"SCRAM"};
inline constexpr std::string_view oauthbearer{"OAUTHBEARER"};
inline constexpr std::string_view plain{"PLAIN"};
inline constexpr auto supported_sasl_mechanisms
  = std::to_array<std::string_view>({gssapi, scram, oauthbearer, plain});

// Source of truth about which sasl mechanisms are enterprise values.
inline constexpr auto enterprise_sasl_mechanisms
  = std::to_array<std::string_view>({gssapi, oauthbearer});

// Checks if a mechanism is within the enterprise sasl mechanisms array
bool is_enterprise_sasl_mechanism(const ss::sstring& sasl_mech);

// Checks if `sasl_mech` is enabled in sasl_mechanisms config or any override
bool has_sasl_mechanism(const std::string_view sasl_mech);

// Checks if `sasl_mech` is enabled for a specific listener
bool has_sasl_mechanism(
  const std::string_view listener, const std::string_view sasl_mech);

// Gets supported sasl mechanisms for a specific listener
const std::vector<ss::sstring>&
get_sasl_mechanisms(const std::string_view listener);

struct sasl_mechanisms_override {
    ss::sstring listener;
    std::vector<ss::sstring> sasl_mechanisms;

    friend bool
    operator==(const sasl_mechanisms_override&, const sasl_mechanisms_override&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const sasl_mechanisms_override& ep);
};

// Checks if there are any enterprise sasl mechanisms in this override
bool is_enterprise_sasl_mechanisms_override(
  const sasl_mechanisms_override& overide);

namespace detail {

template<>
consteval std::string_view property_type_name<sasl_mechanisms_override>() {
    return "config::sasl_mechanisms_override";
}

} // namespace detail

} // namespace config

namespace YAML {

template<>
struct convert<config::sasl_mechanisms_override> {
    using type = config::sasl_mechanisms_override;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::sasl_mechanisms_override& smo);

}
