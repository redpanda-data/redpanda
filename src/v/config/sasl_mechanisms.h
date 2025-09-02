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

#include <seastar/core/sstring.hh>

#include <array>
#include <string_view>

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

// Checks if `sasl_mech` is enabled in sasl_mechanisms config
bool has_sasl_mechanism(const std::string_view sasl_mech);

} // namespace config
