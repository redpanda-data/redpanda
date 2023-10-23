/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "outcome.h"
#include "security/acl.h"
#include "security/fwd.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace security::oidc {

result<acl_principal> authenticate(
  jws const& jws,
  verifier const& verifier,
  std::string_view issuer,
  std::string_view audience,
  std::chrono::seconds clock_skew_tolerance,
  ss::lowres_system_clock::time_point now);

result<acl_principal> authenticate(
  jwt const& jwt,
  std::string_view issuer,
  std::string_view audience,
  std::chrono::seconds clock_skew_tolerance,
  ss::lowres_system_clock::time_point now);

} // namespace security::oidc
