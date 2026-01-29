/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "security/acl.h"
#include "security/jwt.h"
#include "security/oidc_error.h"
#include "security/oidc_principal_mapping.h"

#include <expected>

namespace security::oidc {

result<acl_principal>
principal_mapping_rule_apply(const principal_mapping_rule&, const jwt& jwt);

std::expected<chunked_vector<acl_principal>, errc>
group_policy_apply(const group_claim_policy&, const jwt& jwt);

namespace detail {

std::expected<chunked_vector<std::string_view>, errc>
get_group_claim(const json::Pointer& p, const jwt& jwt);

acl_principal
apply_nested_group_policy(std::string_view user, nested_group_behavior b);

} // namespace detail

} // namespace security::oidc
