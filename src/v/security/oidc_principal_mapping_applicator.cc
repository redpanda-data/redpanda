/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/oidc_principal_mapping_applicator.h"

#include "security/logger.h"

#include <rapidjson/pointer.h>

namespace security::oidc {

result<acl_principal> principal_mapping_rule_apply(
  const principal_mapping_rule& mapping, const jwt& jwt) {
    auto claim = jwt.claim(mapping.claim());
    if (claim.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    auto principal = mapping.mapping().apply(claim.value());
    if (principal.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    return {principal_type::user, std::move(principal).value()};
}

} // namespace security::oidc
