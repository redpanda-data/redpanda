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

#include "base/outcome.h"
#include "json/pointer.h"
#include "security/config.h"
#include "security/mtls_rule.h"

#include <expected>
#include <string_view>

namespace security::oidc {

class principal_mapping_rule {
public:
    principal_mapping_rule() = default;

    explicit principal_mapping_rule(json::Pointer&& claim, tls::rule mapping)
      : _claim{}
      , _mapping{std::move(mapping)} {
        swap(_claim, claim);
    }

    const json::Pointer& claim() const { return _claim; }
    const tls::rule& mapping() const { return _mapping; }

private:
    json::Pointer _claim{"/sub"};
    tls::rule _mapping;
};

result<principal_mapping_rule> parse_principal_mapping_rule(std::string_view);

class group_claim_policy {
public:
    group_claim_policy() = default;

    group_claim_policy(
      json::Pointer&& group_pointer, nested_group_behavior nested_behavior)
      : _group_pointer{}
      , _nested_behavior(nested_behavior) {
        swap(_group_pointer, group_pointer);
    }

    const json::Pointer& group_pointer() const { return _group_pointer; }
    nested_group_behavior nested_behavior() const { return _nested_behavior; }

private:
    json::Pointer _group_pointer{"/groups"};
    nested_group_behavior _nested_behavior{nested_group_behavior::none};
};

/// \brief Parses the path and nested group behavior into a policy
///
/// \result The formed policy or an error
std::expected<group_claim_policy, std::error_code>
parse_group_claim_path(const ss::sstring&, nested_group_behavior);

} // namespace security::oidc
