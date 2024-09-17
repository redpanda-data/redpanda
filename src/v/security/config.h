/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <vector>

namespace security {

std::optional<ss::sstring>
validate_kerberos_mapping_rules(const std::vector<ss::sstring>& r) noexcept;

}

namespace security::tls {

std::optional<ss::sstring>
validate_rules(const std::optional<std::vector<ss::sstring>>& r) noexcept;

}

namespace security::oidc {

std::optional<ss::sstring>
validate_principal_mapping_rule(const ss::sstring& rule);

}
