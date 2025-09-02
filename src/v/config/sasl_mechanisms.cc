// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/sasl_mechanisms.h"

#include "config/configuration.h"

#include <algorithm>

namespace config {

bool is_enterprise_sasl_mechanism(const ss::sstring& sasl_mech) {
    return std::ranges::contains(enterprise_sasl_mechanisms, sasl_mech);
}

bool has_sasl_mechanism(const std::string_view sasl_mech) {
    return std::ranges::contains(
      config::shard_local_cfg().sasl_mechanisms(), sasl_mech);
}

} // namespace config
