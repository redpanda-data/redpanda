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
#include "base/seastarx.h"
#include "config/property.h"
#include "json/pointer.h"
#include "security/acl.h"
#include "security/fwd.h"
#include "security/mtls.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace security::oidc {

class principal_mapping_rule {
public:
    principal_mapping_rule() = default;

    explicit principal_mapping_rule(json::Pointer&& claim, tls::rule mapping)
      : _claim{}
      , _mapping{std::move(mapping)} {
        swap(_claim, claim);
    }

    result<acl_principal> apply(jwt const& jwt) const;

private:
    json::Pointer _claim{"/sub"};
    tls::rule _mapping;
};

result<principal_mapping_rule> parse_principal_mapping_rule(std::string_view);

} // namespace security::oidc
