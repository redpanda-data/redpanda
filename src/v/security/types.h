/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace security {

using credential_user = named_type<ss::sstring, struct credential_user_type>;
using credential_password
  = named_type<ss::sstring, struct credential_password_type>;

using role_name = named_type<ss::sstring, struct role_name_type>;

static constexpr std::string_view default_role_name = "Users";
static const auto default_role = role_name{security::default_role_name};

} // namespace security
