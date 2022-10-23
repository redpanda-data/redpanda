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

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace security {

using credential_user = named_type<ss::sstring, struct credential_user_type>;
using credential_password
  = named_type<ss::sstring, struct credential_password_type>;

} // namespace security
