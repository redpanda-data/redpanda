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
#include <seastar/util/bool_class.hh>

namespace http {

using uri_encode_slash = ss::bool_class<struct uri_encode_slash_t>;
ss::sstring uri_encode(std::string_view input, uri_encode_slash encode_slash);

} // namespace http
