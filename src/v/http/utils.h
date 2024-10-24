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
#include "bytes/iobuf.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_map.h>

namespace http {

using uri_encode_slash = ss::bool_class<struct uri_encode_slash_t>;
ss::sstring uri_encode(std::string_view input, uri_encode_slash encode_slash);

iobuf form_encode_data(
  const absl::flat_hash_map<ss::sstring, ss::sstring>& data);

} // namespace http
