// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/sstring.hh>

namespace iceberg {
struct storage_credentials {
    ss::sstring prefix;
    chunked_hash_map<ss::sstring, ss::sstring> config;
};

} // namespace iceberg
