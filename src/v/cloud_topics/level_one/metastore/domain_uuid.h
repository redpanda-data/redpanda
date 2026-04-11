/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

namespace cloud_topics::l1 {

using domain_uuid = named_type<uuid_t, struct domain_uuid_tag>;

/// Cloud storage path prefix for a domain's LSM data.
inline ss::sstring domain_cloud_prefix(const domain_uuid& id) {
    return fmt::format("level_one/meta/domain/{}", id());
}

} // namespace cloud_topics::l1
