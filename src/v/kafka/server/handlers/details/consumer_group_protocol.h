/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "features/fwd.h"

namespace kafka::details {

/// \brief Whether the KIP-848 consumer group protocol (api keys 68/69) is
/// available on this cluster. Always false while the protocol is incomplete.
///
/// TODO(kip-848): replace the body with the `features::feature_table` check
/// and the operator enable config, once both exist.
inline bool consumer_group_protocol_enabled(const features::feature_table&) {
    return false;
}

} // namespace kafka::details
