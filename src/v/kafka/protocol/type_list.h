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

namespace kafka {

/// A compile-time list of types, used to enumerate Kafka request and handler
/// types (e.g. for building dispatch/flex tables).
template<typename... Ts>
struct type_list {};

} // namespace kafka
