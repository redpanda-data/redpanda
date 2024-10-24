// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <cinttypes>

namespace serde {

#if defined(SERDE_TEST)
using serde_size_t = std::uint16_t;
#else
using serde_size_t = std::uint32_t;
#endif

} // namespace serde
