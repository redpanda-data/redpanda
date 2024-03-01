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

#include <iosfwd>

namespace crypto {
enum class digest_type { MD5, SHA256, SHA512 };
std::ostream& operator<<(std::ostream&, digest_type);
} // namespace crypto
