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

#include <seastar/util/bool_class.hh>

#include <iosfwd>

namespace crypto {
enum class digest_type { MD5, SHA256, SHA512 };
std::ostream& operator<<(std::ostream&, digest_type);

enum class key_type { RSA };
std::ostream& operator<<(std::ostream&, key_type);

enum class format_type { PEM, DER };
std::ostream& operator<<(std::ostream&, format_type);

using is_private_key_t = ss::bool_class<struct is_private_key_tag>;
} // namespace crypto
