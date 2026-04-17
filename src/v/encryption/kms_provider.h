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

#include "base/seastarx.h"
#include "bytes/bytes.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace encryption {

/// \brief Abstract interface for Key Management Service providers.
///
/// Implementations wrap/unwrap Data Encryption Keys (DEKs) using a remote
/// or local key encryption key identified by kms_key_id.
class kms_provider {
public:
    kms_provider() = default;
    kms_provider(const kms_provider&) = delete;
    kms_provider& operator=(const kms_provider&) = delete;
    kms_provider(kms_provider&&) = default;
    kms_provider& operator=(kms_provider&&) = default;
    virtual ~kms_provider() noexcept = default;

    virtual ss::future<bytes>
    wrap_dek(ss::sstring kms_key_id, bytes plaintext_dek) = 0;

    virtual ss::future<bytes>
    unwrap_dek(ss::sstring kms_key_id, bytes encrypted_dek) = 0;
};

} // namespace encryption
