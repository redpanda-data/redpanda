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

#include "encryption/kms_provider.h"

namespace encryption {

/// \brief Mock KMS provider for deterministic testing.
///
/// Uses AES-256 key wrap (RFC 3394) with a fixed local wrapping key.
/// The kms_key_id parameter is accepted but ignored -- all keys use
/// the same local wrapping key. Wrapping is deterministic: the same
/// plaintext DEK always produces the same ciphertext.
class mock_kms_provider final : public kms_provider {
public:
    mock_kms_provider();

    ss::future<bytes>
    wrap_dek(ss::sstring kms_key_id, bytes plaintext_dek) override;

    ss::future<bytes>
    unwrap_dek(ss::sstring kms_key_id, bytes encrypted_dek) override;
};

} // namespace encryption
