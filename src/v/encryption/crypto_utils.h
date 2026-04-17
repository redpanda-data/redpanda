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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"

namespace encryption {

/// Encrypt a plaintext iobuf with AES-GCM (128 or 256 depending on key size).
/// Returns: [12-byte IV | ciphertext | 16-byte auth tag]
iobuf encrypt_field_value(bytes_view dek, iobuf plaintext);

/// Decrypt: parse [IV | ciphertext | tag], verify and decrypt.
iobuf decrypt_field_value(bytes_view dek, iobuf ciphertext);

} // namespace encryption
