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

#include "encryption/crypto_utils.h"
#include "encryption/field_transformer.h"

namespace encryption {

/// Reference (full-copy) field transformer implementation.
///
/// For each schema format:
///  - Avro: parse -> walk tree -> encrypt tagged fields -> re-serialize
///  - Protobuf: parse -> walk tree -> encrypt tagged fields -> re-serialize
///  - JSON: streaming parse/re-emit, encrypting matched fields inline
class ref_field_transformer final : public field_transformer {
public:
    ss::future<iobuf> transform(
      iobuf value,
      const encryption_schema& schema,
      const dek_set& deks) override;
};

} // namespace encryption
