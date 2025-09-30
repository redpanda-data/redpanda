/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/record.h"
namespace model::test {
/**
 * Simple helper class allowing creation of batch with arbitrary header and
 * record data.
 *
 * It is useful in tests where we want to create batches with invalid
 * headers/records size/crc etc.
 */
class raw_record_batch_factory {
public:
    static model::record_batch create_record_batch(
      model::record_batch_header header, iobuf records, bool compressed) {
        return {header, std::move(records), compressed};
    }
};
} // namespace model::test
