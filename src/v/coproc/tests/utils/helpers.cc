/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/utils/helpers.h"

#include "storage/tests/utils/random_batch.h"

model::record_batch_reader single_record_record_batch_reader() {
    return model::make_generating_record_batch_reader([total = 1]() mutable {
        model::record_batch_reader::data_t batches;
        if (total--) {
            model::record_batch_reader::data_t reader;
            batches.push_back(
              storage::test::make_random_batch(model::offset(0), 1, false));
        }
        return ss::make_ready_future<model::record_batch_reader::data_t>(
          std::move(batches));
    });
}
