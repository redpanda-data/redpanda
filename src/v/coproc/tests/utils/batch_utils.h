/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record_batch_reader.h"

/// \brief Returns the number of individual records in an entire batch readers
/// result
std::size_t num_records(const model::record_batch_reader::data_t&);

/// \brief Makes a batch with the exact number of records specified, across a
/// random number of record_batches
model::record_batch_reader make_random_batch(std::size_t n_records);

model::record_batch_reader::data_t
copy_batch(const model::record_batch_reader::data_t&);
