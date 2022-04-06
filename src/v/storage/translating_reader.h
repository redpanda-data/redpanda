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

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/offset_translator_state.h"

#include <seastar/core/shared_ptr.hh>

namespace storage {

/// A struct that holds together a reader that performs offset translation and
/// the corresponding offset translation state (useful when offsets of returned
/// batches need to be translated back to the original offset space)
struct translating_reader {
    model::record_batch_reader reader;
    ss::lw_shared_ptr<const offset_translator_state> ot_state;

    explicit translating_reader(
      model::record_batch_reader&& reader,
      ss::lw_shared_ptr<const offset_translator_state> ot_state = nullptr)
      : reader(std::move(reader))
      , ot_state(std::move(ot_state)) {}
};

} // namespace storage
