/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>

#include <optional>

/// Use this to prepare the storage layer with a desired pre-defined state
using log_layout_map = absl::btree_map<model::topic, size_t>;

/// Contains additional utilities for pushing and reading from underlying logs
/// within the storage api.
class coproc_test_fixture : public redpanda_thread_fixture {
public:
    using opt_reader_data_t = std::optional<model::record_batch_reader::data_t>;

    /// \brief Init the storage layer with the desired ntps
    virtual ss::future<> setup(log_layout_map);

    /// \brief Write records to storage::api
    ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader);

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    ss::future<opt_reader_data_t> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now() + 5s);
};
