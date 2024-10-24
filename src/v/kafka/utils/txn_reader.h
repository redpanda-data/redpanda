/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "model/record_batch_reader.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>

namespace storage {
class offset_translator_state;
}

namespace kafka {

class partition_proxy;

// An interface for computing what transactions are aborted within a given range
// of a partition.
class aborted_transaction_tracker {
public:
    aborted_transaction_tracker() = default;
    aborted_transaction_tracker(const aborted_transaction_tracker&) = delete;
    aborted_transaction_tracker(aborted_transaction_tracker&&) = delete;
    aborted_transaction_tracker& operator=(const aborted_transaction_tracker&)
      = delete;
    aborted_transaction_tracker& operator=(aborted_transaction_tracker&&)
      = delete;
    virtual ~aborted_transaction_tracker() = default;

    // Create the default tracker that uses a partition proxy and the
    // offset_translator state.
    static std::unique_ptr<aborted_transaction_tracker> create_default(
      kafka::partition_proxy*,
      ss::lw_shared_ptr<const storage::offset_translator_state>);

    // Compute the ranges of transactions that are aborted for a given range
    // within the log.
    virtual ss::future<std::vector<model::tx_range>>
    compute_aborted_transactions(model::offset base, model::offset max) = 0;
};

// A record_batch_reader that filters out aborted transactions (as well as
// control batches). The wrapped reader will produce a stream of batches that
// are committed and do not have control batches.
//
// This reader streams results until it finds a control or transactional batch.
// At this point, results are buffered until the reader reaches the end of the
// stream, at which point we determine the aborted transactions + control
// batches within that range and filter them out.
//
// The stream optimization is to minimize latency unless transactions are used.
class read_committed_reader : public model::record_batch_reader::impl {
public:
    read_committed_reader(
      std::unique_ptr<aborted_transaction_tracker>, model::record_batch_reader);

    bool is_end_of_stream() const override;

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override;

    void print(std::ostream& os) override;

private:
    std::unique_ptr<aborted_transaction_tracker> _tracker;
    std::unique_ptr<model::record_batch_reader::impl> _underlying;
};

} // namespace kafka
