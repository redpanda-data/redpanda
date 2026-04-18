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

#include "kafka/data/dek_refilling_reader.h"

#include "encryption/dek_refill.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

namespace {

class dek_refilling_reader_impl final
  : public model::record_batch_reader::impl {
public:
    explicit dek_refilling_reader_impl(
      std::unique_ptr<model::record_batch_reader::impl> inner)
      : _inner(std::move(inner)) {}

    bool is_end_of_stream() const final { return _inner->is_end_of_stream(); }

    void print(std::ostream& os) final {
        fmt::print(os, "dek_refilling_reader wrapping: ");
        _inner->print(os);
    }

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point timeout) final {
        auto slice = co_await _inner->do_load_slice(timeout);

        auto& batches = std::get<model::record_batch_reader::data_t>(slice);
        model::record_batch_reader::data_t refilled;
        for (auto& batch : batches) {
            auto result = co_await encryption::refill_dek_sentinels(
              std::move(batch), std::nullopt);
            refilled.push_back(std::move(result.batch));
        }

        co_return model::record_batch_reader::storage_t{std::move(refilled)};
    }

    ss::future<> finally() noexcept final { return _inner->finally(); }

private:
    std::unique_ptr<model::record_batch_reader::impl> _inner;
};

} // namespace

model::record_batch_reader
make_dek_refilling_reader(model::record_batch_reader inner) {
    auto impl = std::make_unique<dek_refilling_reader_impl>(
      std::move(inner).release());
    return model::record_batch_reader(std::move(impl));
}

} // namespace kafka
