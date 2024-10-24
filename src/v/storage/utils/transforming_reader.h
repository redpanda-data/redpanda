/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "model/record.h"
#include "model/record_batch_reader.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <concepts>
#include <optional>
namespace storage {
template<typename T>
concept batch_transformation = requires(T t, model::record_batch batch) {
    {
        t(std::move(batch))
    } -> std::same_as<ss::future<std::optional<model::record_batch>>>;
};

template<typename Transformation>
requires batch_transformation<Transformation>
model::record_batch_reader make_transforming_reader(
  model::record_batch_reader&& source, Transformation&& t) {
    using storage_t = model::record_batch_reader::storage_t;
    using data_t = model::record_batch_reader::data_t;
    using foreign_t = model::record_batch_reader::foreign_data_t;

    class transforming_reader final : public model::record_batch_reader::impl {
    public:
        transforming_reader(
          std::unique_ptr<model::record_batch_reader::impl> src,
          Transformation&& transformation)
          : _ptr(std::move(src))
          , _transformation(std::forward<Transformation>(transformation)) {}

        transforming_reader(const transforming_reader&) = delete;
        transforming_reader& operator=(const transforming_reader&) = delete;
        transforming_reader(transforming_reader&&) = delete;
        transforming_reader& operator=(transforming_reader&&) = delete;
        ~transforming_reader() override = default;

        bool is_end_of_stream() const final { return _ptr->is_end_of_stream(); }

        void print(std::ostream& os) final { _ptr->print(os); }

        ss::future<storage_t>
        do_load_slice(model::timeout_clock::time_point t) final {
            return _ptr->do_load_slice(t).then([this](storage_t source_slice) {
                return transform_slice(std::move(source_slice));
            });
        }

    private:
        ss::future<storage_t> transform_slice(storage_t source_slice) {
            data_t result;
            for (auto& batch : get_batches(source_slice)) {
                auto opt_batch = co_await transform(std::move(batch));
                if (opt_batch) {
                    result.push_back(std::move(*opt_batch));
                }
            }

            co_return make_slice(source_slice, std::move(result));
        }

        data_t& get_batches(storage_t& st) {
            if (std::holds_alternative<data_t>(st)) {
                return std::get<data_t>(st);
            } else {
                return *std::get<foreign_t>(st).buffer;
            }
        }

        storage_t make_slice(const storage_t& source, data_t new_data) {
            if (std::holds_alternative<data_t>(source)) {
                return new_data;
            } else {
                return foreign_t{
                  .buffer = ss::make_foreign(
                    std::make_unique<data_t>(std::move(new_data)))};
            }
        }

        ss::future<std::optional<model::record_batch>>
        transform(model::record_batch b) {
            return _transformation(std::move(b));
        }

        std::unique_ptr<model::record_batch_reader::impl> _ptr;
        Transformation _transformation;
    };

    auto reader = std::make_unique<transforming_reader>(
      std::move(source).release(), std::forward<Transformation>(t));

    return model::record_batch_reader(std::move(reader));
}
} // namespace storage
