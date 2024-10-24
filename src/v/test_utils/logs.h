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
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/api.h"

#include <seastar/core/thread.hh>

#include <exception>

using namespace std::chrono_literals; // NOLINT

namespace tests {

static inline ss::future<> persist_log_file(
  ss::sstring base_dir,
  model::ntp file_ntp,
  ss::circular_buffer<model::record_batch> batches) {
    return ss::async([base_dir = std::move(base_dir),
                      file_ntp = std::move(file_ntp),
                      batches = std::move(batches)]() mutable {
        ss::sharded<features::feature_table> feature_table;
        feature_table.start().get();
        feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();

        seastar::logger test_logger("test");

        ss::sharded<storage::api> storage;
        storage
          .start(
            [base_dir]() {
                return storage::kvstore_config(
                  1_MiB,
                  config::mock_binding(10ms),
                  base_dir,
                  storage::make_sanitized_file_config());
            },
            [base_dir]() {
                return storage::log_config(
                  base_dir,
                  1_GiB,
                  ss::default_priority_class(),
                  storage::make_sanitized_file_config());
            },
            std::ref(feature_table))
          .get();
        storage.invoke_on_all(&storage::api::start).get();

        auto& mgr = storage.local().log_mgr();
        try {
            mgr.manage(storage::ntp_config(file_ntp, mgr.config().base_dir))
              .then([b = std::move(batches)](
                      ss::shared_ptr<storage::log> log) mutable {
                  storage::log_append_config cfg{
                    storage::log_append_config::fsync::yes,
                    ss::default_priority_class(),
                    model::no_timeout};
                  auto reader = model::make_memory_record_batch_reader(
                    std::move(b));
                  return std::move(reader)
                    .for_each_ref(log->make_appender(cfg), cfg.timeout)
                    .then([log](storage::append_result) mutable {
                        return log->flush();
                    })
                    .finally([log] {});
              })
              .get();
            storage.stop().get();
            feature_table.stop().get();
        } catch (...) {
            storage.stop().get();
            feature_table.stop().get();
            throw;
        }
    });
}

struct to_vector_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        _batches.push_back(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    ss::circular_buffer<model::record_batch> end_of_stream() {
        return std::move(_batches);
    }

private:
    ss::circular_buffer<model::record_batch> _batches;
};

static inline ss::future<ss::circular_buffer<model::record_batch>>
read_log_file(ss::sstring base_dir, model::ntp file_ntp) {
    return ss::async([base_dir = std::move(base_dir),
                      file_ntp = std::move(file_ntp)]() mutable {
        ss::sharded<features::feature_table> feature_table;
        feature_table.start().get();
        feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();

        seastar::logger test_logger("test");

        ss::sharded<storage::api> storage;
        storage
          .start(
            [base_dir]() {
                return storage::kvstore_config(
                  1_MiB,
                  config::mock_binding(10ms),
                  base_dir,
                  storage::make_sanitized_file_config());
            },
            [base_dir]() {
                return storage::log_config(
                  base_dir,
                  1_GiB,
                  ss::default_priority_class(),
                  storage::make_sanitized_file_config());
            },
            std::ref(feature_table))
          .get();
        storage.invoke_on_all(&storage::api::start).get();
        auto& mgr = storage.local().log_mgr();
        try {
            auto batches
              = mgr.manage(storage::ntp_config(file_ntp, mgr.config().base_dir))
                  .then([](ss::shared_ptr<storage::log> log) mutable {
                      return log
                        ->make_reader(storage::log_reader_config(
                          model::offset(0),
                          model::model_limits<model::offset>::max(),
                          ss::default_priority_class()))
                        .then([](model::record_batch_reader reader) {
                            return std::move(reader).consume(
                              to_vector_consumer(), model::no_timeout);
                        });
                  })
                  .get();
            storage.stop().get();
            feature_table.stop().get();
            return batches;
        } catch (...) {
            storage.stop().get();
            feature_table.stop().get();
            throw;
        }
    });
}
} // namespace tests
