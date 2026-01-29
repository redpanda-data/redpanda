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

#include "base/seastarx.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/tests/iterator_test_harness.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/builder.h"
#include "lsm/sst/reader.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {
template<lsm::compression_type CompressionType>
class sst_iterator_factory {
public:
    std::unique_ptr<lsm::internal::iterator>
    make_iterator(std::map<lsm::internal::key, iobuf> map) {
        size_t file_size = 0;
        auto filename = ++_counter;
        {
            auto file
              = _persistence->open_sequential_writer({.id = filename}).get();
            lsm::sst::builder::options opts;
            opts.compression = CompressionType;
            lsm::sst::builder builder(std::move(file), opts);
            for (auto& [key, value] : map) {
                builder.add(key, std::move(value)).get();
            }
            builder.finish().get();
            builder.close().get();
            file_size = builder.file_size();
        }
        auto file
          = _persistence->open_random_access_reader({.id = filename}).get();
        auto reader = lsm::sst::reader::open(
                        std::move(*file),
                        lsm::internal::file_id{_counter},
                        file_size,
                        ss::make_lw_shared<lsm::sst::block_cache>(1_MiB))
                        .get();
        auto it = reader.create_iterator();
        _readers.push_back(std::move(reader));
        return it;
    }

    void close() {
        for (auto& reader : _readers) {
            reader.close().get();
        }
        _persistence->close().get();
    }

private:
    lsm::internal::file_id _counter;
    std::vector<lsm::sst::reader> _readers;
    std::unique_ptr<lsm::io::data_persistence> _persistence
      = lsm::io::make_memory_data_persistence();
};
} // namespace

using SSTIteratorType = ::testing::Types<
  sst_iterator_factory<lsm::compression_type::none>,
  sst_iterator_factory<lsm::compression_type::zstd>>;

INSTANTIATE_TYPED_TEST_SUITE_P(
  SSTIteratorSuite, CoreIteratorTest, SSTIteratorType);
