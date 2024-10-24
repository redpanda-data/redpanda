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
#include "base/units.h"
#include "io/page_cache.h"
#include "io/pager.h"
#include "io/paging_data_source.h"
#include "io/persistence.h"
#include "io/scheduler.h"
#include "io/tests/common.h"
#include "test_utils/test.h"
#include "utils/memory_data_source.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>

namespace io = experimental::io;

namespace {
/*
 * Variety of sizes useful in test configurations for hitting edge cases.
 */
std::vector<uint64_t> test_case_sizes() {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    return std::vector<uint64_t>{
      0,         1,          2,          3,         512 - 2,    512 - 1,
      512,       512 + 1,    512 + 2,    4_KiB - 2, 4_KiB - 1,  4_KiB,
      4_KiB + 1, 4_KiB + 2,  8_KiB - 2,  8_KiB - 1, 8_KiB,      8_KiB + 1,
      8_KiB + 2, 16_KiB - 2, 16_KiB - 1, 16_KiB,    16_KiB + 1, 16_KiB + 2,
    };
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
}
} // namespace

/*
 * Base pager test with a configured pager and its dependencies.
 */
class PagerTestBase : public StorageTest {
public:
    [[nodiscard]] virtual size_t open_files() { return 100; }

    void SetUp() override {
        StorageTest::SetUp();

        /*
         * the scheduler and pager do not create files.
         */
        add_cleanup_file(file_);
        storage()->create(file_.string()).get()->close().get();

        const io::page_cache::config cache_config{
          .cache_size = 2_MiB, .small_size = 1_MiB};
        cache_ = std::make_unique<io::page_cache>(cache_config);

        scheduler_ = std::make_unique<io::scheduler>(open_files());
        pager_ = std::make_unique<io::pager>(
          file_, 0, storage(), cache_.get(), scheduler_.get());
    }

    void TearDown() override {
        pager_->close().get();

        for (auto& file : cleanup_files_) {
            try {
                seastar::remove_file(file.string()).get();
            } catch (const std::exception& ex) {
                std::ignore = ex;
            }
        }
    }

    void add_cleanup_file(std::filesystem::path file) {
        cleanup_files_.push_back(std::move(file));
    }

    io::pager& pager() { return *pager_; }

private:
    std::filesystem::path file_{"foo"};
    std::unique_ptr<io::page_cache> cache_;
    std::unique_ptr<io::scheduler> scheduler_;
    std::unique_ptr<io::pager> pager_;
    std::vector<std::filesystem::path> cleanup_files_;
};

/*
 * Base pager test fixture that preallocates file with random data.
 */
class PreallocPagerTest : public PagerTestBase {
public:
    [[nodiscard]] virtual uint64_t file_size() const = 0;

    void SetUp() override {
        PagerTestBase::SetUp();
        fill_data = make_random_data(file_size()).get();
        pager().append(data()).get();
    }

    seastar::temporary_buffer<char> data() { return fill_data.share(); }

private:
    seastar::temporary_buffer<char> fill_data;
};

/*
 * Simple PagerTest is a PreallocPagerTest with required parameters.
 */
class PagerTest
  : public PreallocPagerTest
  , public ::testing::WithParamInterface<std::tuple<bool, uint64_t>> {
public:
    [[nodiscard]] bool disk_persistence() const override {
        return std::get<0>(GetParam());
    }

    [[nodiscard]] uint64_t file_size() const override {
        return std::get<1>(GetParam());
    }
};

/*
 * Pager test fixture with a test parameter for controlling append size.
 */
class AppendPagerTest
  : public PreallocPagerTest
  , public ::testing::WithParamInterface<std::tuple<bool, uint64_t, uint64_t>> {
public:
    [[nodiscard]] bool disk_persistence() const override {
        return std::get<0>(GetParam());
    }

    [[nodiscard]] uint64_t file_size() const override {
        return std::get<1>(GetParam());
    }

    static uint64_t append_size() { return std::get<2>(GetParam()); }
};

TEST_P(PagerTest, ZeroLengthReadReturnsNoPages) {
    /*
     * No matter the offset, zero length reads return no data
     */
    for (auto offset : boost::irange(file_size())) {
        const io::pager::read_config cfg{.offset = offset, .length = 0};
        EXPECT_TRUE(pager().read(cfg).get().empty());
    }
}

TEST_P(PagerTest, ReadFromPastEOFReturnsNoPages) {
    /*
     * Reads [0, ...) bytes at offset file_size + [0, ...)
     */
    for (auto delta : boost::irange(0, 3)) {
        for (auto length : boost::irange<size_t>(0, 3)) {
            const io::pager::read_config cfg{
              .offset = file_size() + delta, .length = length};
            EXPECT_TRUE(pager().read(cfg).get().empty());
        }
    }
}

TEST_P(PagerTest, Read) {
    /*
     * read from every offset
     */
    for (auto offset : boost::irange(file_size())) {
        /*
         * with a variety of lengths
         */
        std::vector<uint64_t> lengths{1, 2, 4096};
        lengths.push_back(file_size() - offset); // up to eof
        lengths.push_back(file_size() + 1);      // past eof
        std::sort(lengths.begin(), lengths.end());
        lengths.erase(
          std::unique(lengths.begin(), lengths.end()), lengths.end());

        for (const auto len : lengths) {
            auto pages = pager().read({offset, len}).get();

            // returns at least one page
            ASSERT_FALSE(pages.empty());

            // pages are non-empty
            EXPECT_TRUE(std::all_of(pages.begin(), pages.end(), [](auto page) {
                return !page->data().empty();
            }));

            // pages are adjacent, and offsets are in ascending order
            for (auto pit = pages.begin(), it = std::next(pit);
                 it != pages.end();
                 ++it, ++pit) {
                auto& prev = *pit;
                auto& curr = *it;
                EXPECT_EQ(prev->offset() + prev->data().size(), curr->offset());
            }

            /*
             * page range must start at or before the target offset. the result
             * should be minimal, meaning the first page should also contain the
             * target offset, not merely be ordered before the target. same for
             * the end of the range.
             */
            EXPECT_LE(pages.front()->offset(), offset);
            EXPECT_LT(
              offset, pages.front()->offset() + pages.front()->data().size());
            EXPECT_LT(pages.back()->offset(), offset + len);

            // no page should ever start after the end of the file
            EXPECT_LT(pages.back()->offset(), file_size());

            /*
             * the total bytes returned may exceed the read length in order to
             * cover the range (e.g. len=1 will still need to return a full
             * page). in this case it should only be the final page that
             * accounts for the excess.
             */
            const auto total = std::accumulate(
              pages.begin(), pages.end(), size_t{0}, [](size_t acc, auto page) {
                  return acc + page->data().size();
              });
            if (total > len) {
                EXPECT_LT(total - pages.back()->data().size(), len);
            }

            /*
             * the ending offset of the range formed by the intersection of the
             * eof, the requested range, and the pages returned from read().
             */
            const auto end_offset = std::min(
              std::min(file_size(), offset + len),
              pages.back()->offset() + pages.back()->data().size());

            // total requested data read (size of the intersection)
            const auto read_size = end_offset - offset;

            /*
             * an input stream over the intersection of the returned pages from
             * filemap::read and the requested [offset, len) range.
             */
            seastar::input_stream<char> input1(
              seastar::data_source(std::make_unique<io::paging_data_source>(
                pages, io::paging_data_source::config{offset, read_size})));

            /*
             * an input stream over a view of the input seed data constrained to
             * what was returned by pager::read.
             */
            seastar::input_stream<char> input2(
              seastar::data_source(std::make_unique<memory_data_source>(
                data().share(offset, read_size))));

            EXPECT_TRUE(EqualInputStreams(input1, input2));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  Pager,
  PagerTest,
  ::testing::Combine(
    ::testing::Bool(), ::testing::ValuesIn(test_case_sizes())));

TEST_P(AppendPagerTest, Append) {
    auto data_to_append = make_random_data(append_size()).get();

    // input stream over [0, file_size() + append_size() * count)
    auto pager_istream = [&](int count) {
        return seastar::input_stream<char>(
          seastar::data_source(std::make_unique<io::paging_data_source>(
            &pager(),
            io::paging_data_source::config{
              0, file_size() + (count * append_size())})));
    };

    // input stream over sequence of temporary buffers
    auto mem_istream = [](auto... tb) {
        std::vector<seastar::temporary_buffer<char>> bufs;
        (bufs.push_back(std::move(tb)), ...);
        return seastar::input_stream<char>(seastar::data_source(
          std::make_unique<memory_data_source>(std::move(bufs))));
    };

    // no appends
    {
        auto input1 = pager_istream(0);
        auto input2 = mem_istream(data().share());
        EXPECT_TRUE(EqualInputStreams(input1, input2));
    }

    // first append
    pager().append(data_to_append.share()).get();
    {
        auto input1 = pager_istream(1);
        auto input2 = mem_istream(data().share(), data_to_append.share());
        EXPECT_TRUE(EqualInputStreams(input1, input2));
    }

    // second append
    pager().append(data_to_append.share()).get();
    {
        auto input1 = pager_istream(2);
        auto input2 = mem_istream(
          data().share(), data_to_append.share(), data_to_append.share());
        EXPECT_TRUE(EqualInputStreams(input1, input2));
    }

    // third append
    pager().append(data_to_append.share()).get();
    {
        auto input1 = pager_istream(3);
        auto input2 = mem_istream(
          data().share(),
          data_to_append.share(),
          data_to_append.share(),
          data_to_append.share());
        EXPECT_TRUE(EqualInputStreams(input1, input2));
    }
}

INSTANTIATE_TEST_SUITE_P(
  Pager,
  AppendPagerTest,
  ::testing::Combine(
    ::testing::Bool(),
    ::testing::ValuesIn(test_case_sizes()),
    ::testing::ValuesIn(test_case_sizes())));
