// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "storage/mvlog/file.h"

#include "base/units.h"
#include "io/page_cache.h"
#include "io/pager.h"
#include "io/paging_data_source.h"
#include "io/persistence.h"
#include "io/scheduler.h"

namespace storage::experimental::mvlog {
file::~file() = default;

size_t file::size() const {
    vassert(pager_, "file has been closed");
    return pager_->size();
}

ss::input_stream<char> file::make_stream(uint64_t offset, uint64_t length) {
    vassert(pager_, "file has been closed");
    auto max_len = size() - offset;
    return ss::input_stream<char>(
      ss::data_source(std::make_unique<::experimental::io::paging_data_source>(
        pager_.get(),
        ::experimental::io::paging_data_source::config{
          offset, std::min(max_len, length)})));
}

ss::future<> file::append(iobuf buf) {
    vassert(pager_, "file has been closed");
    for (auto& io_frag : buf) {
        co_await pager_->append(std::move(io_frag).release());
    }
}

ss::future<> file::close() {
    vassert(pager_, "file has been closed");
    auto pager = std::move(pager_);
    // First stop IOs to the file.
    co_await pager->close();
    pager.reset();

    // Then close the file handle.
    co_await file_->close();
}

file::file(
  std::filesystem::path path,
  file_t f,
  std::unique_ptr<::experimental::io::pager> pager)
  : path_(std::move(path))
  , file_(std::move(f))
  , pager_(std::move(pager)) {}

file_manager::~file_manager() = default;

ss::future<std::unique_ptr<file>>
file_manager::open_file(std::filesystem::path path) {
    auto f = co_await storage_->open(path);
    auto pager = std::make_unique<::experimental::io::pager>(
      path, 0, storage_.get(), cache_.get(), scheduler_.get());
    co_return std::unique_ptr<file>{
      new file(std::move(path), std::move(f), std::move(pager))};
}

ss::future<std::unique_ptr<file>>
file_manager::create_file(std::filesystem::path path) {
    auto f = co_await storage_->create(path);
    auto pager = std::make_unique<::experimental::io::pager>(
      path, 0, storage_.get(), cache_.get(), scheduler_.get());
    co_return std::unique_ptr<file>{
      new file(std::move(path), std::move(f), std::move(pager))};
}

ss::future<> file_manager::remove_file(std::filesystem::path path) {
    co_return co_await ss::remove_file(path.string());
}

file_manager::file_manager(
  size_t cache_size_bytes,
  size_t small_queue_size_bytes,
  size_t scheduler_num_files)
  : storage_(std::make_unique<::experimental::io::disk_persistence>())
  , cache_(std::make_unique<::experimental::io::page_cache>(
      ::experimental::io::page_cache::config{
        .cache_size = cache_size_bytes, .small_size = small_queue_size_bytes}))
  , scheduler_(
      std::make_unique<::experimental::io::scheduler>(scheduler_num_files)) {}

} // namespace storage::experimental::mvlog
