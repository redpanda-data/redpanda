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
#include "io/persistence.h"

#include "base/units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>

#include <span>

namespace {
/*
 * build an alignment error to return from read/write
 */
seastar::future<size_t> make_alignment_error(
  std::string_view op_name,
  std::string_view arg,
  auto val,
  uint64_t alignment) {
    auto msg = fmt::format(
      R"(Op "{}" arg "{}" with value {} expects alignment {})",
      op_name,
      arg,
      val,
      alignment);
    return seastar::make_exception_future<size_t>(
      std::system_error(std::error_code(EINVAL, std::system_category()), msg));
}

/*
 * helper for checking alignment of read/write
 */
std::optional<seastar::future<size_t>> check_alignment(
  std::string_view name,
  uint64_t pos,
  std::span<const char> data,
  uint64_t memory_alignment,
  uint64_t disk_alignment) {
    if ((pos % disk_alignment) != 0) {
        return make_alignment_error(name, "pos", pos, disk_alignment);
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (auto ptr = reinterpret_cast<std::uintptr_t>(data.data());
        ptr % memory_alignment) {
        return make_alignment_error(name, "buf", ptr, memory_alignment);
    }

    if ((data.size() % disk_alignment) != 0) {
        return make_alignment_error(name, "len", data.size(), disk_alignment);
    }

    return std::nullopt;
}
} // namespace

namespace experimental::io {

void persistence::file::fail_next_read(std::exception_ptr eptr) noexcept {
    read_ex_ = std::move(eptr);
}

void persistence::file::fail_next_write(std::exception_ptr eptr) noexcept {
    write_ex_ = std::move(eptr);
}

void persistence::file::fail_next_close(std::exception_ptr eptr) noexcept {
    close_ex_ = std::move(eptr);
}

seastar::future<> persistence::file::maybe_fail_read() {
    if (read_ex_) [[unlikely]] {
        return seastar::make_exception_future(std::exchange(read_ex_, {}));
    }
    return seastar::make_ready_future<>();
}

seastar::future<> persistence::file::maybe_fail_write() {
    if (write_ex_) [[unlikely]] {
        return seastar::make_exception_future(std::exchange(write_ex_, {}));
    }
    return seastar::make_ready_future<>();
}

seastar::future<> persistence::file::maybe_fail_close() {
    if (close_ex_) [[unlikely]] {
        return seastar::make_exception_future(std::exchange(close_ex_, {}));
    }
    return seastar::make_ready_future<>();
}

void persistence::fail_next_create(std::exception_ptr eptr) noexcept {
    create_ex_ = std::move(eptr);
}

void persistence::fail_next_open(std::exception_ptr eptr) noexcept {
    open_ex_ = std::move(eptr);
}

seastar::future<> persistence::maybe_fail_create() {
    if (create_ex_) [[unlikely]] {
        return seastar::make_exception_future(std::exchange(create_ex_, {}));
    }
    return seastar::make_ready_future<>();
}

seastar::future<> persistence::maybe_fail_open() {
    if (open_ex_) [[unlikely]] {
        return seastar::make_exception_future(std::exchange(open_ex_, {}));
    }
    return seastar::make_ready_future<>();
}

disk_persistence::disk_file::disk_file(seastar::file file)
  : file_(std::move(file)) {}

seastar::future<size_t> disk_persistence::disk_file::dma_read(
  uint64_t pos, char* buf, size_t len) noexcept {
    return maybe_fail_read().then(
      [this, pos, buf, len] { return file_.dma_read(pos, buf, len); });
}

seastar::future<size_t> disk_persistence::disk_file::dma_write(
  uint64_t pos, const char* buf, size_t len) noexcept {
    return maybe_fail_write().then(
      [this, pos, buf, len] { return file_.dma_write(pos, buf, len); });
}

seastar::future<> disk_persistence::disk_file::close() noexcept {
    return maybe_fail_close().then([this] { return file_.close(); });
}

uint64_t disk_persistence::disk_file::disk_read_dma_alignment() const noexcept {
    return file_.disk_read_dma_alignment();
}

uint64_t
disk_persistence::disk_file::disk_write_dma_alignment() const noexcept {
    return file_.disk_write_dma_alignment();
}

uint64_t disk_persistence::disk_file::memory_dma_alignment() const noexcept {
    return file_.memory_dma_alignment();
}

seastar::temporary_buffer<char>
disk_persistence::allocate(uint64_t alignment, size_t size) noexcept {
    return seastar::temporary_buffer<char>::aligned(alignment, size);
}

seastar::future<seastar::shared_ptr<persistence::file>>
disk_persistence::create(std::filesystem::path path) noexcept {
    return maybe_fail_create().then([path = std::move(path)] {
        using of = seastar::open_flags;
        const auto flags = of::create | of::exclusive | of::rw;
        return seastar::open_file_dma(path.string(), flags).then([](auto file) {
            return seastar::make_ready_future<
              seastar::shared_ptr<persistence::file>>(
              seastar::make_shared<disk_file>(std::move(file)));
        });
    });
}

seastar::future<seastar::shared_ptr<persistence::file>>
disk_persistence::open(std::filesystem::path path) noexcept {
    return maybe_fail_open().then([path = std::move(path)] {
        const auto flags = seastar::open_flags::rw;
        return seastar::open_file_dma(path.string(), flags).then([](auto file) {
            return seastar::make_ready_future<
              seastar::shared_ptr<persistence::file>>(
              seastar::make_shared<disk_file>(std::move(file)));
        });
    });
}

memory_persistence::memory_persistence()
  : memory_persistence(
      {default_alignment, default_alignment, default_alignment}) {}

memory_persistence::memory_persistence(config config)
  : disk_read_dma_alignment_(config.disk_read_dma_alignment)
  , disk_write_dma_alignment_(config.disk_write_dma_alignment)
  , memory_dma_alignment_(config.memory_dma_alignment) {}

/*
 * The temporary buffer alignment helper used here is based on posix_memalign,
 * which appears to allow the size to be less than alignment. However, the
 * Seastar implementation of posix_memalign is apparently not compliant, and
 * will a non-aligned (and no error) pointer in some cases where the size is not
 * a multiple of alignment (much closer to aligned_alloc() behavior). To
 * accomodate this difference the memory file system transparently rounds up the
 * request to avoid this case when using Seastar's allocator in release mode.
 *
 * This only matters for the in-memory file system because Seastar's file system
 * interface already has stricter requirements that avoid this case implicitly.
 * We want to be able to use the in-memory file system to get weird with
 * alignments and test edge cases.
 */
seastar::temporary_buffer<char>
memory_persistence::allocate(uint64_t alignment, size_t size) noexcept {
    auto ret = seastar::temporary_buffer<char>::aligned(
      alignment, seastar::align_up(size, alignment));
    ret.trim(size);
    return ret;
}

seastar::future<seastar::shared_ptr<persistence::file>>
memory_persistence::create(std::filesystem::path path) noexcept {
    return maybe_fail_create().then([this, path = std::move(path)] {
        auto ret = files_.try_emplace(
          path, seastar::make_shared<memory_file>(this));
        if (!ret.second) {
            return seastar::make_exception_future<
              seastar::shared_ptr<persistence::file>>(
              std::filesystem::filesystem_error(
                "File exists",
                path,
                std::error_code(EEXIST, std::system_category())));
        }
        return seastar::make_ready_future<
          seastar::shared_ptr<persistence::file>>(ret.first->second);
    });
}

seastar::future<seastar::shared_ptr<persistence::file>>
memory_persistence::open(std::filesystem::path path) noexcept {
    return maybe_fail_open().then([this, path = std::move(path)] {
        auto it = files_.find(path);
        if (it == files_.end()) {
            return seastar::make_exception_future<
              seastar::shared_ptr<persistence::file>>(
              std::filesystem::filesystem_error(
                "File does not exist",
                path,
                std::error_code(ENOENT, std::system_category())));
        }
        return seastar::make_ready_future<
          seastar::shared_ptr<persistence::file>>(it->second);
    });
}

memory_persistence::memory_file::memory_file(memory_persistence* persistence)
  : persistence_(persistence) {}

seastar::future<size_t> memory_persistence::memory_file::dma_read(
  uint64_t pos, char* buf, size_t len) noexcept {
    return maybe_fail_read().then([this, pos, buf, len] {
        if (auto err = check_alignment(
              "dma_read",
              pos,
              {buf, len},
              memory_dma_alignment(),
              disk_read_dma_alignment());
            err.has_value()) {
            return std::move(err.value());
        }
        return read(pos, buf, len);
    });
}

seastar::future<size_t> memory_persistence::memory_file::dma_write(
  uint64_t pos, const char* buf, size_t len) noexcept {
    return maybe_fail_write().then([this, pos, buf, len] {
        if (auto err = check_alignment(
              "dma_write",
              pos,
              {buf, len},
              memory_dma_alignment(),
              disk_write_dma_alignment());
            err.has_value()) {
            return std::move(err.value());
        }
        return write(pos, buf, len).then([this, pos](size_t written) {
            size_ = std::max(size_, pos + written);
            return written;
        });
    });
}

seastar::future<> memory_persistence::memory_file::close() noexcept {
    return maybe_fail_close();
}

uint64_t
memory_persistence::memory_file::disk_read_dma_alignment() const noexcept {
    return persistence_->disk_read_dma_alignment_;
}

uint64_t
memory_persistence::memory_file::disk_write_dma_alignment() const noexcept {
    return persistence_->disk_write_dma_alignment_;
}

uint64_t
memory_persistence::memory_file::memory_dma_alignment() const noexcept {
    return persistence_->memory_dma_alignment_;
}

seastar::future<>
memory_persistence::memory_file::ensure_capacity(size_t size) {
    // futurized to avoid reactor stalls for large capacity expansions
    return seastar::do_until(
      [this, size] { return capacity_ >= size; },
      [this] {
          data_.emplace(capacity_, chunk_size);
          capacity_ += chunk_size;
          return seastar::make_ready_future<>();
      });
}

seastar::future<size_t>
memory_persistence::memory_file::read(uint64_t pos, char* buffer, size_t len) {
    if (pos >= size_) {
        co_return 0;
    }

    std::span output(buffer, len);

    auto it = data_.upper_bound(pos);
    // first chunk that starts strictly after pos ensures that begin() will
    // never be returned since zero is the smallest possible offset.
    it = std::prev(it);

    size_t bytes_read = 0;
    len = std::min(len, size_ - pos);
    while (len != 0) {
        assert(it != data_.end());

        auto chunk_pos = pos - it->first;
        auto chunk_len = std::min(len, chunk_size - chunk_pos);

        std::span chunk(it->second.get(), it->second.size());
        auto src = chunk.subspan(chunk_pos);
        auto dst = output.subspan(bytes_read);
        std::copy_n(src.begin(), chunk_len, dst.begin());

        bytes_read += chunk_len;
        pos += chunk_len;
        len -= chunk_len;

        ++it;

        co_await seastar::maybe_yield();
    }

    co_return bytes_read;
}

seastar::future<size_t> memory_persistence::memory_file::write(
  uint64_t pos, const char* buf, size_t len) {
    if (len == 0) {
        // don't expand capacity with a zero-length write at @pos
        co_return 0;
    }
    co_await ensure_capacity(pos + len);

    auto it = data_.upper_bound(pos);
    // first chunk that starts strictly after pos ensures that begin() will
    // never be returned since zero is the smallest possible offset.
    it = std::prev(it);

    size_t written = 0;
    std::span input(buf, len);
    while (len != 0) {
        assert(it != data_.end());

        auto chunk_pos = pos - it->first;
        auto chunk_len = std::min(len, chunk_size - chunk_pos);

        std::span chunk(it->second.get_write(), it->second.size());
        auto src = input.subspan(written);
        auto dst = chunk.subspan(chunk_pos);
        std::copy_n(src.begin(), chunk_len, dst.begin());

        written += chunk_len;
        pos += chunk_len;
        len -= chunk_len;

        ++it;

        co_await seastar::maybe_yield();
    }

    co_return written;
}

} // namespace experimental::io
