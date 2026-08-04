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
#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "container/chunked_vector.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace tests::manual_file {

/// An operation kind; also usable as a set of kinds via the bitwise
/// operators below.
enum class io_kind : uint8_t {
    none = 0,
    write = 1U << 0U,
    read = 1U << 1U,
    flush = 1U << 2U,
    truncate = 1U << 3U,
    allocate = 1U << 4U,
};

constexpr io_kind operator|(io_kind a, io_kind b) {
    return static_cast<io_kind>(std::to_underlying(a) | std::to_underlying(b));
}

constexpr io_kind operator&(io_kind a, io_kind b) {
    return static_cast<io_kind>(std::to_underlying(a) & std::to_underlying(b));
}

/// Every kind.
inline constexpr io_kind io_kind_all = io_kind::write | io_kind::read
                                       | io_kind::flush | io_kind::truncate
                                       | io_kind::allocate;

/// Complement within io_kind_all.
constexpr io_kind operator~(io_kind a) {
    return static_cast<io_kind>(
      std::to_underlying(io_kind_all) & ~std::to_underlying(a));
}

constexpr bool contains(io_kind set, io_kind k) {
    return (std::to_underlying(set) & std::to_underlying(k)) != 0;
}

/// Number of single io_kinds.
inline constexpr size_t io_kind_count = static_cast<size_t>(
  std::popcount(std::to_underlying(io_kind_all)));

/// Index of a single kind in per-kind arrays.
inline size_t kind_index(io_kind k) {
    vassert(
      std::has_single_bit(std::to_underlying(k)),
      "kind_index({:#b}) requires a single kind",
      std::to_underlying(k));
    return static_cast<size_t>(std::countr_zero(std::to_underlying(k)));
}

constexpr std::string_view to_string_view(io_kind k) {
    switch (k) {
    case io_kind::none:
        return "none";
    case io_kind::write:
        return "write";
    case io_kind::read:
        return "read";
    case io_kind::flush:
        return "flush";
    case io_kind::truncate:
        return "truncate";
    case io_kind::allocate:
        return "allocate";
    }
    return "unknown";
}

inline fmt::iterator format_to(io_kind k, fmt::iterator out) {
    if (k == io_kind::none || std::has_single_bit(std::to_underlying(k))) {
        return fmt::format_to(out, "{}", to_string_view(k));
    }
    std::string_view sep;
    for (size_t i = 0; i < io_kind_count; ++i) {
        const auto bit = static_cast<io_kind>(1U << i);
        if (contains(k, bit)) {
            out = fmt::format_to(out, "{}{}", sep, to_string_view(bit));
            sep = "|";
        }
    }
    return out;
}

/// One operation the caller has issued and the device has not finished.
struct pending_io {
    uint64_t id{0};
    io_kind kind{io_kind::none};
    uint64_t pos{0};
    size_t len{0};
    const char* src{nullptr};            // write
    char* dst{nullptr};                  // read
    ss::temporary_buffer<char> snapshot; // write/read: the buffer at call time
    ss::promise<size_t> done;            // bytes transferred; 0 for void ops
};

/// The device behind a file_impl: its content, parked I/O, and
/// counters. By default every operation parks as an unresolved promise until
/// the test completes it, so the test controls the completion order and can
/// interleave completions with further calls into the component under test --
/// the same relationship ss::manual_clock has to time. Alternatively, a test
/// that cares about only some operation kinds keeps those manual and
/// attaches a driver, which finishes every other operation as it
/// arrives, always from the reactor loop, never inline in the submitting
/// call.
///
/// With the driver running, driver-owned completions happen at times the
/// test does not choose. Like the late-observation rule on durable_span(),
/// this shapes what may be asserted: state that driver-owned operations
/// affect is only meaningful after awaiting a future that depends on them
/// (durable content only after the corresponding flush future resolves);
/// state that only manual operations affect may be asserted at any point.
///
/// The device outlives the ss::file_impl, which holds only a reference, so
/// its state stays inspectable after the file's owner is gone.
///
/// A test verifies content by walking views of it (volatile_span,
/// durable_span) and comparing against its own expectations; how the device
/// stores its content is not part of the interface.
///
/// Completing a write or a read asserts that the caller did not touch the
/// buffer while the operation was in flight -- the O_DIRECT DMA contract.
///
/// Errors and short transfers are never injected: every I/O failure path in
/// the storage layer is an intentional vassert, so an injected error is a
/// guaranteed abort that says nothing about the caller's logic.
class device {
public:
    explicit device(ss::logger& log)
      : _log(log) {}

    ~device() {
        vassert(
          !_driver_attached,
          "device destroyed while driven; await driver::stop() first");
        vassert(
          _pending.empty(),
          "device destroyed with {} parked operations",
          _pending.size());
    }

    device(const device&) = delete;
    device& operator=(const device&) = delete;

    bool closed{false};

    /// Operations issued so far, of any kind; also the next operation's id.
    uint64_t submitted() const { return _submitted; }

    /// Operations of \p kind issued over the device lifetime, completed or
    /// not.
    uint64_t submitted(io_kind k) const {
        return _submitted_by_kind[kind_index(k)];
    }

    /// Resolves once at least \p n operations of \p kind have been
    /// submitted. The count spans the device lifetime, so a wait for an
    /// operation that already arrived resolves immediately.
    ss::future<> wait_submitted(io_kind k, uint64_t n) {
        vlog(
          _log.debug,
          "{:>14} {} >= {} (currently {})",
          "wait_submitted",
          k,
          n,
          submitted(k));
        return _cv.wait([this, k, n] { return submitted(k) >= n; });
    }

    /// Size of the device's current content: every completed write applied,
    /// whether or not an fsync has covered it yet.
    size_t volatile_size() const { return _volatile_cache.size(); }

    /// Size of the content the device has made durable; only ever moves
    /// toward the current content.
    size_t durable_size() const { return _durable.size(); }

    /// A view of the device's current content starting at \p at, spanning as
    /// many bytes as are stored contiguously (at least one). Requires
    /// at < volatile_size(). The view is invalidated by any completion,
    /// including the driver's.
    std::string_view volatile_span(size_t at) const {
        return _volatile_cache.span_at(at);
    }

    /// Like volatile_span() over the durable content: what a completed fsync
    /// covered. A test observes a completed flush from a continuation that
    /// runs some time later, when further writes may already have landed. A
    /// durability claim must therefore stay true once it becomes true: "this
    /// content has been fsynced" qualifies; "no byte under [0, F) was
    /// written after the last fsync" does not, and re-checking it late
    /// reports failures that never happened. The view is invalidated by any
    /// completion, including the driver's.
    std::string_view durable_span(size_t at) const {
        return _durable.span_at(at);
    }

    size_t pending_count() const { return _pending.size(); }

    size_t pending_count(io_kind k) const {
        return std::ranges::count(_pending, k, &pending_io::kind);
    }

    /// The \p i-th pending operation, oldest first. The reference is
    /// invalidated by any completion, including the driver's.
    const pending_io& pending_at(size_t i) const { return _pending[i]; }

    /// Park \p io and return the future that completing it will resolve.
    ss::future<size_t> submit(pending_io io) {
        io.id = _submitted++;
        ++_submitted_by_kind[kind_index(io.kind)];
        if (io.kind == io_kind::write) {
            // _prev_head_write's contract: no two writes over the same file
            // range in flight at once.
            for (const auto& other : _pending) {
                vassert(
                  other.kind != io_kind::write
                    || io.pos >= other.pos + other.len
                    || other.pos >= io.pos + io.len,
                  "write {} [{}, {}) overlaps in-flight write {} [{}, {})",
                  io.id,
                  io.pos,
                  io.pos + io.len,
                  other.id,
                  other.pos,
                  other.pos + other.len);
            }
        }
        vlog(
          _log.debug,
          "{:>14} {} {} {} {}",
          "io_call",
          io.id,
          io.kind,
          io.pos,
          io.len);
        auto f = io.done.get_future();
        _pending.push_back(std::move(io));
        _cv.broadcast();
        return f;
    }

    /// Finish parked operation \p i (0 is the oldest): run the
    /// completion-time contract checks, apply its effect to the device's
    /// content, then resolve the promise. Nothing here suspends; the
    /// caller's continuations run at the next scheduling point. Not usable
    /// while the driver runs: it completes operations concurrently, making
    /// indexes into the pending queue unstable -- use
    /// complete_oldest(io_kind) instead.
    void complete(size_t i) {
        vassert(
          !_driver_attached,
          "complete({}) while a driver is attached; use "
          "complete_oldest(io_kind)",
          i);
        do_complete(i);
    }

    /// Finish the oldest pending operation of kind \p k. While the driver
    /// runs, this is the only completion the test may use, and only for
    /// kinds in its manual set -- the driver owns the rest.
    void complete_oldest(io_kind k) {
        vassert(
          !contains(_driven, k),
          "complete_oldest({}): the driver owns this kind",
          k);
        for (size_t i = 0; i < _pending.size(); ++i) {
            if (_pending[i].kind == k) {
                do_complete(i);
                return;
            }
        }
        for (const auto& io : _pending) {
            vlog(
              _log.error,
              "{:>14} {} {} {} {}",
              "pending",
              io.id,
              io.kind,
              io.pos,
              io.len);
        }
        vunreachable(
          "complete_oldest({}): no pending operation of this kind ({} other "
          "operations pending, see log)",
          k,
          _pending.size());
    }

    /// complete(0) up to \p n times, stopping early if nothing is parked.
    void complete_n(size_t n) {
        for (size_t i = 0; i < n && !_pending.empty(); ++i) {
            complete(0);
        }
    }

private:
    friend class driver;

    /// A file's content: a sparse map of fixed-size pages keyed by page
    /// number. A missing page reads as zeros, so fallocating a large range
    /// costs nothing until it is written.
    ///
    /// Invariant: in every present page, bytes above size() are zero, so
    /// shrinking only re-zeroes the tail of the last kept page.
    class image {
    public:
        static constexpr size_t page_size = 4096;

        /// Number of pages needed to hold \p bytes.
        static constexpr size_t page_count(size_t bytes) {
            return (bytes + page_size - 1) / page_size;
        }

        size_t size() const { return _size; }

        /// Logical file size change. Growing zero-fills, shrinking discards.
        void resize(size_t n) {
            if (n < _size) {
                const size_t want = page_count(n);
                _pages.erase(_pages.lower_bound(want), _pages.end());
                // Re-establish the invariant over the tail of the last kept
                // page.
                zero_span(n, want * page_size - n);
            }
            _size = n;
        }

        /// Overwrite [pos, pos + n), growing the file if the write runs
        /// past the end: a real O_DIRECT write past EOF extends the file.
        void write(size_t pos, const char* src, size_t n) {
            if (n == 0) {
                return;
            }
            for (size_t done = 0; done < n;) {
                const size_t at = pos + done;
                const size_t len = span_len(at, n - done);
                std::memcpy(
                  page(at / page_size) + at % page_size, src + done, len);
                done += len;
            }
            _size = std::max(_size, pos + n);
        }

        /// Zero [pos, min(pos + n, size())) without changing size(),
        /// matching fallocate(FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE).
        void zero(size_t pos, size_t n) {
            if (pos >= _size) {
                return;
            }
            zero_span(pos, std::min(n, _size - pos));
        }

        /// The longest contiguous run of content starting at \p at, clamped
        /// to size() and the containing page. Requires at < size().
        std::string_view span_at(size_t at) const {
            vassert(
              at < _size, "span_at({}) past the content end {}", at, _size);
            const size_t len = span_len(at, _size - at);
            if (auto it = _pages.find(at / page_size); it != _pages.end()) {
                return {it->second.data() + at % page_size, len};
            }
            return {zero_page.data() + at % page_size, len};
        }

        /// Copy [pos, pos + n) out, clamped to size(); returns bytes copied.
        size_t read(size_t pos, char* dst, size_t n) const {
            if (pos >= _size) {
                return 0;
            }
            n = std::min(n, _size - pos);
            for (size_t done = 0; done < n;) {
                const size_t at = pos + done;
                const size_t len = span_len(at, n - done);
                if (auto it = _pages.find(at / page_size); it != _pages.end()) {
                    std::memcpy(
                      dst + done, it->second.data() + at % page_size, len);
                } else {
                    std::memset(dst + done, 0, len);
                }
                done += len;
            }
            return n;
        }

        /// Make page \p p identical to \p src's. Requires
        /// p < page_count(src.size()) and src.size() <= size(), so the copy
        /// stays in bounds and cannot put a nonzero byte above size().
        void copy_page_from(const image& src, size_t p) {
            vassert(
              p < page_count(src._size) && src._size <= _size,
              "copy_page_from page {} src size {} dst size {}",
              p,
              src._size,
              _size);
            if (auto it = src._pages.find(p); it != src._pages.end()) {
                std::memcpy(page(p), it->second.data(), page_size);
            } else {
                _pages.erase(p);
            }
        }

    private:
        using page_type = std::array<char, page_size>;

        static inline const page_type zero_page{};

        /// Length of the page-bounded span starting at \p at, at most
        /// \p left.
        static constexpr size_t span_len(size_t at, size_t left) {
            return std::min(page_size - (at % page_size), left);
        }

        /// Page \p p's storage, materialized zero-filled on first use.
        char* page(size_t p) {
            return _pages.try_emplace(p).first->second.data();
        }

        void zero_span(size_t pos, size_t n) {
            for (size_t done = 0; done < n;) {
                const size_t at = pos + done;
                const size_t len = span_len(at, n - done);
                if (auto it = _pages.find(at / page_size); it != _pages.end()) {
                    std::memset(it->second.data() + at % page_size, 0, len);
                }
                done += len;
            }
        }

        std::map<size_t, page_type> _pages;
        size_t _size{0};
    };

    void do_complete(size_t i) {
        vassert(
          i < _pending.size(),
          "complete({}) with {} parked operations",
          i,
          _pending.size());
        pending_io io = std::move(_pending[i]);
        std::move(
          _pending.begin() + i + 1, _pending.end(), _pending.begin() + i);
        _pending.pop_back_n(1);

        if (io.kind == io_kind::write || io.kind == io_kind::read) {
            // The caller must not touch memory under an in-flight DMA.
            const char* buf = io.kind == io_kind::write ? io.src : io.dst;
            const char* diff
              = std::mismatch(buf, buf + io.len, io.snapshot.get()).first;
            vassert(
              diff == buf + io.len,
              "{} {} at pos {} len {}: buffer changed while the operation was "
              "in flight, first difference at offset {}",
              io.kind,
              io.id,
              io.pos,
              io.len,
              diff - buf);
        }

        size_t result = 0;
        switch (io.kind) {
        case io_kind::write:
            _volatile_cache.write(io.pos, io.src, io.len);
            mark_dirty(io.pos, io.len);
            result = io.len;
            break;
        case io_kind::read:
            result = _volatile_cache.read(io.pos, io.dst, io.len);
            break;
        case io_kind::flush:
            sync_durable();
            break;
        case io_kind::truncate:
            _volatile_cache.resize(io.len);
            _durable.resize(std::min(_durable.size(), io.len));
            break;
        case io_kind::allocate:
            // fallocate(FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE): zero
            // within the file, no size change.
            _volatile_cache.zero(io.pos, io.len);
            if (io.pos < _volatile_cache.size()) {
                mark_dirty(
                  io.pos, std::min(io.len, _volatile_cache.size() - io.pos));
            }
            break;
        case io_kind::none:
            vunreachable("operation {} with no kind", io.id);
        }

        vlog(
          _log.debug, "{:>14} {} {} {}", "io_complete", io.id, io.kind, result);
        io.done.set_value(result);
    }

    /// Record that [pos, pos + len) of `_volatile_cache` has moved ahead of
    /// `_durable`.
    void mark_dirty(uint64_t pos, size_t len) {
        const size_t last = image::page_count(pos + len);
        for (size_t p = pos / image::page_size; p < last; ++p) {
            _dirty_pages.push_back(p);
        }
    }

    /// An fsync makes everything written before it durable. Copy only the
    /// pages written since the previous fsync: tests fsync often, and copying
    /// all of `_volatile_cache` each time would dominate the run.
    void sync_durable() {
        if (_durable.size() < _volatile_cache.size()) {
            _durable.resize(_volatile_cache.size());
        }
        // A page rewritten between fsyncs appears once per write in
        // _dirty_pages; deduplicate so it is copied once.
        std::sort(_dirty_pages.begin(), _dirty_pages.end());
        const auto dedup_end = std::unique(
          _dirty_pages.begin(), _dirty_pages.end());
        for (auto it = _dirty_pages.begin(); it != dedup_end; ++it) {
            if (*it >= image::page_count(_volatile_cache.size())) {
                break;
            }
            _durable.copy_page_from(_volatile_cache, *it);
        }
        _dirty_pages.clear();
    }

    /// The device's current content: every completed write applied, whether
    /// or not an fsync has covered it yet.
    image _volatile_cache;

    /// The content the device has made durable: a completed fsync copies the
    /// pages written since the previous one out of `_volatile_cache`, so
    /// this only ever moves toward `_volatile_cache`.
    image _durable;

    chunked_vector<pending_io> _pending;
    chunked_vector<size_t> _dirty_pages;
    uint64_t _submitted{0};
    std::array<uint64_t, io_kind_count> _submitted_by_kind{};

    /// Kinds the attached driver owns; maintained by the driver, read by the
    /// completion guards. Empty when no driver is attached.
    io_kind _driven{io_kind::none};
    bool _driver_attached{false};

    ss::condition_variable _cv;
    ss::logger& _log;
};

/// Drives a device: a fiber that finishes pending operations of the
/// \p automated kinds as they arrive, one per scheduling point. Kinds
/// outside the set stay parked for the test to complete with
/// complete_oldest(io_kind); automate() grows the set mid-test.
/// Construction starts driving; stop() must complete before the driver or
/// the device is destroyed.
class driver {
public:
    driver(device& dev, io_kind automated)
      : _dev(dev) {
        vassert(!_dev._driver_attached, "the device is already driven");
        _dev._driver_attached = true;
        _dev._driven = automated;
        _run = drive();
    }

    ~driver() {
        vassert(
          !_run.has_value(),
          "driver destroyed while running; await stop() first");
    }

    driver(const driver&) = delete;
    driver& operator=(const driver&) = delete;

    /// Add \p kinds to the driven set: the driver finishes any pending
    /// operations of these kinds and every future one. Contracting the set
    /// would race with the driver; stop() and construct a new one instead.
    void automate(io_kind kinds) {
        _dev._driven = _dev._driven | kinds;
        _dev._cv.broadcast();
    }

    ss::future<> stop() {
        if (!_run.has_value()) {
            co_return;
        }
        _stop = true;
        _dev._cv.broadcast();
        co_await std::exchange(_run, std::nullopt).value();
        _dev._driven = io_kind::none;
        _dev._driver_attached = false;
    }

private:
    std::optional<size_t> next_driven() const {
        for (size_t i = 0; i < _dev._pending.size(); ++i) {
            if (contains(_dev._driven, _dev._pending[i].kind)) {
                return i;
            }
        }
        return std::nullopt;
    }

    ss::future<> drive() {
        while (true) {
            co_await _dev._cv.wait(
              [this] { return _stop || next_driven().has_value(); });
            if (_stop) {
                co_return;
            }
            // The wakeup and this resumption are separate tasks; the device's
            // guards on complete()/complete_oldest() keep driven operations
            // ours alone in between.
            const auto i = next_driven();
            vassert(i.has_value(), "driver woke up with nothing to finish");
            _dev.do_complete(*i);
            // Let the completion's continuations run -- and possibly submit
            // follow-up operations -- before finishing another one: one
            // completion per scheduling point.
            co_await ss::yield();
        }
    }

    device& _dev;
    bool _stop{false};
    std::optional<ss::future<>> _run;
};

/// An ss::file backed by a device: every operation the caller issues
/// becomes a pending_io there, and the test decides when each one finishes.
/// Every call checks ss::file's unconditional DMA preconditions (non-zero,
/// alignment-multiple pos, len and buffer address), so a component that would
/// fail on a real O_DIRECT file fails here too. The methods the storage layer
/// never calls abort, so a change in a component's file usage cannot silently
/// escape a test's device model.
class file_impl final : public ss::file_impl {
public:
    explicit file_impl(device& dev, uint32_t dma_alignment)
      : _dev(dev) {
        // Every knob is set explicitly so a change in seastar's ss::file_impl
        // defaults cannot silently weaken the checks below.
        _memory_dma_alignment = dma_alignment;
        _disk_read_dma_alignment = dma_alignment;
        _disk_write_dma_alignment = dma_alignment;
        _disk_overwrite_dma_alignment = dma_alignment;
        _read_max_length = 1U << 30;
        _write_max_length = 1U << 30;
    }

    ss::future<size_t> write_dma(
      uint64_t pos, const void* buffer, size_t len, ss::io_intent*) final {
        check_dma(io_kind::write, pos, buffer, len);
        const char* src = static_cast<const char*>(buffer);
        return _dev.submit(
          {.kind = io_kind::write,
           .pos = pos,
           .len = len,
           .src = src,
           .snapshot = ss::temporary_buffer<char>(src, len)});
    }

    ss::future<size_t>
    read_dma(uint64_t pos, void* buffer, size_t len, ss::io_intent*) final {
        check_dma(io_kind::read, pos, buffer, len);
        char* dst = static_cast<char*>(buffer);
        return _dev.submit(
          {.kind = io_kind::read,
           .pos = pos,
           .len = len,
           .dst = dst,
           .snapshot = ss::temporary_buffer<char>(dst, len)});
    }

    ss::future<> flush() final {
        vassert(!_dev.closed, "flush() after close()");
        return _dev.submit({.kind = io_kind::flush}).discard_result();
    }

    ss::future<> truncate(uint64_t len) final {
        vassert(!_dev.closed, "truncate({}) after close()", len);
        return _dev.submit({.kind = io_kind::truncate, .len = len})
          .discard_result();
    }

    ss::future<> allocate(uint64_t pos, uint64_t len) final {
        vassert(!_dev.closed, "allocate({}, {}) after close()", pos, len);
        return _dev.submit({.kind = io_kind::allocate, .pos = pos, .len = len})
          .discard_result();
    }

    /// Never parks: close() is the last call a component makes on a file, so
    /// parking it would reach no new state.
    ss::future<> close() final {
        vassert(!_dev.closed, "close() called twice");
        _dev.closed = true;
        return ss::now();
    }

    ss::future<uint64_t> size() final { abort_unexpected("size"); }

    ss::future<size_t>
    write_dma(uint64_t, std::vector<iovec>, ss::io_intent*) final {
        abort_unexpected("write_dma(iovec)");
    }

    ss::future<size_t>
    read_dma(uint64_t, std::vector<iovec>, ss::io_intent*) final {
        abort_unexpected("read_dma(iovec)");
    }

    ss::future<ss::temporary_buffer<uint8_t>>
    dma_read_bulk(uint64_t, size_t, ss::io_intent*) final {
        abort_unexpected("dma_read_bulk");
    }

    ss::future<struct stat> stat() final { abort_unexpected("stat"); }

    ss::future<> discard(uint64_t, uint64_t) final {
        abort_unexpected("discard");
    }

    ss::subscription<ss::directory_entry>
    list_directory(std::function<ss::future<>(ss::directory_entry)>) final {
        abort_unexpected("list_directory");
    }

private:
    [[noreturn]] static void abort_unexpected(const char* what) {
        vunreachable(
          "file_impl: unexpected {}(); the caller's file usage changed, "
          "extend the device model to cover it",
          what);
    }

    /// ss::file's unconditional preconditions on dma_write/dma_read: they
    /// hold for every caller of a real O_DIRECT file, so they cannot
    /// false-positive.
    void check_dma(
      io_kind kind, uint64_t pos, const void* buffer, size_t len) const {
        const bool is_write = kind == io_kind::write;
        const char* op = is_write ? "write_dma" : "read_dma";
        const size_t max_len = is_write ? _write_max_length : _read_max_length;
        const uint64_t disk_alignment = is_write ? _disk_write_dma_alignment
                                                 : _disk_read_dma_alignment;
        vassert(!_dev.closed, "{} at pos {} after close()", op, pos);
        vassert(len > 0, "{} with zero length at pos {}", op, pos);
        vassert(
          len <= max_len,
          "{} len {} over the device maximum {}",
          op,
          len,
          max_len);
        vassert(
          pos % disk_alignment == 0,
          "{} pos {} not a multiple of the disk alignment {}",
          op,
          pos,
          disk_alignment);
        vassert(
          len % disk_alignment == 0,
          "{} len {} not a multiple of the disk alignment {}",
          op,
          len,
          disk_alignment);
        const auto addr = std::bit_cast<uintptr_t>(buffer);
        vassert(
          addr % _memory_dma_alignment == 0,
          "{} buffer {:#x} not a multiple of the memory alignment {}",
          op,
          addr,
          _memory_dma_alignment);
    }

    device& _dev;
};

/// An ss::file backed by \p dev.
inline ss::file make_file(device& dev, uint32_t dma_alignment) {
    return ss::file(ss::make_shared<file_impl>(dev, dma_alignment));
}

} // namespace tests::manual_file
