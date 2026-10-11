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
#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "config/property.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/future-util.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"
#include "test_utils/manual_file.h"
#include "test_utils/scoped_config.h"
#include "test_utils/seastar_fuzz.h"

#include <seastar/core/align.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace storage {

struct segment_appender_test_accessor {
    segment_appender& sa; // NOLINT(*-avoid-const-or-ref-data-members)

    bool inactive_timer_armed() const { return sa._inactive_timer.armed(); }

    /// cancel() first: append() arms the timer and arming an armed timer
    /// aborts. The handler may dispatch a background head write, so only call
    /// this between appender calls, never with an append() in flight.
    void fire_inactive_timer() {
        sa._inactive_timer.cancel();
        sa.handle_inactive_timer();
    }
};

} // namespace storage

namespace {

constexpr size_t prologue_size = 8;
constexpr size_t max_ops = 512;
constexpr size_t max_total_appended_bytes = 256 * 1024;
constexpr size_t max_outstanding_flushes = 32;
constexpr size_t teardown_yields = 8;

constexpr size_t dma_alignment = 4096;
static_assert(storage::internal::chunk_cache::alignment == dma_alignment);

/// 4 KiB multiples only; bounded_property's bounds are not enforced through
/// set_value(), so this table is the only guard.
constexpr std::array<size_t, 3> chunk_sizes{4096, 8192, 16384};

/// A limit of one chunk deadlocks: do_append's old_head local is the sole
/// owner across the chunk-cache park, so the chunk it would recycle can never
/// come back. 16 is the most the byte budget can pin at the largest chunk
/// size.
constexpr std::array<size_t, 6> chunk_limits{2, 3, 4, 6, 8, 16};

/// Zero is excluded: do_append would loop with no scheduling point and wedge
/// the reactor. Small steps make fallocation fire repeatedly.
constexpr std::array<size_t, 4> falloc_steps{4096, 8192, 16384, 65536};

constexpr std::array<uint64_t, 4> segment_sizes{4096, 32768, 262144, 1048576};

constexpr std::array<size_t, 16> truncate_deltas{
  0,
  1,
  2,
  3,
  8,
  64,
  512,
  4095,
  4096,
  4097,
  8191,
  8192,
  16384,
  65536,
  1U << 20,
  std::numeric_limits<size_t>::max()};

/// memory_shares in resource_mgmt/memory_groups.cc with wasm, datalake and
/// cloud storage off.
constexpr size_t memory_shares_total = 87;
constexpr size_t chunk_cache_shares = 15;

constexpr size_t
total_memory_for_chunk_limit(size_t chunk_size, size_t chunks) {
    return memory_shares_total * (chunks * chunk_size / chunk_cache_shares);
}

/// Too long to ever fire on its own; timer firings come only from the input.
constexpr std::chrono::milliseconds inactive_timeout{std::chrono::hours{24}};

ss::logger fuzz_log("appender-fuzz");

struct decoded_input {
    size_t chunk_size{0};
    size_t chunk_limit{0};
    size_t falloc_step{0};
    std::optional<uint64_t> segment_size;
    uint8_t payload_seed{0};
    std::string_view ops;
};

uint8_t byte_at(std::string_view d, size_t i) {
    return i < d.size() ? static_cast<uint8_t>(d[i]) : uint8_t{0};
}

/// Knobs sit in a fixed prologue so libFuzzer's tail edits mutate only the op
/// stream; bytes 4, 6 and 7 are reserved so a future knob does not invalidate
/// the corpus. Missing bytes read as zero.
decoded_input decode_input(std::string_view d) {
    decoded_input in;
    in.chunk_size = chunk_sizes[byte_at(d, 0) % chunk_sizes.size()];
    in.chunk_limit = chunk_limits[byte_at(d, 1) % chunk_limits.size()];
    in.falloc_step = falloc_steps[byte_at(d, 2) % falloc_steps.size()];
    const uint8_t b3 = byte_at(d, 3);
    if ((b3 & 0x80U) == 0) {
        in.segment_size = segment_sizes[b3 % segment_sizes.size()];
    }
    in.payload_seed = byte_at(d, 5);
    if (d.size() > prologue_size) {
        in.ops = d.substr(prologue_size);
    }
    return in;
}

/// Counter-seeded splitmix64 rather than input bytes, so every input byte
/// buys an operation instead of data.
void fill_payload(char* dst, size_t n, uint64_t seed) {
    uint64_t state = seed;
    size_t done = 0;
    while (done < n) {
        state += 0x9e3779b97f4a7c15ULL;
        uint64_t z = state;
        z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
        z = z ^ (z >> 31U);
        const size_t len = std::min(sizeof(z), n - done);
        std::memcpy(dst + done, &z, len);
        done += len;
    }
}

enum class chain_kind : uint8_t {
    append = 0,
    truncate = 1,
    close = 2,
    fire_timer = 3,
};

constexpr std::string_view to_string_view(chain_kind k) {
    switch (k) {
    case chain_kind::append:
        return "append";
    case chain_kind::truncate:
        return "truncate";
    case chain_kind::close:
        return "close";
    case chain_kind::fire_timer:
        return "fire_timer";
    }
    return "?";
}

struct chain_op {
    chain_kind kind{chain_kind::append};
    size_t arg{0};
    size_t repeat{1};
};

/// Every field is monotone or a queue depth, so an unchanged tuple means
/// nothing observable happened.
struct progress_key {
    uint64_t io_calls{0};
    uint64_t pending{0};
    uint64_t chain_done{0};
    uint64_t flushes_done{0};

    bool operator==(const progress_key&) const = default;
};

/// Highest offset already compared against the reference, so prefix checks
/// stay linear. Restarting at its alignment floor is sound: writes start at
/// align_down(committed_offset), committed_offset drops only on truncate,
/// and truncate clamps this watermark.
struct verify_watermark {
    size_t to{0};

    size_t from(size_t cap) const {
        return std::min(ss::align_down(to, dma_alignment), cap);
    }
    void note(size_t v) { to = std::max(to, v); }
    void clamp(size_t floor) { to = std::min(to, floor); }
};

/// Replays one fuzz input: ops call into the appender and pick which parked
/// device operation completes next, so I/O completion order is part of the
/// reproducible input.
class interpreter {
public:
    interpreter(
      const decoded_input& in,
      tests::manual_file::device& dev,
      std::unique_ptr<storage::segment_appender> appender)
      : _in(in)
      , _dev(dev)
      , _appender(std::move(appender))
      , _scratch(2 * chunk_sizes.back() + 1)
      , _append_sizes{
          1,
          2,
          7,
          8,
          63,
          64,
          512,
          4095,
          4096,
          4097,
          8192,
          in.chunk_size / 2,
          in.chunk_size - 1,
          in.chunk_size,
          in.chunk_size + 1,
          2 * in.chunk_size + 1} {
        _appender->set_stable_offset_callback(
          [this](size_t s) { on_stable_offset(s); });
    }

    interpreter(const interpreter&) = delete;
    interpreter& operator=(const interpreter&) = delete;
    interpreter(interpreter&&) = delete;
    interpreter& operator=(interpreter&&) = delete;
    ~interpreter() = default;

    void execute() {
        vlog(
          fuzz_log.debug,
          "input: chunk_size={} chunk_limit={} falloc_step={} payload_seed={}",
          _in.chunk_size,
          _in.chunk_limit,
          _in.falloc_step,
          _in.payload_seed);

        const size_t count = std::min(_in.ops.size(), max_ops);
        for (size_t i = 0; i < count; ++i) {
            const auto b = static_cast<uint8_t>(_in.ops[i]);
            const auto code = static_cast<uint8_t>(b >> 4U);
            const auto param = static_cast<uint8_t>(b & 0x0FU);
            vlog(fuzz_log.debug, "op[{}] code={} param={}", i, code, param);
            dispatch(code, param);
            // A completion only schedules continuations; run them before the
            // next op.
            ss::thread::yield();
        }

        drain();
        if (!_close_requested) {
            op_close();
            drain();
        }
        vassert(_closed, "close() never completed");

        final_oracles();

        _chain_tail.get();
        _gate.close().get();
        // Run leftover continuations while their captures are still alive so
        // the next input starts on an empty task queue.
        for (size_t i = 0; i < teardown_yields; ++i) {
            ss::thread::yield();
        }
        _appender.reset();

        vlog(
          fuzz_log.debug,
          "final: reference={} file={}",
          _reference.size(),
          _dev.volatile_size());
    }

private:
    void dispatch(uint8_t code, uint8_t param) {
        // Duplicate labels weight the op mix toward appends and completions.
        switch (code) {
        case 0:
        case 1:
        case 2:
            op_append(_append_sizes[param], 1);
            return;
        case 3:
            op_append(_in.chunk_size, (param & 3U) + 1U);
            return;
        case 4:
        case 5:
            op_flush_start();
            return;
        case 6:
        case 7:
            op_complete_one(param);
            return;
        case 8:
            _dev.complete_n((param & 3U) + 1U);
            return;
        case 9:
            _dev.complete_n(_dev.pending_count());
            return;
        case 10:
        case 15:
            for (size_t i = 0; i < (param & 7U) + 1U; ++i) {
                ss::thread::yield();
            }
            return;
        case 11:
            op_fire_timer();
            return;
        case 12:
            op_truncate(truncate_deltas[param]);
            return;
        case 13:
            op_close();
            return;
        default:
            return;
        }
    }

    void skip(std::string_view reason) {
        vlog(fuzz_log.debug, "op skipped: {}", reason);
    }

    bool skip_if_closed() {
        if (_close_requested) {
            skip("closed");
        }
        return _close_requested;
    }

    void op_append(size_t size, size_t repeat) {
        if (skip_if_closed()) {
            return;
        }
        const size_t total = size * repeat;
        if (_bytes_budget_used + total > max_total_appended_bytes) {
            skip("byte budget");
            return;
        }
        _bytes_budget_used += total;
        enqueue(
          chain_op{.kind = chain_kind::append, .arg = size, .repeat = repeat});
    }

    void op_truncate(size_t delta) {
        if (skip_if_closed()) {
            return;
        }
        enqueue(chain_op{.kind = chain_kind::truncate, .arg = delta});
    }

    void op_close() {
        if (skip_if_closed()) {
            return;
        }
        _close_requested = true;
        enqueue(chain_op{.kind = chain_kind::close});
    }

    void op_fire_timer() {
        if (skip_if_closed()) {
            return;
        }
        enqueue(chain_op{.kind = chain_kind::fire_timer});
    }

    void op_complete_one(uint8_t param) {
        if (_dev.pending_count() == 0) {
            skip("nothing parked");
            return;
        }
        _dev.complete(param % _dev.pending_count());
    }

    /// Appender calls run on one chained fiber: append() forbids concurrency,
    /// and truncate()/close() wait on writes that only further ops can
    /// complete, so blocking the interpreter here would deadlock.
    void enqueue(chain_op op) {
        ++_chain_issued;
        // A plain lambda returning a member coroutine's future, not a lambda
        // coroutine: then() frees the captures once the continuation returns,
        // while the coroutine may still be suspended.
        _chain_tail = std::move(_chain_tail).then([this, op] {
            return run_chain_op(op);
        });
    }

    bool chain_busy() const { return _chain_done != _chain_issued; }

    ss::future<> run_chain_op(chain_op op) {
        const uint64_t seq = _chain_done;
        vlog(
          fuzz_log.debug,
          "chain[{}] begin: {} arg={} repeat={}",
          seq,
          to_string_view(op.kind),
          op.arg,
          op.repeat);
        switch (op.kind) {
        case chain_kind::append:
            for (size_t i = 0; i < op.repeat; ++i) {
                co_await do_append(op.arg);
            }
            break;
        case chain_kind::truncate:
            co_await do_truncate(op.arg);
            break;
        case chain_kind::close:
            co_await do_close();
            break;
        case chain_kind::fire_timer:
            fire_timer();
            break;
        }
        ++_chain_done;
        vlog(
          fuzz_log.debug,
          "chain[{}] end: fbo={} size_bytes={}",
          seq,
          _appender->file_byte_offset(),
          _appender->size_bytes());
    }

    ss::future<> do_append(size_t n) {
        vassert(
          n <= _scratch.size(),
          "append({}) over the scratch buffer size {}",
          n,
          _scratch.size());
        fill_payload(_scratch.get_write(), n, payload_seed_for(_append_calls));
        // The reference grows before the call so a flush resolving mid-append
        // can compare against it. _scratch must stay untouched until this
        // append resolves: segment_appender::do_append suspends and resumes
        // reading from it.
        _reference.append(_scratch.get(), n);
        ++_append_calls;
        auto res = co_await ss::coroutine::as_future(
          _appender->append(_scratch.get(), n));
        if (res.failed()) {
            auto e = res.get_exception();
            vunreachable("append({}) failed: {}", n, e);
        }
    }

    ss::future<> do_truncate(size_t delta) {
        const size_t fbo = _appender->file_byte_offset();
        const size_t target = fbo - std::min(delta, fbo);
        begin_content_mutation();
        auto res = co_await ss::coroutine::as_future(
          _appender->truncate(target));
        if (res.failed()) {
            auto e = res.get_exception();
            vunreachable("truncate({}) failed: {}", target, e);
        }
        _reference.resize(target);
        end_content_mutation(target);
    }

    ss::future<> do_close() {
        // Production close() runs under the segment's write lock, so a flush
        // outstanding across close() is outside the appender's contract.
        while (_flushes_outstanding > 0) {
            co_await _flush_idle_cv.wait();
        }
        begin_content_mutation();
        auto res = co_await ss::coroutine::as_future(_appender->close());
        if (res.failed()) {
            auto e = res.get_exception();
            vunreachable("close() failed: {}", e);
        }
        _closed = true;
        end_content_mutation(_appender->file_byte_offset());
    }

    void op_flush_start() {
        if (skip_if_closed()) {
            return;
        }
        if (_flushes_outstanding >= max_outstanding_flushes) {
            skip("too many flushes");
            return;
        }
        const uint64_t id = _next_flush_id++;
        const size_t f = _appender->file_byte_offset();
        const uint64_t epoch = _content_epoch;
        ++_flushes_outstanding;
        vlog(fuzz_log.debug, "flush[{}] start: f={} epoch={}", id, f, epoch);
        auto fut = _appender->flush();
        // Not ssx::spawn_with_gate: it swallows shutdown exceptions, and a
        // swallowed failure here would leave _flushes_outstanding stuck.
        ssx::background
          = ss::with_gate(
              _gate,
              [this, id, f, epoch, fut = std::move(fut)]() mutable {
                  return run_flush(id, f, epoch, std::move(fut));
              })
              .handle_exception([id](const std::exception_ptr& e) {
                  vunreachable(
                    "the flush side channel failed for flush {}: {}", id, e);
              });
    }

    ss::future<>
    run_flush(uint64_t id, size_t f, uint64_t epoch, ss::future<> fut) {
        auto res = co_await ss::coroutine::as_future(std::move(fut));
        if (res.failed()) {
            auto e = res.get_exception();
            vunreachable("flush {} failed: {}", id, e);
        }

        // A flush overlapping a truncate/close would compare against a
        // reference the mutation has since trimmed; skip rather than risk a
        // false content failure.
        bool compared = false;
        if (epoch == _content_epoch && !_content_frozen) {
            vassert(
              _appender->file_byte_offset() >= f,
              "flush {}: file_byte_offset() went backwards from {} to {}",
              id,
              f,
              _appender->file_byte_offset());
            const auto what = fmt::format("flush {}", id);
            verify_volatile_prefix(what, f);
            vassert(
              _dev.durable_size() >= f,
              "{}: only {} bytes have been fsynced, short of the flushed "
              "offset {}",
              what,
              _dev.durable_size(),
              f);
            const auto unsynced = first_durable_difference(
              _durable_verified.from(f), f);
            vassert(
              !unsynced.has_value(),
              "{}: the fsynced content differs from the reference at offset "
              "{} within the flushed range [0, {})",
              what,
              unsynced.value_or(0),
              f);
            _durable_verified.note(f);
            compared = true;
        }

        vlog(
          fuzz_log.debug,
          "flush[{}] resolved: f={} compared={}",
          id,
          f,
          compared);
        ++_flushes_done;
        --_flushes_outstanding;
        if (_flushes_outstanding == 0) {
            _flush_idle_cv.broadcast();
        }
    }

    void begin_content_mutation() {
        ++_content_epoch;
        _content_frozen = true;
    }

    /// Clamping at the end of the window matters: hard_flush()'s head write
    /// reports a pre-truncate stable offset from inside the window.
    void end_content_mutation(size_t content_floor) {
        ++_content_epoch;
        _content_frozen = false;
        _last_stable_offset = content_floor;
        _volatile_verified.clamp(content_floor);
        _durable_verified.clamp(content_floor);
    }

    /// The oracle shared by flush completion and the stable-offset callback:
    /// the device content must match the reference over [0, upto). \p what
    /// prefixes the failure messages.
    void verify_volatile_prefix(std::string_view what, size_t upto) {
        vassert(
          upto <= _reference.size(),
          "{}: offset {} over the reference size {}",
          what,
          upto,
          _reference.size());
        vassert(
          _dev.volatile_size() >= upto,
          "{}: the file is only {} bytes, short of offset {}",
          what,
          _dev.volatile_size(),
          upto);
        const auto diff = first_volatile_difference(
          _volatile_verified.from(upto), upto);
        vassert(
          !diff.has_value(),
          "{}: the file differs from the reference at offset {} within "
          "[0, {})",
          what,
          diff.value_or(0),
          upto);
        _volatile_verified.note(upto);
    }

    void on_stable_offset(size_t s) {
        vlog(fuzz_log.debug, "stable_offset {}", s);
        if (_content_frozen) {
            // Inside a truncate/close window: offsets here are pre-mutation
            // and the reference is about to be trimmed under them.
            return;
        }
        vassert(
          s >= _last_stable_offset,
          "stable offset went backwards: {} then {}",
          _last_stable_offset,
          s);
        _last_stable_offset = s;
        vassert(
          s <= _appender->file_byte_offset(),
          "stable offset {} over file_byte_offset() {}",
          s,
          _appender->file_byte_offset());
        verify_volatile_prefix("stable offset", s);
    }

    bool quiescent() const {
        return !chain_busy() && _flushes_outstanding == 0
               && _dev.pending_count() == 0;
    }

    progress_key snapshot_progress() const {
        return progress_key{
          .io_calls = _dev.submitted(),
          .pending = _dev.pending_count(),
          .chain_done = _chain_done,
          .flushes_done = _flushes_done};
    }

    /// A real timer fires only while armed and seastar disarms it before
    /// running the callback; firing an unarmed one would put the handler in
    /// states production cannot reach.
    void fire_timer() {
        storage::segment_appender_test_accessor acc{*_appender};
        if (!acc.inactive_timer_armed()) {
            skip("timer not armed");
            return;
        }
        acc.fire_inactive_timer();
        vlog(fuzz_log.debug, "timer fired: chain_done={}", _chain_done);
    }

    void drain() {
        constexpr size_t max_drain_rounds = 1U << 20;
        constexpr size_t stall_limit = 512;

        progress_key last{};
        size_t stalled = 0;
        size_t rounds = 0;
        while (!quiescent()) {
            vassert(
              rounds++ < max_drain_rounds,
              "drain did not converge after {} rounds",
              max_drain_rounds);
            if (_dev.pending_count() > 0) {
                _dev.complete(0);
            }
            ss::thread::yield();
            const auto now = snapshot_progress();
            if (now == last) {
                ++stalled;
                vassert(
                  stalled < stall_limit,
                  "the appender made no observable progress for {} task-queue "
                  "rounds with no parked I/O left; a parked flush here is a "
                  "liveness bug (only device completions resolve one); if the "
                  "chain is still busy, check whether chunk_cache::get() is "
                  "spinning",
                  stall_limit);
            } else {
                stalled = 0;
                last = now;
            }
        }
    }

    /// First offset in [from, to) at which the device content read through
    /// \p content_span (volatile_span or durable_span) differs from the
    /// reference, or nullopt.
    std::optional<size_t> first_reference_difference(
      std::string_view (tests::manual_file::device::*content_span)(size_t)
        const,
      size_t from,
      size_t to) const {
        vassert(
          from <= to && to <= _reference.size(),
          "compare range [{}, {}) over the reference size {}",
          from,
          to,
          _reference.size());
        for (size_t at = from; at < to;) {
            const std::string_view content = (_dev.*content_span)(at);
            const size_t n = std::min(to - at, content.size());
            const char* m = std::mismatch(
                              content.data(),
                              content.data() + n,
                              _reference.data() + at)
                              .first;
            if (m != content.data() + n) {
                return at + static_cast<size_t>(m - content.data());
            }
            at += n;
        }
        return std::nullopt;
    }

    std::optional<size_t>
    first_volatile_difference(size_t from, size_t to) const {
        return first_reference_difference(
          &tests::manual_file::device::volatile_span, from, to);
    }

    std::optional<size_t>
    first_durable_difference(size_t from, size_t to) const {
        return first_reference_difference(
          &tests::manual_file::device::durable_span, from, to);
    }

    void final_oracles() {
        vassert(
          _appender->file_byte_offset() == _reference.size(),
          "file_byte_offset() {} != appended bytes {}",
          _appender->file_byte_offset(),
          _reference.size());
        vassert(
          _appender->size_bytes() == _reference.size(),
          "size_bytes() {} != appended bytes {}",
          _appender->size_bytes(),
          _reference.size());
        vassert(
          _dev.volatile_size() == _reference.size(),
          "the file is {} bytes but {} were appended",
          _dev.volatile_size(),
          _reference.size());
        const auto diff = first_volatile_difference(0, _reference.size());
        vassert(
          !diff.has_value(),
          "the file differs from the reference at offset {}",
          diff.value_or(0));
        vassert(
          _dev.durable_size() == _reference.size(),
          "close() resolved with {} bytes fsynced but {} appended",
          _dev.durable_size(),
          _reference.size());
        const auto unsynced = first_durable_difference(0, _reference.size());
        vassert(
          !unsynced.has_value(),
          "close() resolved but the fsynced content differs from the reference "
          "at offset {}",
          unsynced.value_or(0));
    }

    uint64_t payload_seed_for(uint64_t append_index) const {
        return static_cast<uint64_t>(_in.payload_seed)
               ^ (append_index * 0x9e3779b97f4a7c15ULL);
    }

    const decoded_input& _in;
    tests::manual_file::device& _dev;
    std::unique_ptr<storage::segment_appender> _appender;

    std::string _reference;
    ss::temporary_buffer<char> _scratch;
    std::array<size_t, 16> _append_sizes;

    ss::future<> _chain_tail{ss::now()};
    uint64_t _chain_issued{0};
    uint64_t _chain_done{0};

    ss::gate _gate;
    ss::condition_variable _flush_idle_cv;
    size_t _flushes_outstanding{0};
    uint64_t _flushes_done{0};
    uint64_t _next_flush_id{0};

    uint64_t _content_epoch{0};
    bool _content_frozen{false};
    size_t _last_stable_offset{0};
    verify_watermark _volatile_verified;
    verify_watermark _durable_verified;

    size_t _bytes_budget_used{0};
    uint64_t _append_calls{0};

    bool _close_requested{false};
    bool _closed{false};
};

/// The chunk cache snapshots append_chunk_size in its constructor, so the
/// config must precede storage_resources and the memory groups must precede
/// start(); teardown reverses this via declaration order.
void execute_once(const decoded_input& in) {
    scoped_config cfg;
    cfg.get("append_chunk_size").set_value(in.chunk_size);
    cfg.get("segment_appender_flush_timeout_ms").set_value(inactive_timeout);
    // chunk_cache would otherwise register metrics once per input
    cfg.get("disable_metrics").set_value(true);

    memory_groups_holder().emplace(
      total_memory_for_chunk_limit(in.chunk_size, in.chunk_limit),
      compaction_memory_reservation{},
      cloud_topics_compaction_memory_reservation{},
      cloud_topics_reconciler_memory_reservation{},
      data_transforms_memory_reservation{},
      /*wasm_enabled=*/false,
      /*datalake_enabled=*/false,
      /*cloud_storage_enabled=*/false,
      partitions_memory_reservation{.max_limit_pct = 0});
    auto reset_memory_groups = ss::defer(
      [] { memory_groups_holder().reset(); });

    const auto& mem = memory_groups();
    const size_t limit = mem.chunk_cache_max_memory();
    vassert(
      (limit + in.chunk_size - 1) / in.chunk_size == in.chunk_limit,
      "chunk cache budget is off: limit {}, chunk size {}, wanted {} chunks",
      limit,
      in.chunk_size,
      in.chunk_limit);
    vassert(
      mem.chunk_cache_min_memory() > 0,
      "chunk cache soft target is 0, so add() would drop every returned chunk "
      "without signalling and strand a waiter");

    // Mock bindings so nothing binds into shard_local_cfg() beyond the
    // resources' lifetime; only the falloc step matters to the appender.
    storage::storage_resources resources(
      config::mock_binding<size_t>(size_t{in.falloc_step}),
      config::mock_binding<uint64_t>(uint64_t{10 * 1024 * 1024}),
      config::mock_binding<uint64_t>(uint64_t{1024}),
      config::mock_binding<uint64_t>(uint64_t{128 * 1024 * 1024}));
    resources.start().get();

    tests::manual_file::device dev(fuzz_log);

    {
        interpreter interp(
          in,
          dev,
          std::make_unique<storage::segment_appender>(
            tests::manual_file::make_file(dev, dma_alignment),
            storage::segment_appender::options(
              in.segment_size, resources, nullptr)));
        interp.execute();
    }

    resources.stop().get();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::string input(reinterpret_cast<const char*>(data), size);
    seastar_fuzz::test_one_input(
      [input = std::move(input)] { execute_once(decode_input(input)); });
    return 0;
}
