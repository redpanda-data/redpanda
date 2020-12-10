/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "storage/version.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>

#include <optional>
#include <variant>

namespace storage {

// FSM states:
//
// - st_pre_open - file is created but not yet accessed
// - st_open     - actual file descriptor is opened
// - st_close    - file closed (no further I/O operations are possible)
// - st_evict    - file closed temporarily and prepared to be reopened
//
// FSM events:
//
// - ev_access   - triggered by any async file operation except close
// - ev_close    - triggered by close
// - ev_evict    - triggered by eviction process
//
// FSM transition table
//
// clang-format off
// +--------------+-----------+--------------+------------------------------+------------------------------------+
// | source state |   event   | target state |        event trigger         |         transition effect          |
// +--------------+-----------+--------------+------------------------------+------------------------------------+
// | st_pre_open  | ev_access | st_open      | any operation except close   | file is opened/created and         |
// |              |           |              |                              | added to cache triggering eviction |
// |              |           |              | and the cache is full        |                                    |
// | st_pre_open  | ev_close  | st_close     | close method called          | no effect                          |
// | ------------ | --------- | ------------ | ---------------------------- | ---------------------------------- |
// | st_open      | ev_evict  | st_evict     | triggered by eviction policy | file is temporarily closed         |
// |              |           |              |                              | and removed from cache             |
// | st_open      | ev_access | st_open      | any operation except close   | touch LRU cache entry              |
// | st_open      | ev_close  | st_close     | close method called          | file is closed, removed from cache |
// | ------------ | --------- | ------------ | ---------------------------- | ---------------------------------- |
// | st_evict     | ev_access | st_open      | any operation except close   | file is reopened and               |
// |              |           |              |                              | added to cache                     |
// | st_evict     | ev_close  | st_close     | close method called          | no effect                          |
// | ------------ | --------- | ------------ | ---------------------------- | ---------------------------------- |
// | st_closed    | any       | st_close     |                              |                                    |
// +--------------+-----------+--------------+------------------------------+------------------------------------+
// clang-format on

/// Control the way file_stm is created
enum class open_file_stm_options {
    /// Open/create file on first access
    lazy,
    /// Force open/create file ASAP
    eager,
};

namespace details {

/// This object acts as a synchronization primitive for open_file_cache.
/// It allows multiple file_stm objects to wait for eviction. It guarantees
/// fairness so waiting fibers will be served in fifo order.
template<class... T>
class wait_list {
public:
    /// Wait for the trigger method to be called
    ss::future<T...> wait() {
        _fifo.push(ss::promise<T...>());
        return _fifo.back().get_future();
    }

    /// Trigger oldest request in the list
    void trigger(T&&... value) {
        if (!_fifo.empty()) {
            _fifo.front().set_value(std::forward<T>(value)...);
            _fifo.pop();
        }
    }

    /// Number of clients in the queue
    size_t size() const { return _fifo.size(); }

    /// Trigger all requests
    void trigger_all(const T&... value) {
        while (!_fifo.empty()) {
            trigger(T(value)...);
        }
    }

private:
    using list_t = std::queue<ss::promise<T...>>;
    list_t _fifo;
};

// FSM states

using event_t = std::function<ss::future<>()>;

struct st_pre_open {
    std::string path;
    ss::open_flags flags;
    ss::file_open_options options;
};
struct st_open {
    st_pre_open open_args;
    std::optional<ss::file> file;
    ss::gate gate;
    /// Used by open_file_cache
    intrusive_list_hook _hook;
    /// Set by the file_stm::impl and invoked by cache on eviction. Switches the
    /// file_stm state to st_evict.
    event_t _on_evict;
    /// Used by the file_stm in case when multiple fibers need to open the file
    wait_list<> _wait_open;
};
struct st_evict {
    st_pre_open reopen_args;
    event_t _on_restore;
};
struct st_close {};
} // namespace details

/// \brief Cache that tracks opened files.
///
/// The max allowed cache size is an integer which
/// allows the cache to hold more than 4-billion opened
/// which is plentyfull. Zero value allows the cache
/// to grow unconstrained.
class open_file_cache final : ss::weakly_referencable<open_file_cache> {
public:
    using entry_t = details::st_open;
    using entry_ref = std::reference_wrapper<entry_t>;

    /// \brief Create file cache
    ///
    /// \param limit is a max allowed cache size
    explicit open_file_cache(uint32_t limit);
    ~open_file_cache();
    open_file_cache(const open_file_cache&) = delete;
    open_file_cache(open_file_cache&&) = delete;
    open_file_cache& operator=(const open_file_cache&) = delete;
    open_file_cache& operator=(open_file_cache&&) = delete;

    /// Returns true if the cache is empty, and false otherwise.
    bool empty() const { return _lru.empty(); }

    /// Returns true if the cache is full
    bool is_full() const { return _limit && _lru.size() >= _limit; }

    /// \brief Add entry to the cache
    ///
    /// \param entry is a cache entry to put
    ss::future<> put(entry_t& entry);

    /// Evict file from cache
    void evict(entry_t& entry);

    /// Return true if the 'entry' is a first candidate for eviction
    bool is_candidate(const entry_t& entry) const;

    /// Bump file position in LRU cache
    void touch(entry_t& entry);

    /// Returns max allowed cache size
    uint32_t max_allowed_count() const;

    /// Returns true if some other fiber is waiting for eviction
    bool eviction_awaited() const;

private:
    uint32_t _limit;
    using list_t = intrusive_list<entry_t, &entry_t::_hook>;
    using wait_list_t = details::wait_list<entry_ref>;
    list_t _lru;
    wait_list_t _eviction;
};

class file_stm final {
    /// The object enters the gate of the st_open state
    /// variable on creation and exits it when the object
    /// is destroyed. Can be moved safely.
    class lock_guard {
        using gate_ref_t = std::reference_wrapper<ss::gate>;
        using element_t = details::st_open;
        using element_ref = std::reference_wrapper<element_t>;

    public:
        lock_guard();
        lock_guard(
          details::st_open& st, ss::lw_shared_ptr<open_file_cache> cache);
        lock_guard(const lock_guard&) = delete;
        lock_guard(lock_guard&&) = default;
        lock_guard& operator=(const lock_guard&) = delete;
        lock_guard& operator=(lock_guard&&) = default;
        ~lock_guard();

        /// Get transient file handle
        /// The value returned by the method only works until the object
        /// exist
        ss::file get_file();

    private:
        std::optional<element_ref> _elem;
        ss::lw_shared_ptr<open_file_cache> _cache;
    };

    class impl {
        enum {
            ix_pre_open,
            ix_open,
            ix_evict,
            ix_close,
        };
        using file_state_t = std::variant<
          details::st_pre_open,
          details::st_open,
          details::st_evict,
          details::st_close>;

    public:
        impl(
          std::string_view name,
          ss::open_flags flags,
          ss::file_open_options options,
          ss::lw_shared_ptr<open_file_cache> cache);
        ~impl();

        // FSM events
        ss::future<lock_guard> ev_access();
        ss::future<> ev_close();
        ss::future<> ev_evict();

        bool is_pre_open() const;
        bool is_opened() const;
        bool is_evicted() const;
        bool is_closed() const;

        template<class... Arg>
        static ss::lw_shared_ptr<impl> create(Arg... arg) {
            return ss::make_lw_shared<impl>(arg...);
        }

    private:
        ss::future<lock_guard>
        state_transition_to_open(details::st_open&& newstate);

        friend class open_file_cache;
        file_state_t _state;
        ss::lw_shared_ptr<open_file_cache> _cache;
    };

    explicit file_stm(ss::lw_shared_ptr<impl> impl);

public:
    file_stm() = default;
    file_stm(const file_stm&) = default;
    file_stm(file_stm&&) = default;
    file_stm& operator=(const file_stm&) = default;
    file_stm& operator=(file_stm&&) = default;

    /// Call the function with ss::file as a parameter
    ///
    /// \param cb is a function to call with file
    /// \param args is a list of optional arguments for 'cb'
    /// \return future that returns result of the 'cb'
    template<class Fn, class... OptArgs>
    auto with_file(Fn&& cb, OptArgs... args) {
        return _impl->ev_access().then([this,
                                        func = std::forward<Fn>(cb),
                                        args...](lock_guard guard) mutable {
            return func(guard.get_file(), args...);
        });
    }

    /// Check internal state
    bool is_pre_open() const;
    bool is_opened() const;
    bool is_evicted() const;
    bool is_closed() const;

    /// Returns true if the object refers to the actual file
    explicit operator bool() const noexcept { return static_cast<bool>(_impl); }

    ss::future<> close();

    /// Access the file object indirectly
    ss::future<lock_guard> get_file();

private:
    friend class open_file_cache;
    friend ss::future<file_stm> open_file_dma(
      std::string_view name,
      ss::open_flags flags,
      ss::file_open_options options,
      ss::lw_shared_ptr<open_file_cache> cache,
      open_file_stm_options stm_opt);

    ss::lw_shared_ptr<impl> _impl;
};

/// Opens or creates a file_stm object. Similar to
/// seastar function with the same name.
///
/// \param name  the name of the file
/// \param flags open flags
/// \param options file opening options
/// \param cache is a file cache
/// \param stm_opt controls how the file is opened (immediately or on-demand)
/// \return a \ref file_stm as a future
ss::future<file_stm> open_file_dma(
  std::string_view name,
  ss::open_flags flags,
  ss::file_open_options options,
  ss::lw_shared_ptr<open_file_cache> cache,
  open_file_stm_options stm_opt = open_file_stm_options::eager);
ss::future<file_stm> open_file_dma(
  std::string_view name,
  ss::open_flags flags,
  ss::lw_shared_ptr<open_file_cache> cache,
  open_file_stm_options stm_opt = open_file_stm_options::eager);

/// \brief Creates an input_stream to read a portion of a file.
///
/// Same as make_file_input_stream. Prevents eviction of the file until
/// input_stream exists.
ss::input_stream<char> make_file_input_stream(
  file_stm file,
  uint64_t offset,
  uint64_t len,
  ss::file_input_stream_options options = {});

} // namespace storage