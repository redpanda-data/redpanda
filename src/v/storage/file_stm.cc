#include "storage/file_stm.h"

#include "storage/logger.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>

#include <exception>
#include <variant>

namespace storage {

open_file_cache::open_file_cache(uint32_t limit)
  : _limit(limit) {}

open_file_cache::~open_file_cache() {
    vassert(_lru.empty(), "open_file_cache is not empty");
}

void open_file_cache::touch(entry_t& file) {
    file._hook.unlink();
    _lru.push_back(file);
    if (is_full()) {
        auto candidate = _lru.begin();
        if (candidate->gate.get_count() == 0) {
            _eviction.trigger(*candidate);
        }
    }
}

ss::future<> open_file_cache::put(entry_t& file) {
    // if we have a room for a new element just add it
    // to the cache, otherwise, use this fiber to evict
    // the other element from the cache and add the new
    // one
    _lru.push_back(file);
    auto fut = ss::now();
    if (is_full()) {
        auto candidate = _lru.begin();
        if (candidate->gate.get_count() == 0) {
            // eviction candidate is not used
            fut = candidate->_on_evict();
        } else {
            fut = _eviction.wait().then(
              [](entry_t& it) { return it._on_evict(); });
        }
    }
    return fut;
}

void open_file_cache::evict(entry_t& entry) {
    vlog(
      stlog.debug,
      "open_file_cache evict called, path: {}",
      entry.open_args.path);
    entry._hook.unlink();
    vassert(entry.gate.get_count() == 0, "file is in use");
    // This is OK since the eviction will be triggered by the futurized
    // code path that will 'wait' on '_eviction' before calling 'evict'.
    _eviction.trigger(entry);
}

bool open_file_cache::is_candidate(const entry_t& entry) const {
    auto it = _lru.iterator_to(entry);
    return it == _lru.begin();
}

bool open_file_cache::eviction_awaited() const { return _eviction.size() != 0; }

uint32_t open_file_cache::max_allowed_count() const { return _limit; }

/// file_stm implementation

file_stm::impl::impl(
  std::string_view name,
  ss::open_flags flags,
  ss::file_open_options options,
  ss::lw_shared_ptr<open_file_cache> cache)
  : _state(details::st_pre_open{
    .path = std::string(name.begin(), name.end()),
    .flags = flags,
    .options = options})
  , _cache(std::move(cache)) {}

// Removes 'create' flag from 'flags', keeps remaining flags
// intact.
static ss::open_flags make_reopen_flags(ss::open_flags flags) {
    auto tmp = flags & ss::open_flags::create;
    return static_cast<ss::open_flags>((unsigned)tmp ^ (unsigned)flags);
}

// lock_guard implementation

file_stm::lock_guard::lock_guard()
  : _elem(std::nullopt)
  , _cache() {}

file_stm::lock_guard::lock_guard(
  details::st_open& state, ss::lw_shared_ptr<open_file_cache> cache)
  : _elem(std::ref(state))
  , _cache(std::move(cache)) {
    _elem->get().gate.enter();
}

file_stm::lock_guard::~lock_guard() {
    try {
        if (_cache) {
            auto& element = _elem->get();
            element.gate.leave();
            if (
              element._hook.is_linked() && element.gate.get_count() == 0
              && _cache->eviction_awaited() && _cache->is_candidate(element)) {
                // This will trigger actual cache eviction, the node will be
                // unlinked and the file_stm::impl object that owns the element
                // will be transitioned to st_evict state.
                _cache->evict(element);
            }
        }
    } catch (...) {
        vlog(
          stlog.error,
          "delayed eviction failed, error: {}",
          std::current_exception());
    }
}

ss::file file_stm::lock_guard::get_file() {
    vassert((bool)_elem, "lock_guard is empty");
    return *_elem->get().file;
}

ss::future<file_stm::lock_guard>
file_stm::impl::state_transition_to_open(details::st_open&& newstate) {
    vlog(
      stlog.debug,
      "file_stm transition to st_open state, path: {}",
      newstate.open_args.path);
    _state = std::move(newstate);
    auto fut = _cache->put(std::get<details::st_open>(_state));
    return fut.then([this] {
        const auto& st = std::get<details::st_open>(_state);
        return ss::open_file_dma(
                 st.open_args.path, st.open_args.flags, st.open_args.options)
          .then_wrapped([this](ss::future<ss::file> fd) {
              auto& current = std::get<details::st_open>(_state);
              try {
                  current.open_args.flags = make_reopen_flags(
                    current.open_args.flags);
                  current.file = fd.get();
                  current._on_evict = [this] { return ev_evict(); };
                  // notify other fibers that wait for this state to be
                  // initialized
                  current._wait_open.trigger_all();
              } catch (...) {
                  vlog(
                    stlog.error,
                    "can't open file: {} error: {}",
                    current.open_args.path,
                    std::current_exception());
              }
              return ss::make_ready_future<lock_guard>(
                lock_guard(current, _cache));
          });
    });
}

ss::future<file_stm::lock_guard> file_stm::impl::ev_access() {
    auto result = ss::make_ready_future<lock_guard>();
    switch (_state.index()) {
    case ix_pre_open: {
        const auto& args = std::get<details::st_pre_open>(_state);
        result = state_transition_to_open(details::st_open{
          .open_args = args,
          .file = std::nullopt,
          .gate = {},
        });
    } break;
    case ix_evict: {
        const auto& args = std::get<details::st_evict>(_state);
        result = state_transition_to_open(details::st_open{
          .open_args = args.reopen_args,
          .file = std::nullopt,
          .gate = {},
        });
    } break;
    case ix_open: {
        // ..touch lru entry, state stays the same
        auto& item = std::get<details::st_open>(_state);
        if (item.file) {
            _cache->touch(item);
            result = ss::make_ready_future<lock_guard>(
              lock_guard(item, _cache));
        } else {
            // This path handles concuurent access to file. In this case
            // first future will change state to 'st_open' and start opening the
            // file asynchronously. Before it's completed the 'file' filed of
            // the state will be set to std::nullopt. Fibers that see that this
            // is the case should wait until it'll be opened.
            result = item._wait_open.wait().then_wrapped(
              [this](ss::future<> fut) {
                  if (fut.failed()) {
                      // handle case when file was closed
                      return ss::make_exception_future<lock_guard>(
                        fut.get_exception());
                  }
                  if (auto p = std::get_if<details::st_open>(&_state);
                      p != nullptr) {
                      return ss::make_ready_future<lock_guard>(
                        lock_guard(*p, _cache));
                  }
                  return ss::make_exception_future<lock_guard>(
                    std::runtime_error("file is not opened"));
              });
        }
    } break;
    case ix_close:
        result = ss::make_exception_future<lock_guard>(
          std::runtime_error("file is not opened"));
        break;
    }
    return result;
}

ss::future<> file_stm::impl::ev_close() {
    auto fut = ss::now();
    if (auto s = std::get_if<details::st_open>(&_state); s && s->file) {
        auto open_state = std::move(*s);
        vlog(
          stlog.debug,
          "file_stm transition to st_close state, path: {}",
          open_state.open_args.path);
        fut = open_state.gate.close().then(
          [this, os = std::move(open_state)]() mutable {
              if (_cache->is_full() && _cache->is_candidate(os)) {
                  _cache->evict(os);
              }
              return os.file->flush().then([file = *os.file]() mutable {
                  return file.close().finally([file] {});
              });
          });
    }
    _state = details::st_close{};
    return fut;
}

ss::future<> file_stm::impl::ev_evict() {
    if (auto s = std::get_if<details::st_open>(&_state); s && s->file) {
        auto fut = ss::now();
        auto open_state = std::move(*s);
        vlog(
          stlog.debug,
          "file_stm evict wait for readers {}",
          open_state.open_args.path);
        fut = open_state.gate.close().then(
          [file = std::move(*open_state.file),
           path = open_state.open_args.path]() mutable {
              vlog(stlog.debug, "file_stm evicting {}", path);
              return file.flush().then(
                [file]() mutable { return file.close().finally([file] {}); });
          });
        // This will be called from the open_file_cache::evict method. In this
        // scenario the old state (st_open) will already be unlinked from the
        // cache.
        _state = details::st_evict{
          .reopen_args = {
            .path = open_state.open_args.path,
            .flags = make_reopen_flags(open_state.open_args.flags),
            .options = open_state.open_args.options}};
        return fut;
    }
    return ss::make_exception_future<>(
      std::runtime_error("can't evict file from cache, file not opened"));
}

bool file_stm::impl::is_pre_open() const {
    return std::holds_alternative<details::st_pre_open>(_state);
}

bool file_stm::impl::is_opened() const {
    return std::holds_alternative<details::st_open>(_state);
}

bool file_stm::impl::is_evicted() const {
    return std::holds_alternative<details::st_evict>(_state);
}

bool file_stm::impl::is_closed() const {
    return std::holds_alternative<details::st_close>(_state);
}

file_stm::impl::~impl() {
    vassert(is_closed(), "file_stm is not closed properly");
}

file_stm::file_stm(ss::lw_shared_ptr<file_stm::impl> i)
  : _impl(std::move(i)) {}

ss::future<> file_stm::close() { return _impl->ev_close(); }

bool file_stm::is_pre_open() const {
    vassert(static_cast<bool>(_impl), "not initialized");
    return _impl->is_pre_open();
}
bool file_stm::is_opened() const {
    vassert(static_cast<bool>(_impl), "not initialized");
    return _impl->is_opened();
}
bool file_stm::is_evicted() const {
    vassert(static_cast<bool>(_impl), "not initialized");
    return _impl->is_evicted();
}
bool file_stm::is_closed() const {
    vassert(static_cast<bool>(_impl), "not initialized");
    return _impl->is_closed();
}

ss::future<file_stm> open_file_dma(
  std::string_view name,
  ss::open_flags flags,
  ss::file_open_options options,
  ss::lw_shared_ptr<open_file_cache> cache,
  open_file_stm_options stm_opt) {
    auto file = file_stm::impl::create(name, flags, options, std::move(cache));
    if (stm_opt == open_file_stm_options::lazy) {
        return ss::make_ready_future<file_stm>(file_stm(file));
    }
    return file->ev_access().then(
      [file]([[maybe_unused]] file_stm::lock_guard lg) mutable {
          return ss::make_ready_future<file_stm>(file_stm(file));
      });
}

ss::future<file_stm> open_file_dma(
  std::string_view name,
  ss::open_flags flags,
  ss::lw_shared_ptr<open_file_cache> cache,
  open_file_stm_options stm_opt) {
    return open_file_dma(name, flags, {}, std::move(cache), stm_opt);
}

} // namespace storage
