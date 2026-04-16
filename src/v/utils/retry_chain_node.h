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

/* This mechanism is supposed to help with the situation when
 * we have a fiber that spawns multiple fibers which in turn
 * can spawn even more fibers. It's a normal situation with
 * Seastar but the problem has many dimentions:
 * - logging is tricky since every shard runs many concurrent
 *   futures and it's hard to correlate messages with each other
 * - retrying things is also tricky because the parent fiber can
 *   have some time limitation but child fibers might not finish
 *   in time.
 * - aborting is only possible from top to bottom, it's not possible
 *   for a child fiber to abort the entire tree
 * Provided mechanism can solve these two issues by creating an
 * explicit tree of dependency between the fibers and tracking
 * retries and fiber identities/relationships.
 *
 * Code sample (problem):
 *
 * ss::future<reply_t> endpoint::send_request(request req) {
 *   while (true) {
 *     try {
 *       co_return co_await do_send_request(req);
 *     } catch (const network_error& err) {
 *       vlog(logger, "send_request to {} error {}", name, err);
 *     }
 *     co_await ss::sleep(100ms);
 *   }
 *   __builtin_unreachable();
 * }
 *
 * // send request to many endpoints
 * ss::future<reply_t> fan_out_read(request req,
 *                                  const endpoint_list& endpoints)
 * {
 *   using result_fut = std::future<reply_t>;
 *   std::vector<result_fut> fut;
 *   for (auto endpoint: endpoints) {
 *     // send request is long running and can retry
 *     results.emplace_back(endpoint.send_request(req));
 *   }
 *   co_await ss::when_all_succeed(fut.begin(), fut.end());
 *   co_return fut.front().get();
 * }
 *
 * Errors are not handled here for clarity. The sample function sends a
 * request to many endpoints. It receives the same reply from every one
 * of them. It's hard to integrate retry strategy into fan_out_read because
 * we might receive successful reply from one endpoint but still waiting
 * or retrying the other endpoint. Also, there is a log output that will
 * look pretty much identical for every retry attempt.
 *
 * ss::future<reply_t> endpoint::send_request(request req,
 *                                            retry_chain_node& n) {
 *   auto now = ss::lowres_clock::now();
 *   retry_chain_node fn(&n, now + 1000ms, 100ms);
 *   while (true) {
 *     try {
 *       co_return co_await do_send_request(req);
 *     } catch (network_error) {
 *       vlog(logger, "{} send_request to {} error {}", fn(), name, err);
 *     }
 *     auto ret = fn.retry();
 *     if (!ret.is_allowed) {
 *       ...trigger error
 *     }
 *     co_await ss::sleep_abortable(ret.delay, *ret.abort_source);
 *   }
 *   __builtin_unreachable();
 * }
 *
 * // send request to many endpoints, stop when the reply is
 * // received
 * ss::future<reply_t> fan_out_read(request req,
 *                                  const endpoint_list& endpoints)
 * {
 *   auto now = ss::lowres_clock::now();
 *   retry_chain_node root(abort_source, now + 10000ms, 100ms);
 *   retry_permit perm = root.retry();
 *   using result_fut = std::future<reply_t>;
 *   while (perm.is_allowed) {
 *      std::vector<result_fut> fut;
 *      for (auto endpoint: endpoints) {
 *        // send request is long running and can retry
 *        results.emplace_back(endpoint.send_request(req, root)
 *               .then([&root] (reply_t r) {
 *                 // this will cancel all fibers that might be waiting
 *                 // for the next retry inside sleep_abortable
 *                 root.request_abort();
 *                 return ss::make_ready_future<reply_t>(r);
 *         }));
 *      }
 *      auto res = co_await ss::when_all(fut.begin(), fut.end());
 *      for (auto& f: res) {
 *        if (!f.failed()) {
 *          co_return f.get();
 *        }
 *      }
 *      // all replies failed
 *      co_await ss::sleep_abortable(perm.delay, abort_source);
 *      perm = root.retry();
 *   }
 * }
 *
 * Here the 'retry_chain_node' inside of the 'fan_out_read' function is a
 * tree root. The 'retry_chain_node' instances inside the 'send_request' are
 * leaves of the tree. The 'abort_source' is propagated from the root, so any
 * leaf can be used to cancel the entire tree. This is done when the first
 * reply is received. Also, the 'retry_chain_node' is used to generate unique
 * fiber ids in the example.
 * The leaf instances of 'retry_chain_node' will respect time limitations of the
 * root node. The backoff interval that leaf nodes will generate will be inside
 * the interval of the root node.
 * The tree can have more levels than the example has. For instance, the
 * 'fan_out_read' function may accept a root node as a parameter and create a
 * leaf node instead of the root node.
 *
 * The lifetimes of the nodes are constrained. The assertion is triggered when
 * invariant of the tree is broken (e.g. when the root node is destroyed before
 * all of its children).
 *
 */

#include "base/seastarx.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <chrono>
#include <exception>
#include <iterator>
#include <ranges>
#include <variant>

namespace detail {
/// A fixed-size circular buffer that behaves like std::vector for basic
/// operations. When the buffer reaches its maximum capacity, new elements
/// overwrite the oldest elements in FIFO order.
class rtc_circular_buffer {
public:
    using value_type = char;
    using reference = char&;
    using const_reference = const char&;

    /// Create a circular buffer with the specified maximum capacity
    explicit rtc_circular_buffer(size_t capacity);

    /// Add an element to the buffer. When the buffer is full, the oldest
    /// element is overwritten.
    void push_back(value_type value);

    void push_back(std::string_view sv);

    /// Access element at logical index (0 is the oldest element)
    reference operator[](size_t index);

    /// Access element at logical index (0 is the oldest element)
    const_reference operator[](size_t index) const;

    /// Access element at logical index with bounds checking
    reference at(size_t index);

    /// Access element at logical index with bounds checking
    const_reference at(size_t index) const;

    /// Get the first (oldest) element
    reference front();

    /// Get the first (oldest) element
    const_reference front() const;

    /// Get the last (newest) element
    reference back();

    /// Get the last (newest) element
    const_reference back() const;

    /// Number of elements currently in the buffer
    size_t size() const noexcept;

    /// Maximum capacity of the buffer
    size_t capacity() const noexcept;

    /// Check if the buffer is empty
    bool empty() const noexcept;

    /// Check if the buffer is at full capacity
    bool full() const noexcept;

    /// Get the number of elements that were added to the buffer and
    /// then overwritten and lost. The count is reset if 'clear' is
    /// called.
    size_t overwritten() const noexcept;

    /// Clear all elements from the buffer
    void clear() noexcept;

    /// Return a view of the entire buffer (const version)
    auto view() const {
        return std::views::join(
          std::views::all(
            std::array{
              std::span(_data).subspan(
                _start,
                _size < _capacity ? _size - _start : _capacity - _start),
              std::span(_data).subspan(0, _size < _capacity ? 0 : _start)}));
    }

    /// Return a view of the entire buffer
    auto view() {
        return std::views::join(
          std::views::all(
            std::array{
              std::span(_data).subspan(
                _start,
                _size < _capacity ? _size - _start : _capacity - _start),
              std::span(_data).subspan(0, _size < _capacity ? 0 : _start)}));
    }

private:
    std::vector<value_type> _data;
    size_t _capacity;
    size_t _size{0};
    size_t _start{0};
    size_t _total_added{0};
};

} // namespace detail

/// Represents the context for a retry chain node, capable of collecting trace
/// messages in memory.
///
/// This context is intended for logging repeated or periodic processes—
/// for example, a background fiber that wakes up at intervals to perform work.
/// It works in conjunction with `retry_chain_node` and `retry_chain_logger`
/// to provide structured logging.
///
/// The context maintains an in-memory buffer for trace messages, with a
/// configurable memory limit. This limit should be set based on the number of
/// trace messages expected per iteration of the process.
///
/// When the buffer reaches its capacity, additional trace messages are
/// discarded. In such cases, the buffer will end with a "[truncated]" message
/// to indicate overflow. As such, this class is not intended for logging
/// unbounded traces between resets.
///
/// Additionally, the context can provide an abort source, allowing external
/// control to stop the associated fiber.
template<typename Clock = ss::lowres_clock>
class basic_retry_chain_context {
public:
    /// Create a trace memory buffer with the specified size.
    /// \name is a name of the context
    /// \as is an abort source
    /// \size is a size limit for the in-memory buffer
    explicit basic_retry_chain_context(
      ss::sstring name, ss::abort_source& as, size_t size)
      : _name(std::move(name))
      , _as(as)
      , _size_limit(size)
      , _buf(size) {}

    /// Add a trace to the buffer.
    void add_trace(const ss::sstring& msg) const {
        _last_trace_ts = Clock::now();
        _buf.push_back(std::string_view{msg.data(), msg.size()});
        _buf.push_back('\n');
    }

    /// Returns a view of trace strings that can be iterated
    auto traces() const {
        return std::ranges::split_view(_buf.view(), '\n')
               | std::views::transform([](auto&& r) {
                     auto begin = std::ranges::begin(r);
                     auto end = std::ranges::end(r);
                     return ss::sstring(begin, end);
                 })
               | std::views::filter(
                 [](const ss::sstring& s) { return !s.empty(); });
    }

    bool truncation_warning() const { return _buf.overwritten() > 0; }

    /// Get the entire trace log as a string (added for testing).
    ss::sstring get_trace_log() const {
        ss::sstring res;
        for (const auto& trace : traces()) {
            res += trace;
            res += "\n";
        }
        return res;
    }

    auto get_last_trace_time() const noexcept { return _last_trace_ts; }

    /// Reset the state of the context and empties the buffer
    void reset() {
        _buf.clear();
        _last_trace_ts = Clock::now();
        _suspended_to = Clock::time_point::min();
    }

    /// Get shared abort source of the context
    ss::abort_source& as() noexcept { return _as; }

    /// This method informs the context about the stall.
    /// This will push the deadline into the future. The idea is that this
    /// method should be invoked before the 'seastar::sleep' or similar method
    /// is invoked to avoid triggering the stall detection logic.
    void suspend_to(Clock::time_point sp) noexcept { _suspended_to = sp; }

    /// Get the deadline value.
    /// The deadline is computed as the timestamp of the last message logged +
    /// stall timeout. The 'suspend_to' can override this value.
    Clock::time_point get_deadline(Clock::duration stall_timeout) const {
        return std::max(_last_trace_ts + stall_timeout, _suspended_to);
    }

    const ss::sstring& name() const noexcept { return _name; }

private:
    ss::sstring _name;
    /// Root abort source
    ss::abort_source& _as;
    /// Trace buffer size limit
    size_t _size_limit;
    /// Buffer to store traces
    mutable detail::rtc_circular_buffer _buf;
    /// Last trace timestamp
    mutable Clock::time_point _last_trace_ts;
    Clock::time_point _suspended_to;
};

/// Retry strategy
enum class retry_strategy : uint8_t {
    /// Simple polling
    polling,
    /// Exponential backoff
    backoff,
    /// No retries
    disallow
};

/// Retry permit
/// If `is_allowed` is false retry is not allowed. Otherwise, the fiber
/// should wait `delay` milliseconds (using the `abort_source` if it's not
/// null) and retry.
struct retry_permit {
    bool is_allowed;
    ss::abort_source* abort_source;
    ss::lowres_clock::duration delay;
};

/// \brief Node of the retry chain.
///
/// The object is relatively lightweight (40 bytes).
/// The retry chain is a set of fibers that has the same timeout or
/// deadline. The fibers form a hierarhy (root fiber spawns several child
/// fibers, some child fibers spawn next set of fibers, etc). The fibers
/// that perform some I/O may periodicly retry on failure. Retries should
/// be kept inside time bounds of the retry chain.
///
/// The instance of this object can be created on the stack of a fiber.
/// It's supposed to be passed by reference to the child fibers and used
/// to create another retry_chain_node instances. The nodes form a
/// tree-like data structure. This data structure can be used to
/// calculate backoff timeouts that take into acount total time budget
/// that the parent node has. It can be used to share the abort_source
/// instance between related fibers. This allows child fibers to trigger
/// abort_source and stop the entire computation. Also, the instance of
/// this class can be used to provide identitiy for logging. Call operator
/// returns a unique fiber id that takes hierarchy into account.
///
/// The node can be either a root or a leaf. The root node doesn't receive
/// a pointer to the parent node in c-tor. It can receive an abort_source,
/// timeout value, or backoff interval in c-tor. These parameters are used
/// by all child nodes. They have to respect the timeout of the root and
/// can't set larger timeout.
/// The leaf node receives a pointer to a parent node (leaf or root).
/// It can also receive its own timeout value and backoff interval. The
/// timeout value have to be smaller that the one of the parent node.
template<class Clock = ss::lowres_clock>
class [[gnu::warn_unused]] basic_retry_chain_node {
    using context_t = basic_retry_chain_context<Clock>;

public:
    using clock = Clock;
    using duration = typename clock::duration;
    using time_point = typename clock::time_point;
    using milliseconds_uint16_t
      = std::chrono::duration<uint16_t, std::chrono::milliseconds::period>;

    // No default constructor: we always need an abort source.
    basic_retry_chain_node() = delete;

    /// Create a head of the chain without backoff but with abort_source
    explicit basic_retry_chain_node(ss::abort_source& as);
    /// Creates a head with the provided abort_source, deadline, and
    /// backoff granularity.
    basic_retry_chain_node(
      ss::abort_source& as, time_point deadline, duration initial_backoff);
    basic_retry_chain_node(
      ss::abort_source& as,
      time_point deadline,
      duration initial_backoff,
      retry_strategy retry_strategy);
    basic_retry_chain_node(
      ss::abort_source& as, duration timeout, duration initial_backoff);
    basic_retry_chain_node(
      ss::abort_source& as,
      duration timeout,
      duration initial_backoff,
      retry_strategy retry_strategy);

    /// Create a head of the chain without backoff but with context.
    explicit basic_retry_chain_node(basic_retry_chain_context<Clock>& ctx);
    /// Creates a head with the provided context, deadline, and
    /// backoff granularity.
    basic_retry_chain_node(
      basic_retry_chain_context<Clock>& ctx,
      time_point deadline,
      duration initial_backoff);
    basic_retry_chain_node(
      basic_retry_chain_context<Clock>& ctx,
      time_point deadline,
      duration initial_backoff,
      retry_strategy retry_strategy);
    basic_retry_chain_node(
      basic_retry_chain_context<Clock>& ctx,
      duration timeout,
      duration initial_backoff);
    basic_retry_chain_node(
      basic_retry_chain_context<Clock>& ctx,
      duration timeout,
      duration initial_backoff,
      retry_strategy retry_strategy);
    /// Create a node attached to the parent.
    /// The node will share the time budget and backoff granularity with
    /// the parent.
    /// This isn't a copy c-tor!
    explicit basic_retry_chain_node(basic_retry_chain_node* parent);
    basic_retry_chain_node(
      retry_strategy retry_strategy, basic_retry_chain_node* parent);
    /// Create a node attached to the parent.
    /// The node will share the time budget with the parent.
    /// The initial backoff can be set explicitly.
    basic_retry_chain_node(
      duration initial_backoff, basic_retry_chain_node* parent);
    basic_retry_chain_node(
      duration initial_backoff,
      retry_strategy retry_strategy,
      basic_retry_chain_node* parent);
    /// Create a node attached to the parent.
    /// The initial backoff and deadline can be set explicitly.
    basic_retry_chain_node(
      time_point deadline,
      duration initial_backoff,
      basic_retry_chain_node* parent);
    basic_retry_chain_node(
      time_point deadline,
      duration initial_backoff,
      retry_strategy retry_strategy,
      basic_retry_chain_node* parent);
    basic_retry_chain_node(
      duration timeout,
      duration initial_backoff,
      basic_retry_chain_node* parent);
    basic_retry_chain_node(
      duration timeout,
      duration initial_backoff,
      retry_strategy retry_strategy,
      basic_retry_chain_node* parent);
    /// D-tor (performs some validaton steps and can fail)
    ~basic_retry_chain_node();
    basic_retry_chain_node(const basic_retry_chain_node&) = delete;
    basic_retry_chain_node& operator=(const basic_retry_chain_node&) = delete;
    basic_retry_chain_node(basic_retry_chain_node&&) = delete;
    basic_retry_chain_node& operator=(basic_retry_chain_node&&) = delete;

    /// Generate formatted log prefix in the following format:
    /// [fiber42~3~1|2|100ms] where 'fiber42~3~1' is a unique fiber id
    /// the ~ delimits individual node ids, '2' is a number of retries
    /// (0 - no retries), and 100ms is a remaining time budget.
    ss::sstring operator()() const;

    /// Generate formattend log prefix and add custom string into it:
    /// Example: [fiber42~3~1|2|100ms ns/topic/42]
    template<typename... Args>
    ss::sstring
    operator()(fmt::format_string<Args...> format_str, Args&&... args) const {
        fmt::memory_buffer mbuf;
        auto bii = std::back_insert_iterator(mbuf);
        bii = '[';
        format(bii);
        bii = ' ';
        fmt::format_to(bii, format_str, std::forward<Args>(args)...);
        bii = ']';
        return ss::sstring(mbuf.data(), mbuf.size());
    }

    /// Find abort source in the root of the tree
    /// Always traverses the tree back to the root and returns the abort
    /// source if it was set in the root c-tor.
    ss::abort_source& root_abort_source();

    /// Return true if both retry chains share the same
    /// root.
    bool same_root(const basic_retry_chain_node& other) const;

    /// \brief Request retry
    ///
    /// The retry can be allowed or disallowed. The caller can call this
    /// method before the actual retry is needed or after. Subsequent
    /// calls to this method increment the retry counter and generate
    /// larger backoff intervals (if backoff strategy is used).
    /// The generated backoff interval is guaranteed to be short enough
    /// for the sleep to awake inside the allowed time interval.
    /// @return retry permit object
    retry_permit retry();

    uint16_t retry_count() const { return _retry; }

    /// Requests abort using the abort_source set in a root node c-tor.
    ///
    /// Relatively expensive operation. Follows the links to the root.
    /// If the abort_source was set requests abort.
    void request_abort();

    /// Check if abort was requested (throws if requested)
    ///
    /// Relatively expensive operation. Follows the links to the root.
    /// Checks if the abort_source was provided and abort was requested.
    void check_abort() const;

    void maybe_add_trace(const ss::sstring&) const;

    /// Return backoff duration that should be used before the next retry
    duration get_backoff() const;

    /// Return polling interval
    duration get_poll_interval() const;

    /// Return timeout value (time interval before the deadline)
    duration get_timeout() const;

    /// Return deadline time
    time_point get_deadline() const;

    /// Return root node of the retry chain
    const basic_retry_chain_node* get_root() const;

    bool has_retry_chain_context() const;

private:
    void format(std::back_insert_iterator<fmt::memory_buffer>& bii) const;

    uint16_t add_child();

    void rem_child();

    uint16_t get_len() const;

    /// Fetch parent of the node
    /// Method returns nullptr if root
    basic_retry_chain_node* get_parent();
    const basic_retry_chain_node* get_parent() const;

    /// Fetch abort source of the node
    /// Method returns nullptr if not root
    ss::abort_source* get_abort_source();
    const ss::abort_source* get_abort_source() const;

    /// Retry strategy to be used by this node
    retry_strategy _retry_strategy{retry_strategy::backoff};
    /// This node's id
    uint16_t _id;
    /// Number of retries
    uint16_t _retry{0};
    /// Number of child nodes
    uint16_t _num_children{0};
    /// Index of the next child node (used to generate ids)
    uint16_t _fanout_id{0};
    /// Initial backoff value
    milliseconds_uint16_t _backoff;
    /// Deadline for retry attempts
    time_point _deadline;
    /// optional parent node or (if root) abort source or the context object
    std::variant<basic_retry_chain_node*, ss::abort_source*, context_t*>
      _parent;
};

using retry_chain_node = basic_retry_chain_node<ss::lowres_clock>;

/// Logger that adds context from retry_chain_node to the output
template<class Clock>
class basic_retry_chain_logger final {
public:
    /// Make logger that adds retry_chain_node id to every message
    basic_retry_chain_logger(
      ss::logger& log, basic_retry_chain_node<Clock>& node)
      : _log(log)
      , _node(node)
      , _has_tracing(node.has_retry_chain_context()) {}
    /// Make logger that adds retry_chain_node id and custom string
    /// to every message
    basic_retry_chain_logger(
      ss::logger& log, basic_retry_chain_node<Clock>& node, ss::sstring context)
      : _log(log)
      , _node(node)
      , _ctx(std::move(context))
      , _has_tracing(node.has_retry_chain_context()) {}
    template<typename... Args>
    void
    log(ss::log_level lvl, fmt::format_string<Args...> format, Args&&... args)
      const {
        if (!_has_tracing && !_log.is_enabled(lvl)) {
            return;
        }
        auto msg = ssx::sformat(format, std::forward<Args>(args)...);
        if (_has_tracing) {
            _node.maybe_add_trace(msg);
        }
        if (_log.is_enabled(lvl)) {
            auto lambda = [&](ss::logger& logger, ss::log_level lvl) {
                if (_ctx) {
                    logger.log(lvl, "{} - {}", _node("{}", _ctx.value()), msg);
                } else {
                    logger.log(lvl, "{} - {}", _node(), msg);
                }
            };
            do_log(lvl, std::move(lambda));
        }
    }
    template<typename... Args>
    void error(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::error, format, std::forward<Args>(args)...);
    }
    template<typename... Args>
    void warn(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::warn, format, std::forward<Args>(args)...);
    }
    template<typename... Args>
    void info(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::info, format, std::forward<Args>(args)...);
    }
    template<typename... Args>
    void debug(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::debug, format, std::forward<Args>(args)...);
    }
    template<typename... Args>
    void trace(fmt::format_string<Args...> format, Args&&... args) const {
        log(ss::log_level::trace, format, std::forward<Args>(args)...);
    }
    /// Invoke the lambda function but disable tracing while the function is
    /// running.
    template<class Fn>
    void bypass_tracing(Fn&& fn) {
        auto d = ss::defer([this, t = _has_tracing] { _has_tracing = t; });
        std::exception_ptr err;
        auto tmp = _has_tracing;
        _has_tracing = false;
        try {
            std::invoke(std::forward<Fn>(fn));
        } catch (...) {
            err = std::current_exception();
        }
        _has_tracing = tmp;
        if (err) {
            std::rethrow_exception(err);
        }
    }

private:
    void __attribute__((noinline)) do_log(
      ss::log_level lvl,
      ss::noncopyable_function<void(ss::logger&, ss::log_level)>) const;

    ss::logger& _log;
    const basic_retry_chain_node<Clock>& _node;
    std::optional<ss::sstring> _ctx;
    bool _has_tracing;
};

using retry_chain_logger = basic_retry_chain_logger<ss::lowres_clock>;
using retry_chain_context = basic_retry_chain_context<ss::lowres_clock>;
