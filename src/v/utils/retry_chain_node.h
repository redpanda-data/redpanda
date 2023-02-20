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
 * we have a fiber that spauns multiple fibers which in turn
 * can spaun even more fibers. It's a normal situation with the
 * Seastar but the problem has many dimentions:
 * - logging is tricky since every shard runs many concurrent
 *   futures and it's hard to correlate messages with each other
 * - retrying things is also tricky because the parent fiber can
 *   have some time limitation but child fibers might not finish
 *   in time.
 * - aborting is only possible from top to bottom, it's not possible
 *   for the child fiber to abort an entire tree
 * Provided mechanism can solve this two issues by creating an
 * explicit tree of dependency between the fibers and tracking
 * retries and fiber identities/relationships.
 *
 * Code sample (problem):
 *
 * ss::future<reply_t> endpoint::send_request(request req) {
 *   while (true) {
 *     try {
 *       co_retury co_await do_send_request(req);
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
 *   using result_fut = std::future<reply>;
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
 *       co_retury co_await do_send_request(req);
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
 *   using result_fut = std::future<reply>;
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
 *      auo res = co_await ss::when_all(fut.begin(), fut.end());
 *      for(auto& f: res) {
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
 * leafs of the tree. The 'abort_source' is propagated form the root, so any
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

#include "random/fast_prng.h"
#include "seastarx.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <chrono>
#include <variant>

/// Retry strategy
enum class retry_strategy {
    /// Simple polling
    polling,
    /// Exponential backoff
    backoff,
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
/// timeout value, or backoff interval in c-tor. This parameters are used
/// by all child nodes. They have to respect the timeout of the root and
/// can't set larger timeout.
/// The leaf node receives a pointer to a parent node (leaf or root).
/// It can also receive its own timeout value and backoff interval. The
/// timeout value have to be smaller that the one of the parent node.
class retry_chain_node {
public:
    using milliseconds_uint16_t
      = std::chrono::duration<uint16_t, std::chrono::milliseconds::period>;

    // No default constructor: we always need an abort source.
    retry_chain_node() = delete;

    /// Create a head of the chain without backoff but with abort_source
    explicit retry_chain_node(ss::abort_source& as);
    /// Creates a head with the provided abort_source, deadline, and
    /// backoff granularity.
    retry_chain_node(
      ss::abort_source& as,
      ss::lowres_clock::time_point deadline,
      ss::lowres_clock::duration initial_backoff);
    retry_chain_node(
      ss::abort_source& as,
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration initial_backoff);
    /// Create a node attached to the parent.
    /// The node will share the time budget and backoff granularity with
    /// the parent.
    /// This isn't a copy c-tor!
    explicit retry_chain_node(retry_chain_node* parent);
    /// Create a node attached to the parent.
    /// The node will share the time budget with the parent.
    /// The initial backoff can be set explicitly.
    retry_chain_node(
      ss::lowres_clock::duration initial_backoff, retry_chain_node* parent);
    /// Create a node attached to the parent.
    /// The initial backoff and deadline can be set explicitly.
    retry_chain_node(
      ss::lowres_clock::time_point deadline,
      ss::lowres_clock::duration initial_backoff,
      retry_chain_node* parent);
    retry_chain_node(
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration initial_backoff,
      retry_chain_node* parent);
    /// D-tor (performs some validaton steps and can fail)
    ~retry_chain_node();
    retry_chain_node(const retry_chain_node&) = delete;
    retry_chain_node& operator=(const retry_chain_node&) = delete;
    retry_chain_node(retry_chain_node&&) = delete;
    retry_chain_node& operator=(retry_chain_node&&) = delete;

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
    bool same_root(const retry_chain_node& other) const;

    /// \brief Request retry
    ///
    /// The retry can be allowed or disallowed. The caller can call this
    /// method before the actual retry is needed or after. Subsequent
    /// calls to this method increment the retry counter and generate
    /// larger backoff intervals (if backoff strategy is used).
    /// The generated backoff interval is guaranteed to be short enough
    /// for the sleep to awake inside the allowed time interval.
    /// @return retry permit object
    retry_permit retry(retry_strategy st = retry_strategy::backoff);

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

    /// Return backoff duration that should be used before the next retry
    ss::lowres_clock::duration get_backoff() const;

    /// Return polling interval
    ss::lowres_clock::duration get_poll_interval() const;

    /// Return timeout value (time interval before the deadline)
    ss::lowres_clock::duration get_timeout() const;

    /// Return deadline time
    ss::lowres_clock::time_point get_deadline() const;

private:
    void format(std::back_insert_iterator<fmt::memory_buffer>& bii) const;

    uint16_t add_child();

    void rem_child();

    uint16_t get_len() const;

    /// Return root node of the retry chain
    const retry_chain_node* get_root() const;

    /// Fetch parent of the node
    /// Method returns nullptr if root
    retry_chain_node* get_parent();
    const retry_chain_node* get_parent() const;

    /// Fetch abort source of the node
    /// Method returns nullptr if not root
    ss::abort_source* get_abort_source();
    const ss::abort_source* get_abort_source() const;

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
    ss::lowres_clock::time_point _deadline;
    /// optional parent node or (if root) abort source for all fibers
    std::variant<retry_chain_node*, ss::abort_source*> _parent;
};

/// Logger that adds context from retry_chain_node to the output
class retry_chain_logger final {
public:
    /// Make logger that adds retry_chain_node id to every message
    retry_chain_logger(ss::logger& log, retry_chain_node& node)
      : _log(log)
      , _node(node) {}
    /// Make logger that adds retry_chain_node id and custom string
    /// to every message
    retry_chain_logger(
      ss::logger& log, retry_chain_node& node, ss::sstring context)
      : _log(log)
      , _node(node)
      , _ctx(std::move(context)) {}
    template<typename... Args>
    void
    log(ss::log_level lvl, fmt::format_string<Args...> format, Args&&... args)
      const {
        if (_log.is_enabled(lvl)) {
            auto lambda = [&](ss::logger& logger, ss::log_level lvl) {
                auto msg = ssx::sformat(format, std::forward<Args>(args)...);
                if (_ctx) {
                    logger.log(
                      lvl,
                      "{} - {}",
                      _node("{}", _ctx.value()),
                      std::move(msg));
                } else {
                    logger.log(lvl, "{} - {}", _node(), std::move(msg));
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

private:
    void __attribute__((noinline)) do_log(
      ss::log_level lvl,
      ss::noncopyable_function<void(ss::logger&, ss::log_level)>) const;

    ss::logger& _log;
    const retry_chain_node& _node;
    std::optional<ss::sstring> _ctx;
};
