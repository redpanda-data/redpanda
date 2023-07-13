#pragma once

#include "raft/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace raft {

class consensus;
/**
 * This class allows to buffer append entries request  waiting for consensus op
 * mutex and when mutex is eventually available execute all buffered calls
 * without releasing mutex in between single calls. Additionally the
 * append_entries buffer leverages the fact that single flush after all log
 * append is enough to synchronize data on disk and guarantee safety. This
 * approach leverages the fact that our append are much faster (up to hundreds
 * of microseconds) compared to flush.
 *
 * Standard append entries processing:
 *
 *  +---------------+              +-----------+   +-------+ +-----+
 *  | raft_service  |              | consensus |   | mutex | | log |
 *  +---------------+              +-----------+   +-------+ +-----+
 *          |                            |             |        |
 *          | append_entries             |             |        |
 *          |--------------------------->|             |        |
 *          |                            |             |        |
 *          |                            | acquire     |        |
 *          |                            |------------>|        |
 *          |                            |             |        |
 *          |                            |     granted |        |
 *          |                            |<------------|        |
 *          |                            |             |        |
 *          |                            | append      |        |
 *          |                            |--------------------->|
 *          |                            |             |        |
 *          |                            |        append_result |
 *          |                            |<---------------------|
 *          |                            |             |        |
 *          |                            | flush       |        |
 *          |                            |--------------------->|
 *          |                            |             |        |
 *          |                            |             |     ok |
 *          |                            |<---------------------|
 *          |                            |             |        |
 *          |       append entries reply |             |        |
 *          |<---------------------------|             |        |
 *          |                            |             |        |
 *          |                            | release     |        |
 *          |                            |------------>|        |
 *          |                            |             |        |
 *          |                            |             |        |
 *          | next append_entries        |             |        |
 *          |--------------------------->|             |        |
 *          |                            |             |        |
 *
 * With append_entries_buffer:
 *
 * +---------------+             +-----------+   +--------+    +-------+ +-----+
 * | raft_service  |             | consensus |   | buffer |    | mutex | | log |
 * +---------------+             +-----------+   +--------+    +-------+ +-----+
 *         |                           |            |                 |        |
 *         | append_entries            |            |                 |        |
 *         |-------------------------->|            |                 |        |
 *         |                           |            |                 |        |
 *         |                           | enqueue    |                 |        |
 *         |                           |----------->|                 |        |
 *         |                           |            |                 |        |
 *         | next append_entries       |            |                 |        |
 *         |-------------------------->|            |                 |        |
 *         |                           |            |                 |        |
 *         |                           | enqueue    |                 |        |
 *         |                           |----------->|                 |        |
 *         |                           |            |                 |        |
 *         |                           |            | acquire         |        |
 *         |                           |            |---------------->|        |
 *         |                           |            |                 |        |
 *         |                           |            |         granted |        |
 *         |                           |            |<----------------|        |
 *         |                           |            |                 |        |
 *         |                           |            | append          |        |
 *         |                           |            |------------------------->|
 *         |                           |            |                 |        |
 *         |                           |            |            append_result |
 *         |                           |<--------------------------------------|
 *         |                           |            |                 |        |
 *         |                           |            | next append     |        |
 *         |                           |            |------------------------->|
 *         |                           |            |                 |        |
 *         |                           |            |            append_result |
 *         |                           |<--------------------------------------|
 *         |                           |            |                 |        |
 *         |                           | flush      |                 |        |
 *         |                           |-------------------------------------->|
 *         |                           |            |                 |        |
 *         |                           |            |                 |   ok   |
 *         |                           |<--------------------------------------|
 *         |                           |            |                 |        |
 *         | append entries reply      |            |                 |        |
 *         |<---------------------------------------|                 |        |
 *         |                           |            |                 |        |
 *         | next append entries reply |            |                 |        |
 *         |<---------------------------------------|                 |        |
 *         |                           |            |                 |        |
 *         |                           | release    |                 |        |
 *         |                           |----------------------------->|        |
 *         |                           |            |                 |        |
 *
 * Described approach allows us to reduce number of expensive log::flush
 * operations.
 *
 * The class is taking additional advantage by setting all replies committed
 * offset to the latest value updated by log::flush. Since all the requests were
 * already sent by the leader it means that leader already appendend all
 * entries to its local log. It can therefore update commit_index as soon as it
 * will receive the first response, still being correct and guaranteeing safety.
 *
 * Note on backpressure handling:
 *
 * The backpressure is handled using condition variable. When buffer has free
 * space we notify waiters.
 */
class append_entries_buffer {
public:
    explicit append_entries_buffer(consensus&, size_t max_buffered_elements);

    ss::future<append_entries_reply> enqueue(append_entries_request&& r);

    void start();
    ss::future<> stop();

private:
    /**
     * Use 32 items per chunk as usually the buffer size is small
     */
    using request_t = ss::chunked_fifo<append_entries_request, 32>;
    using response_t = ss::chunked_fifo<ss::promise<append_entries_reply>, 32>;
    using reply_t = std::variant<append_entries_reply, std::exception_ptr>;
    using reply_list_t = ss::chunked_fifo<reply_t, 32>;

    ss::future<> flush();
    ss::future<> do_flush(request_t, response_t, ssx::semaphore_units);

    void propagate_results(reply_list_t, response_t);

    consensus& _consensus;
    request_t _requests;
    response_t _responses;
    ss::condition_variable _enqueued;
    ss::gate _gate;
    ss::condition_variable _flushed;
    const size_t _max_buffered;
};

} // namespace raft
