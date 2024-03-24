/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "io/page.h"
#include "io/persistence.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>

namespace experimental::io {

namespace testing_details {
class io_queue_accessor;
}; // namespace testing_details

/**
 * Manager of I/O requests for a single file.
 *
 * An I/O request is added to the queue using the `submit_*` interfaces. For
 * example, a page may be read or written by passing a reference to the page to
 * either `submit_read` or `submit_write`.
 *
 * There is no guarantee of the order in which I/O requests are processed.
 *
 * Submission of I/O requests can be made at any time, but pending I/O requests
 * are processed only when the queue is in the open state. A new I/O queue is in
 * the closed state. To open the queue use the `open()` interface which will
 * open a handle to the underlying file. To close the queue and the file handle
 * use the `close()` interface. A queue may be opened and closed any number of
 * times. This is useful if the queue is used in a context that control the
 * maximum number of open file handles or wants to pause I/O for some reason.
 */
class io_queue {
public:
    /**
     * Callback invoked when the scheduler completes an operation.
     *
     * Write completions are delivered only if after the write has completed the
     * operation will not be requeued.
     */
    using completion_callback_type
      = seastar::noncopyable_function<void(page&) noexcept>;

    /**
     * Construct a new io_queue.
     */
    io_queue(
      persistence* storage,
      std::filesystem::path path,
      completion_callback_type complete) noexcept;

    io_queue(const io_queue&) = delete;
    io_queue& operator=(const io_queue&) = delete;
    io_queue(io_queue&&) noexcept = delete;
    io_queue& operator=(io_queue&&) noexcept = delete;
    ~io_queue() noexcept = default;

    /**
     * Starts the I/O queue's background dispatcher.
     */
    void start() noexcept;

    /**
     * Stops the I/O queue's background dispatcher.
     *
     * It is recommended that close() be invoked prior to stopping the queue.
     * While Seastar will close the file handle automatically, ensuring that the
     * queue is drained before the file handle is closed will avoid errors
     * within Seastar related to closing file handles with pending I/O.
     */
    seastar::future<> stop() noexcept;

    /**
     * Return the path of the backing file.
     */
    [[nodiscard]] const std::filesystem::path& path() const noexcept;

    /**
     * Open the queue and begin dispatching pending I/O requests.
     *
     * Requires that opened() be false. The queue will remain in the closed
     * state if an exceptional future is returned.
     */
    seastar::future<> open() noexcept;

    /**
     * Returns true if the queue is open, and false otherwise.
     */
    bool opened() const noexcept;

    /**
     * Close the queue and release the underlying file handle.
     *
     * Waits until inflight I/Os are complete. Pending I/Os remain in the
     * pending state until the queue is re-opened.
     *
     * Requires that opened() be true.
     */
    seastar::future<> close() noexcept;

    /**
     * Submit a read I/O request.
     *
     * A page submitted for read must not be resubmitted for read or write until
     * it has completed.
     */
    void submit_read(page&) noexcept;

    /**
     * Submit a write I/O request.
     *
     * If a write request is submitted for a page that had been dispatched but
     * not yet completed then it will be requeued to be written again.
     */
    void submit_write(page&) noexcept;

private:
    friend testing_details::io_queue_accessor;

    using request_list_type = intrusive_list<page, &page::io_queue_hook>;

    /*
     * file always represents path on the storage backend. the file pointer is
     * set via open() and cleared via close(). this may happen any number of
     * times during the life time of the queue.
     */
    persistence* storage_;
    std::filesystem::path path_;
    seastar::shared_ptr<persistence::file> file_;

    /*
     * submitted requests first land on the pending list. they are moved to the
     * running list when they are dispatched. when a request is completed it is
     * removed from the running list, and passed to the completion callback.
     *
     * operations are dispatched under the ops semaphore which is used to
     * synchronize completion of operations with pending calls to close().
     */
    request_list_type pending_;
    request_list_type running_;
    completion_callback_type complete_;
    seastar::semaphore ops_{seastar::semaphore::max_counter()};
    void enqueue_pending(page&) noexcept;

    /*
     * dispatcher holds the dispatch() fiber which processes the pending list of
     * requests. stop() will shutdown the dispatcher and wait for it to finish.
     */
    bool stop_{false};
    seastar::condition_variable cond_;
    seastar::gate gate_;
    seastar::future<> dispatcher_{seastar::make_ready_future<>()};
    seastar::future<> dispatch() noexcept;
    void dispatch_read(page&, seastar::semaphore_units<>) noexcept;
    void dispatch_write(page&, seastar::semaphore_units<>) noexcept;
};

} // namespace experimental::io
