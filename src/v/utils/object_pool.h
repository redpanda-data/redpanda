/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <concepts>
#include <deque>
#include <stack>

/*
 * This class provides a pool of objects that can allocated or released to.
 *
 * The pool doesn't allocate objects internally and instead relies on users
 * releasing an initial group of objects to it.
 *
 */
template<std::movable T>
class object_pool {
    struct wait_item {
        ss::promise<T> p;
        ss::abort_source::subscription abort_sub;
    };

public:
    /*
     * This function tries to retrieve an object from the pool and return it to
     * the caller.
     *
     * If an object is available in the pool already then it is returned right
     * away as a ready future.
     *
     * Otherwise the returned future is set when a object is released back to
     * the pool in a first come first serve order.
     */
    ss::future<T> allocate_object(ss::abort_source& as) {
        // Return any available object right away without setting up
        // a waiter.
        if (!_objects.empty()) {
            auto ret = ss::make_ready_future<T>(std::move(_objects.top()));
            _objects.pop();
            return ret;
        }

        auto item = std::make_unique<wait_item>();

        auto sub_opt = as.subscribe([this, item_ptr = item.get()]() noexcept {
            // it should be safe to use a raw pointer as the lifetime of this
            // callback is the lifetime of the allocation the pointer points to.
            item_ptr->p.set_exception(ss::abort_requested_exception());
            std::erase_if(
              _waiters, [item_ptr](const std::unique_ptr<wait_item>& it) {
                  return it.get() == item_ptr;
              });
        });

        if (!sub_opt) {
            // Abort source already fired!
            return ss::make_exception_future<T>(
              ss::abort_requested_exception());
        }

        item->abort_sub = std::move(*sub_opt);
        _waiters.emplace_back(std::move(item));
        return _waiters.back()->p.get_future();
    }

    /*
     * This function releases an object back into the pool.
     *
     * If there are any waiters to allocate an object from the pool
     * then the oldest waiter is given the object.
     *
     * Otherwise the object is returned to the pool.
     */
    void release_object(T obj) {
        if (!_waiters.empty()) {
            _waiters.front()->p.set_value(std::move(obj));
            _waiters.pop_front();
        } else {
            _objects.emplace(std::move(obj));
        }
    }

    struct scoped_object {
        scoped_object(scoped_object&& so) noexcept
          : _obj(std::move(so._obj))
          , _pool(so._pool) {
            so._pool = std::nullopt;
        }

        scoped_object(T obj, object_pool<T>& pool) noexcept
          : _obj(std::move(obj))
          , _pool(pool) {}

        scoped_object(const scoped_object&) = delete;

        ~scoped_object() {
            if (_pool) {
                _pool->get().release_object(std::move(_obj));
            }
        }

        T& operator*() noexcept { return _obj; }

    private:
        T _obj;
        std::optional<std::reference_wrapper<object_pool<T>>> _pool;
    };

    /*
     * This function allocates a `scoped_object` from the pool.
     *
     * This scoped_object automatically releases the underlying object to the
     * pool when its lifetime ends.
     */
    ss::future<scoped_object> allocate_scoped_object(ss::abort_source& as) {
        return allocate_object(as).then(
          [this](T obj) { return scoped_object(std::move(obj), *this); });
    }

private:
    // try to perserve any locality by reusing the
    // most recently used object
    std::stack<T> _objects;
    std::deque<std::unique_ptr<wait_item>> _waiters;
};
