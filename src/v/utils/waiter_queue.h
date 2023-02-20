/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>

/**
 * Generic 'list of waiters' helper, where a waiter is waiting for a particular
 * item of type `T`, and the owner of the queue is responsible for calling
 * `notify` with a value of type `T` to wake up all the fibers waiting for
 * that value.
 *
 * Waiters are woken in the same order they called `await`.
 *
 * If the abort source passed into `await` fires, then an exceptional
 * future is returned to the caller with `abort_requested_exception`: callers
 * should handle that gracefully (this is already done if the caller is inside
 * a ssx::spawn_with_gate or similar helper).
 */
template<typename T>
class waiter_queue {
    struct wait_item : ss::weakly_referencable<wait_item> {
        wait_item(T data_)
          : data(std::move(data_)) {}

        T data;
        ss::promise<> p;
        ss::abort_source::subscription abort_sub;
    };

    std::vector<std::unique_ptr<wait_item>> _items;

public:
    ~waiter_queue() { abort_all(); }

    /**
     * Callers of await() *should* use abort sources that fire
     * before this object is destroyed.  However, to make it
     * easier to reason about safety during shutdown, we also
     * explicitly evict them all.
     */
    void abort_all() {
        for (auto& i : _items) {
            i->p.set_exception(ss::abort_requested_exception());
        }
        _items.clear();
    }

    ss::future<> await(T value, ss::abort_source& as) {
        auto item = std::make_unique<wait_item>(value);
        auto fut = item->p.get_future();

        auto sub_opt = as.subscribe(
          [this, item_ptr = item->weak_from_this()]() noexcept {
              if (item_ptr) {
                  item_ptr->p.set_exception(ss::abort_requested_exception());
                  std::erase_if(
                    _items, [item_ptr](const std::unique_ptr<wait_item>& it) {
                        return it.get() == item_ptr.get();
                    });
              }
          });

        if (!sub_opt) {
            // Abort source already fired!
            return ss::make_exception_future<>(ss::abort_requested_exception());
        } else {
            item->abort_sub = std::move(*sub_opt);
        }

        _items.emplace_back(std::move(item));
        return fut;
    }
    void notify(const T& value) {
        auto items = std::exchange(_items, {});
        _items.reserve(items.size());
        for (auto& wi : items) {
            if (wi->data == value) {
                wi->p.set_value();
            } else {
                _items.push_back(std::move(wi));
            }
        }
        _items.shrink_to_fit();
    };
};
