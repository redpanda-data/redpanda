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

#include "cluster/commands.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "seastar/core/abort_source.hh"
#include "seastar/core/weak_ptr.hh"

#include <absl/container/flat_hash_map.h>

namespace cluster {

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
    ss::future<> await(T value, ss::abort_source& as) {
        auto item = std::make_unique<wait_item>(value);
        auto fut = item->p.get_future();

        auto sub_opt = as.subscribe(
          [item_ptr = item->weak_from_this()]() noexcept {
              if (item_ptr) {
                  item_ptr->p.set_exception(ss::abort_requested_exception());
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
        for (auto& wi : items) {
            if (wi->data == value) {
                wi->p.set_value();
            } else {
                _items.push_back(std::move(wi));
            }
        }
    };
};

/// Class containing information about cluster members. The members class is
/// instantiated on each core. Cluster members updates are comming directly from
/// cluster::members_manager
class members_table {
public:
    using broker_ptr = ss::lw_shared_ptr<model::broker>;

    std::vector<broker_ptr> all_brokers() const;

    std::vector<model::node_id> all_broker_ids() const;

    /// Returns single broker if exists in cache
    std::optional<broker_ptr> get_broker(model::node_id) const;

    std::vector<model::node_id> get_decommissioned() const;

    bool contains(model::node_id) const;

    void update_brokers(model::offset, const std::vector<model::broker>&);

    std::error_code apply(model::offset, decommission_node_cmd);
    std::error_code apply(model::offset, recommission_node_cmd);

    model::revision_id version() const { return _version; }

    ss::future<> await_membership(model::node_id id, ss::abort_source& as) {
        if (!contains(id)) {
            return _waiters.await(id, as);
        } else {
            return ss::now();
        }
    }

private:
    using broker_cache_t = absl::flat_hash_map<model::node_id, broker_ptr>;
    broker_cache_t _brokers;
    model::revision_id _version;

    waiter_queue<model::node_id> _waiters;
};
} // namespace cluster
