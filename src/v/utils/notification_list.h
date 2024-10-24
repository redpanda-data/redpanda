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

#include <algorithm>
#include <vector>

/**
 * Enables subscription & unsubscription of multi-use synchronous callbacks
 *
 * Type C is a callback prototype that all registered callbacks must
 * fulfil.
 *
 * Type I is an integer-like type to use for registration IDs.
 *
 */
template<typename C, typename I>
class notification_list {
public:
    /**
     * Register a new callback to be invoked when notify() is called
     */
    I register_cb(C cb) {
        auto id = _notification_id++;
        _notifications.emplace_back(id, std::move(cb));
        return id;
    }

    /**
     * Deregister a callback that was previously registered with register_cb()
     */
    void unregister_cb(I id) {
        auto it = std::find_if(
          _notifications.begin(),
          _notifications.end(),
          [id](const std::pair<I, C>& n) { return n.first == id; });
        if (it != _notifications.end()) {
            _notifications.erase(it);
        }
    }

    /**
     * Call `f` on all registered callbacks
     */
    template<typename... Args>
    void notify(Args&&... args) const {
        for (auto& [_, cb] : _notifications) {
            cb(args...);
        }
    }

private:
    I _notification_id{0};
    std::vector<std::pair<I, C>> _notifications;
};
