// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/optimized_optional.hh>

namespace lsm::db {

// A mixin for a intrusive linked list that is weakly held. So only something
// other than the head element is strongly referenced directly, but if there are
// references outside the list to older elements they are kept alive and are
// reachable via list iteration.
//
// This is extracted out as a utility so that it can be independantly tested.
template<typename T>
class weak_intrusive_list : public ss::enable_lw_shared_from_this<T> {
public:
    // Push new_node to the head of the current list, which is `head`.
    static void
    push_front(ss::lw_shared_ptr<T>* head, ss::lw_shared_ptr<T> new_node) {
        if (*head) {
            new_node->_next = head->get();
            (*head)->_prev = new_node.get();
        }
        *head = std::move(new_node);
    };
    weak_intrusive_list() = default;
    weak_intrusive_list(const weak_intrusive_list&) = delete;
    weak_intrusive_list(weak_intrusive_list&&) = delete;
    weak_intrusive_list& operator=(const weak_intrusive_list&) = delete;
    weak_intrusive_list& operator=(weak_intrusive_list&&) = delete;
    ~weak_intrusive_list() {
        // Remove myself from the linked list.
        if (_prev) {
            (*_prev)->_next = _next;
        }
        if (_next) {
            (*_next)->_prev = _prev;
        }
    }

    // Get the next element in the linked list if it exists.
    //
    // Maybe `nullptr` if at the end of the list.
    ss::optimized_optional<ss::lw_shared_ptr<T>> next() {
        if (_next) {
            return (*_next)->shared_from_this();
        }
        return {};
    }

private:
    ss::optimized_optional<T*> _prev = nullptr;
    ss::optimized_optional<T*> _next = nullptr;
};

} // namespace lsm::db
