// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <algorithm>
#include <functional>
#include <type_traits>
#include <vector>

namespace lsm::db {

/// An immutable (persistent) sorted tree.
///
/// All mutation operations return a new tree that shares structure with the
/// original via ss::lw_shared_ptr. This makes it safe to hold references to
/// old versions of the tree while new versions are created.
///
/// Key-value data within each node is shared via ss::lw_shared_ptr so that
/// path-copying during tree operations only copies pointers, not actual data.
/// This means neither Key nor Value needs to be copy constructible.
template<typename Key, typename Value, typename Compare = std::less<Key>>
class immutable_tree {
    struct entry {
        entry(Key k, Value v)
          : key(std::move(k))
          , value(std::move(v)) {}

        Key key;
        Value value;
    };
    using entry_ptr = ss::lw_shared_ptr<entry>;

    struct node {
        node(
          entry_ptr d,
          // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
          ss::lw_shared_ptr<node> l,
          ss::lw_shared_ptr<node> r,
          int h)
          : data(std::move(d))
          , left(std::move(l))
          , right(std::move(r))
          , height(h) {}

        entry_ptr data;
        ss::lw_shared_ptr<node> left;
        ss::lw_shared_ptr<node> right;
        int height;
    };
    using node_ptr = ss::lw_shared_ptr<node>;

    node_ptr _root;
    size_t _size = 0;
    Compare _cmp;

    immutable_tree(node_ptr root, size_t size, Compare cmp)
      : _root(std::move(root))
      , _size(size)
      , _cmp(std::move(cmp)) {}

public:
    /// Create an empty tree.
    immutable_tree() = default;

    /// Create an empty tree with a custom comparator.
    explicit immutable_tree(Compare cmp)
      : _cmp(std::move(cmp)) {}

    /// Insert a key-value pair, returning a new tree.
    ///
    /// If the key already exists, the value is replaced. The original tree
    /// is not modified.
    immutable_tree insert(Key key, Value value) const {
        bool replaced = false;
        auto e = ss::make_lw_shared<entry>(std::move(key), std::move(value));
        auto new_root = do_insert(_root, std::move(e), replaced);
        return {std::move(new_root), replaced ? _size : _size + 1, _cmp};
    }

    /// Remove a key from the tree, returning a new tree.
    ///
    /// If the key does not exist, returns a copy of this tree.
    immutable_tree remove(const Key& key) const {
        bool removed = false;
        auto new_root = do_remove(_root, key, removed);
        if (!removed) {
            return *this;
        }
        return {std::move(new_root), _size - 1, _cmp};
    }

    /// Look up a key, returning a pointer to the value or nullptr.
    const Value* get(const Key& key) const {
        auto curr = _root;
        while (curr) {
            if (_cmp(key, curr->data->key)) {
                curr = curr->left;
            } else if (_cmp(curr->data->key, key)) {
                curr = curr->right;
            } else {
                return &curr->data->value;
            }
        }
        return nullptr;
    }

    /// Iterate over all entries in sorted order.
    ///
    /// The callback is invoked as fn(const Key&, const Value&) for each
    /// entry. Uses an explicit stack instead of recursion.
    ///
    /// If the callback returns void, iteration is synchronous. If it
    /// returns ss::future<>, iteration is asynchronous and the returned
    /// future must be awaited.
    template<typename Fn>
    auto for_each(Fn fn) const {
        using R = std::invoke_result_t<Fn&, const Key&, const Value&>;
        if constexpr (std::is_void_v<R>) {
            if (!_root) {
                return;
            }
            std::vector<node_ptr> stack;
            stack.reserve(_root->height);
            auto curr = _root;
            while (curr || !stack.empty()) {
                while (curr) {
                    stack.push_back(curr);
                    curr = curr->left;
                }
                curr = std::move(stack.back());
                stack.pop_back();
                fn(curr->data->key, curr->data->value);
                curr = curr->right;
            }
        } else {
            return do_for_each_async(std::move(fn));
        }
    }

    /// The number of entries in the tree.
    size_t size() const { return _size; }

    /// Whether the tree is empty.
    bool empty() const { return !_root; }

private:
    template<typename Fn>
    ss::future<> do_for_each_async(Fn fn) const {
        if (!_root) {
            co_return;
        }
        std::vector<node_ptr> stack;
        stack.reserve(_root->height);
        auto curr = _root;
        while (curr || !stack.empty()) {
            while (curr) {
                stack.push_back(curr);
                curr = curr->left;
            }
            curr = std::move(stack.back());
            stack.pop_back();
            co_await fn(curr->data->key, curr->data->value);
            curr = curr->right;
        }
    }

    static int node_height(const node_ptr& n) { return n ? n->height : 0; }

    static int balance_factor(const node_ptr& n) {
        return n ? node_height(n->left) - node_height(n->right) : 0;
    }

    static node_ptr make_node(entry_ptr data, node_ptr left, node_ptr right) {
        int h = 1 + std::max(node_height(left), node_height(right));
        return ss::make_lw_shared<node>(
          std::move(data), std::move(left), std::move(right), h);
    }

    static node_ptr rotate_right(const node_ptr& y) {
        const auto& x = y->left;
        auto new_y = make_node(y->data, x->right, y->right);
        return make_node(x->data, x->left, std::move(new_y));
    }

    static node_ptr rotate_left(const node_ptr& x) {
        const auto& y = x->right;
        auto new_x = make_node(x->data, x->left, y->left);
        return make_node(y->data, std::move(new_x), y->right);
    }

    static node_ptr do_balance(node_ptr n) {
        int bf = balance_factor(n);
        if (bf > 1) {
            // Left-Right case
            if (balance_factor(n->left) < 0) {
                auto new_left = rotate_left(n->left);
                n = make_node(n->data, std::move(new_left), n->right);
            }
            return rotate_right(n);
        }
        if (bf < -1) {
            // Right-Left case
            if (balance_factor(n->right) > 0) {
                auto new_right = rotate_right(n->right);
                n = make_node(n->data, n->left, std::move(new_right));
            }
            return rotate_left(n);
        }
        return n;
    }

    node_ptr
    do_insert(const node_ptr& root, entry_ptr e, bool& replaced) const {
        if (!root) {
            replaced = false;
            return make_node(std::move(e), nullptr, nullptr);
        }
        if (_cmp(e->key, root->data->key)) {
            auto new_left = do_insert(root->left, std::move(e), replaced);
            return do_balance(
              make_node(root->data, std::move(new_left), root->right));
        }
        if (_cmp(root->data->key, e->key)) {
            auto new_right = do_insert(root->right, std::move(e), replaced);
            return do_balance(
              make_node(root->data, root->left, std::move(new_right)));
        }
        // Key already exists, replace the value.
        replaced = true;
        return make_node(std::move(e), root->left, root->right);
    }

    static const node_ptr& find_min(const node_ptr& n) {
        const node_ptr* curr = &n;
        while ((*curr)->left) {
            curr = &(*curr)->left;
        }
        return *curr;
    }

    static node_ptr remove_min(const node_ptr& root) {
        if (!root->left) {
            return root->right;
        }
        auto new_left = remove_min(root->left);
        return do_balance(
          make_node(root->data, std::move(new_left), root->right));
    }

    node_ptr
    do_remove(const node_ptr& root, const Key& key, bool& removed) const {
        if (!root) {
            removed = false;
            return nullptr;
        }
        if (_cmp(key, root->data->key)) {
            auto new_left = do_remove(root->left, key, removed);
            if (!removed) {
                return root;
            }
            return do_balance(
              make_node(root->data, std::move(new_left), root->right));
        }
        if (_cmp(root->data->key, key)) {
            auto new_right = do_remove(root->right, key, removed);
            if (!removed) {
                return root;
            }
            return do_balance(
              make_node(root->data, root->left, std::move(new_right)));
        }
        // Found the key.
        removed = true;
        if (!root->left) {
            return root->right;
        }
        if (!root->right) {
            return root->left;
        }
        // Two children: replace with the in-order successor.
        const auto& successor = find_min(root->right);
        auto new_right = remove_min(root->right);
        return do_balance(
          make_node(successor->data, root->left, std::move(new_right)));
    }
};

} // namespace lsm::db
