/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "absl/container/flat_hash_map.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"

#include <seastar/core/sstring.hh>

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

/**
 * A compressed radix tree over byte-string keys, mapping each stored key to a
 * Value.
 *
 * Each node is individually heap-allocated and owned by its parent via a
 * unique_ptr; an edge is just the child pointer held in the parent's per-node
 * map. Nodes are therefore pointer-stable (never relocated), and no allocation
 * is ever a single large contiguous block.
 *
 * The edges leaving a node are "compressed": each edge is labelled with the
 * whole run of bytes consumed between two branch points, so a chain of
 * single-child nodes collapses into one edge. This keeps the node count
 * proportional to the number of distinct branch points rather than to the
 * total length of the stored keys.
 *
 */
template<typename Value>
class radix_tree {
    struct node {
        /// Bytes consumed on the edge from this node's parent into this node.
        /// Empty only for the root.
        ss::sstring label;
        /// First byte of a child's edge label -> that child. A radix tree never
        /// has two children whose edges start with the same byte, so the first
        /// byte uniquely identifies the edge to follow, and fan-out is bounded
        /// by the byte alphabet (<=256) regardless of how many keys are stored.
        absl::flat_hash_map<char, std::unique_ptr<node>> children;
        /// Set iff a key terminates exactly at this node.
        std::optional<Value> value;
    };

public:
    radix_tree()
      : _root(std::make_unique<node>()) {}

    radix_tree(radix_tree&& other) noexcept
      : _root(std::move(other._root))
      , _size(std::exchange(other._size, 0)) {}
    radix_tree& operator=(radix_tree&& other) noexcept {
        if (this != &other) {
            destroy(std::move(_root));
            _root = std::move(other._root);
            _size = std::exchange(other._size, 0);
        }
        return *this;
    }
    radix_tree(const radix_tree&) = delete;
    radix_tree& operator=(const radix_tree&) = delete;

    ~radix_tree() noexcept { destroy(std::move(_root)); }

    /// Number of distinct keys currently stored (terminal nodes).
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }

    /// Insert or overwrite the value stored at \p key.
    template<typename V>
    void insert(std::string_view key, V&& value) {
        node* cur = _root.get();
        std::string_view rest = key;
        while (true) {
            if (rest.empty()) {
                set_value(cur, std::forward<V>(value));
                return;
            }
            auto it = cur->children.find(rest.front());
            if (it == cur->children.end()) {
                // No edge starts with this byte: hang a fresh leaf carrying the
                // whole remaining suffix as its edge label.
                auto leaf = std::make_unique<node>(ss::sstring{rest});
                set_value(leaf.get(), std::forward<V>(value));
                cur->children.emplace(rest.front(), std::move(leaf));
                return;
            }
            std::unique_ptr<node>& child = it->second;
            const std::string_view label = child->label;
            const size_t common = common_prefix_len(label, rest);
            rest = rest.substr(common);
            if (common != label.size()) {
                // `rest` matches only the first `common` bytes of `child`'s
                // edge label before diverging, so split `child`'s edge there: a
                // new intermediate node takes the shared prefix and `child`
                // keeps the remaining suffix beneath it.
                split_edge(child, common);
            }
            // Descend. After a split `child`'s slot holds the new intermediate
            // node, so this lands there; otherwise it lands on the
            // fully-matched child. Either way the next iteration places the
            // rest of the key beneath it.
            cur = child.get();
        }
    }

    /// Returns a pointer to the value stored at exactly \p key, or nullptr if
    /// \p key is not a stored key. The pointer is borrowed from internal
    /// storage and is invalidated by the next mutation that removes \p key
    /// (erase/clear); do not retain it across one.
    const Value* find(std::string_view key) const {
        const node* cur = _root.get();
        std::string_view rest = key;
        while (!rest.empty()) {
            auto it = cur->children.find(rest.front());
            if (it == cur->children.end()) {
                return nullptr;
            }
            const std::string_view label = it->second->label;
            if (!rest.starts_with(label)) {
                return nullptr;
            }
            cur = it->second.get();
            rest = rest.substr(label.size());
        }
        return cur->value.has_value() ? &cur->value.value() : nullptr;
    }

    bool contains(std::string_view key) const { return find(key) != nullptr; }

    /// Invoke \p fn(key, value) for every stored key that is a prefix of
    /// \p query (including \p query itself if stored, and the empty key if
    /// stored). Matches are reported shortest-prefix-first, in a single
    /// descent. \p key is a view into \p query and is valid only for the
    /// duration of the call to \p fn.
    template<typename F>
    void for_each_prefix_of(std::string_view query, F&& fn) const {
        const node* cur = _root.get();
        size_t consumed = 0;
        if (_root->value.has_value()) {
            fn(std::string_view{}, _root->value.value());
        }
        std::string_view rest = query;
        while (!rest.empty()) {
            auto it = cur->children.find(rest.front());
            if (it == cur->children.end()) {
                return;
            }
            const std::string_view label = it->second->label;
            if (!rest.starts_with(label)) {
                // The edge label is not a prefix of the search string, so the
                // traversal ends here.
                return;
            }
            cur = it->second.get();
            consumed += label.size();
            rest = rest.substr(label.size());
            if (cur->value.has_value()) {
                fn(query.substr(0, consumed), cur->value.value());
            }
        }
    }

    /// Remove \p key. Returns true if a value was present. Frees the node and
    /// folds any single-child chain the removal exposes.
    bool erase(std::string_view key) {
        // Track the owning slots of cur and its parent so a removal can fold
        // the exposed chain upward without re-deriving them.
        std::unique_ptr<node>* parent_slot = nullptr;
        std::unique_ptr<node>* cur_slot = &_root;
        node* cur = _root.get();
        std::string_view rest = key;
        while (!rest.empty()) {
            auto it = cur->children.find(rest.front());
            if (it == cur->children.end()) {
                return false;
            }
            const std::string_view label = it->second->label;
            if (!rest.starts_with(label)) {
                return false;
            }
            parent_slot = cur_slot;
            cur_slot = &it->second;
            cur = it->second.get();
            rest = rest.substr(label.size());
        }
        if (!cur->value.has_value()) {
            return false;
        }
        cur->value.reset();
        --_size;
        if (parent_slot != nullptr) {
            // A non-root removal (which always has a parent slot) needs repair.
            repair_after_erase(*parent_slot, *cur_slot);
        }
        return true;
    }

    void clear() {
        destroy(std::move(_root));
        _root = std::make_unique<node>();
        _size = 0;
    }

    /// Number of live nodes, including the always-present root. Reflects the
    /// tree's structural size; exposed for introspection and tests.
    size_t node_count() const { return count(_root.get()); }

private:
    template<typename V>
    void set_value(node* n, V&& value) {
        if (!n->value.has_value()) {
            ++_size;
        }
        n->value = std::forward<V>(value);
    }

    // Free the subtree rooted at \p root without recursion: move each node's
    // children onto an explicit worklist so every node is destroyed
    // iteratively, keeping stack depth O(1) regardless of tree height.
    static void destroy(std::unique_ptr<node> root) {
        if (!root) {
            return;
        }
        chunked_vector<std::unique_ptr<node>> to_delete;
        to_delete.push_back(std::move(root));
        while (!to_delete.empty()) {
            std::unique_ptr<node> n = std::move(to_delete.back());
            to_delete.pop_back();
            for (auto& [byte, child] : n->children) {
                if (child) {
                    to_delete.push_back(std::move(child));
                }
            }
            // ~n()
        }
    }

    // Iterative for the same reason as destroy(): a recursive walk would be
    // O(tree height) deep and overflow the stack for a tree with very long
    // keys.
    static size_t count(const node* root) {
        size_t c = 0;
        chunked_vector<const node*> worklist;
        worklist.push_back(root);
        while (!worklist.empty()) {
            const node* n = worklist.back();
            worklist.pop_back();
            ++c;
            for (const auto& [byte, child] : n->children) {
                worklist.push_back(child.get());
            }
        }
        return c;
    }

    static size_t common_prefix_len(std::string_view a, std::string_view b) {
        const size_t n = std::min(a.size(), b.size());
        size_t i = 0;
        while (i < n && a[i] == b[i]) {
            ++i;
        }
        return i;
    }

    /// Split the edge held in \p slot at offset \p common (strictly inside the
    /// child's label): introduce an intermediate node carrying the shared
    /// prefix, re-parent the old child beneath it, and install it in \p slot.
    /// The caller reads it back through \p slot and places the remainder of its
    /// key beneath it.
    ///
    /// E.g. splitting a "cat" edge at common = 2 (shared prefix "ca"):
    ///
    ///   before:  slot --"cat"--> child
    ///   after:   slot --"ca"--> mid --"t"--> child
    ///
    /// Node `mid` (now in \p slot) holds label "ca" and `child` keeps the "t"
    /// suffix. Inserting "car" would then add a leaf node with label "r" under
    /// `mid`.
    static void split_edge(std::unique_ptr<node>& slot, size_t common) {
        auto child = std::exchange(slot, nullptr);
        auto& child_label = child->label;
        auto shared_prefix = child_label.substr(0, common);
        auto mid = std::make_unique<node>(std::move(shared_prefix));
        child_label.erase(child_label.begin(), child_label.begin() + common);
        mid->children.emplace(child_label.front(), std::move(child));
        slot = std::move(mid);
    }

    /// Fold the node owned by \p slot into its only child, if it is a
    /// single-child non-terminal node. Otherwise, leave \p slot untouched.
    ///
    /// E.g. folding a non-terminal node (N) with label "ca" into a single "t"
    /// child:
    ///   before:  slot --"ca"--> N --"t"--> "cat"
    ///   after:   slot --"cat"--> "cat"
    static void maybe_fold(std::unique_ptr<node>& slot) {
        if (slot->value.has_value() || slot->children.size() != 1) {
            // Node must be single-child, non-terminal
            return;
        }
        std::unique_ptr<node> child = std::move(slot->children.begin()->second);
        child->label = slot->label + child->label;
        slot = std::move(child);
    }

    /// Restore the canonical shape after erase() cleared the value of the node
    /// in \p cur. It may now be redundant in one of two ways, each repaired by
    /// a single fold. \p cur and \p parent are the owning slots of the node and
    /// its parent, so a fold re-points the owner in place. cur_byte is the
    /// first byte of cur's edge label. The node in \p cur must not be the root.
    ///
    /// Case 1: cur is a dead leaf (no value, no children). Unlink it from its
    /// parent P; if that leaves P a single-child non-terminal, fold P into its
    /// surviving child. E.g. erasing "car" from a tree holding "cat" and "car":
    ///
    ///   before:  root --"ca"--> P --"t"--> "cat"
    ///                           |
    ///                           \--"r"--> C   (cur; erased)
    ///                               ^ cur_byte = 'r'
    ///
    ///   during:  root --"ca"--> P --"t"--> "cat"   (C unlinked)
    ///
    ///   after:   root --"cat"--> "cat"             (P folded into "cat")
    ///
    /// Case 2: cur kept its children but lost its value. If it now has exactly
    /// one child, fold cur into it; cur's parent (root in this example) is not
    /// involved. E.g. erasing "ca" from a tree holding "ca" and "cat":
    ///
    ///   before:  root (P) --"ca"--> C --"t"--> "cat" (value cleared, 1 child)
    ///
    ///   after:   root (P) --"cat"--> "cat"           (C folded into "cat")
    void repair_after_erase(
      std::unique_ptr<node>& parent, std::unique_ptr<node>& cur) {
        if (cur->children.empty()) {
            // Dead leaf: unlink it from the parent (which frees it), then fold
            // the parent if that left it a single-child non-terminal. cur is
            // keyed in the parent's map by the first byte of its edge label.
            const char cur_byte = cur->label.front();
            parent->children.erase(cur_byte);
            if (parent.get() != _root.get()) {
                maybe_fold(parent);
            }
        } else {
            // cur lost its value but kept its children; if only one remains,
            // fold it into that child (maybe_fold() is a no-op while cur
            // branches).
            maybe_fold(cur);
        }
    }

private:
    std::unique_ptr<node> _root{nullptr};
    size_t _size{0};
};
