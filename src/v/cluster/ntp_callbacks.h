#pragma once
#include "cluster/partition.h"
#include "cluster/types.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/**
 * A compact trie-like index for ntp callback management.
 *
 * This structure stores and invokes callbacks associated with any ntp path
 * prefix. For exmaple, callbacks can be registered for any ntp, as well as for
 * all ntps within a specific topic.
 *
 * Registration and callback dispatch are fast. Deregistration is assumed to
 * occur during shutdown, and is currently inefficient. Add a reverse index
 * if deregistration needs to be made efficient at the expensive of the
 * extra memory required for the index.
 */
template<typename Callback>
class ntp_callbacks {
public:
    /// Register for all ntp notifications.
    notification_id_type register_notify(Callback cb) {
        return register_callback(_root.callbacks, std::move(cb));
    }

    /// Register for ntp notification for a specific namespace.
    notification_id_type register_notify(const model::ns& ns, Callback cb) {
        auto& callbacks = _root.next[ns].callbacks;
        return register_callback(callbacks, std::move(cb));
    }

    /// Register for ntp notification for a specific topic.
    notification_id_type register_notify(
      const model::ns& ns, const model::topic& topic, Callback cb) {
        auto& callbacks = _root.next[ns].next[topic].callbacks;
        return register_callback(callbacks, std::move(cb));
    }

    /// Register for notifications about a specific ntp.
    notification_id_type register_notify(const model::ntp& ntp, Callback cb) {
        const auto& topic = ntp.tp.topic;
        const auto& part = ntp.tp.partition;
        auto& callbacks = _root.next[ntp.ns].next[topic].next[part];
        return register_callback(callbacks, std::move(cb));
    }

    /// Invoke all matching callbacks.
    template<typename... Args>
    void notify(const model::ntp& ntp, Args&&... args) const {
        // invoke for wildcard watchers
        notify(_root.callbacks, std::forward<Args>(args)...);

        // filter callbacks on ntp path components
        const auto& ns = ntp.ns;
        const auto& topic = ntp.tp.topic;
        const auto& part = ntp.tp.partition;

        // invoke for namespace watchers
        const auto& n_nodes = _root.next;
        if (auto n = n_nodes.find(ns); n != n_nodes.end()) {
            notify(n->second.callbacks, std::forward<Args>(args)...);

            // invoke for topic watchers
            const auto& t_nodes = n->second.next;
            if (auto t = t_nodes.find(topic); t != t_nodes.end()) {
                notify(t->second.callbacks, std::forward<Args>(args)...);

                // invoke for partition watchers
                const auto& p_nodes = t->second.next;
                if (auto p = p_nodes.find(part); p != p_nodes.end()) {
                    notify(p->second, std::forward<Args>(args)...);
                }
            }
        }
    }

    /// Remove the callback for the given id.
    void unregister_notify(notification_id_type id) {
        if (_root.callbacks.erase(id)) {
            return;
        }
        for (auto& n : _root.next) {
            if (n.second.callbacks.erase(id)) {
                return;
            }
            for (auto& t : n.second.next) {
                if (t.second.callbacks.erase(id)) {
                    return;
                }
                for (auto& p : t.second.next) {
                    if (p.second.erase(id)) {
                        return;
                    }
                }
            }
        }
    }

private:
    using callbacks_t = absl::flat_hash_map<notification_id_type, Callback>;

    // current level callbacks and next-level index
    template<typename Key, typename Value>
    struct node {
        callbacks_t callbacks;
        absl::flat_hash_map<Key, Value> next;
    };

    notification_id_type
    register_callback(callbacks_t& callbacks, Callback&& cb) {
        auto id = _notification_id++;
        callbacks.emplace(id, std::move(cb));
        return id;
    }

    template<typename... Args>
    void notify(const callbacks_t& callbacks, Args&&... args) const {
        for (auto& cb : callbacks) {
            cb.second(std::forward<Args>(args)...);
        }
    }

    // clang-format off
    node<model::ns,
        node<model::topic,
            node<model::partition_id, callbacks_t>>> _root;
    // clang-format on

    notification_id_type _notification_id{0};
};

} // namespace cluster
