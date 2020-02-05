#pragma once

#include "raft/types.h"
#include "vassert.h"

#include <boost/container/flat_map.hpp>

namespace raft {

class follower_stats {
public:
    using container_t
      = boost::container::flat_map<model::node_id, follower_index_metadata>;
    using iterator = container_t::iterator;
    using const_iterator = container_t::const_iterator;

    explicit follower_stats(std::vector<follower_index_metadata> meta) {
        for (auto& i : meta) {
            _followers.emplace(i.node_id, i);
        }
    }

    const follower_index_metadata& get(model::node_id n) const {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }
    follower_index_metadata& get(model::node_id n) {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }

    bool contains(model::node_id n) const {
        return _followers.find(n) != _followers.end();
    }

    iterator emplace(model::node_id n, follower_index_metadata m) {
        auto [it, success] = _followers.insert_or_assign(n, m);
        vassert(success, "could not insert node:{}", n);
        return it;
    }

    iterator find(model::node_id n) { return _followers.find(n); }
    const_iterator find(model::node_id n) const { return _followers.find(n); }

    iterator begin() { return _followers.begin(); }
    iterator end() { return _followers.end(); }
    const_iterator begin() const { return _followers.begin(); }
    const_iterator end() const { return _followers.end(); }

private:
    friend std::ostream& operator<<(std::ostream&, const follower_stats&);
    container_t _followers;
};

} // namespace raft
