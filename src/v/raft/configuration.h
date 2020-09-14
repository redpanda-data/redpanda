#pragma once
#include "model/metadata.h"
#include "utils/concepts-enabled.h"

#include <boost/range/join.hpp>

#include <algorithm>
#include <numeric>
#include <type_traits>

namespace raft {
struct group_configuration {
    group_configuration() noexcept = default;
    ~group_configuration() noexcept = default;
    group_configuration(const group_configuration&) = default;
    group_configuration& operator=(const group_configuration&) = delete;
    group_configuration(group_configuration&&) noexcept = default;
    group_configuration& operator=(group_configuration&&) noexcept = default;

    using brokers_t = std::vector<model::broker>;
    using iterator = brokers_t::iterator;
    using const_iterator = brokers_t::const_iterator;

    bool has_voters() const { return !nodes.empty(); }
    bool has_learners() const { return !learners.empty(); }
    size_t voters_majority() const { return (nodes.size() / 2) + 1; }
    size_t unique_voters_count() const { return nodes.size(); }
    iterator find_in_nodes(model::node_id id);
    const_iterator find_in_nodes(model::node_id id) const;
    iterator find_in_learners(model::node_id id);
    const_iterator find_in_learners(model::node_id id) const;
    bool contains_broker(model::node_id id) const;
    void update_broker(model::broker);

    template<typename Func>
    void for_each(Func&& f) const {
        auto joined_range = boost::join(nodes, learners);
        std::for_each(
          std::cbegin(joined_range),
          std::cend(joined_range),
          std::forward<Func>(f));
    }

    template<typename Func>
    void for_each_voter(Func&& f) const {
        std::for_each(
          std::cbegin(nodes), std::cend(nodes), std::forward<Func>(f));
    }

    /**
     * Return largest value for which every server in a quorum (majority) has a
     * value greater than or equal to.
     *
     *
     * This method is used to find an offset that was replicated by majority of
     * nodes.
     */
    // clang-format off
    template<
      typename ValueProvider,
      typename Ret = std::invoke_result_t<ValueProvider, const model::broker&>>
    CONCEPT(requires requires(
        ValueProvider f, const model::broker& broker, Ret ret_a, Ret ret_b) {
        {f(broker)};
        { ret_a < ret_b } -> bool;
    })
    // clang-format on
    auto quorum_match(ValueProvider&& f) const {
        using ret_t = std::invoke_result_t<ValueProvider, const model::broker&>;
        if (nodes.empty()) {
            return ret_t{};
        }

        std::vector<ret_t> values;
        values.reserve(nodes.size());
        std::transform(
          std::cbegin(nodes),
          std::cend(nodes),
          std::back_inserter(values),
          std::forward<ValueProvider>(f));

        size_t majority_match_idx = (values.size() - 1) / 2;
        std::nth_element(
          values.begin(),
          std::next(values.begin(), majority_match_idx),
          values.end());

        return values[majority_match_idx];
    }

    /**
     * Returns true if for majority of nodes predicate returns true
     */
    // clang-format off
    template<typename Predicate>
    CONCEPT(requires requires(Predicate f, const model::broker& broker) {
        { f(broker) } -> bool;
    })
    // clang-format on
    bool majority(Predicate&& f) const {
        if (nodes.empty()) {
            return true;
        }

        size_t cnt = std::count_if(
          std::cbegin(nodes), std::cend(nodes), std::forward<Predicate>(f));

        return cnt >= voters_majority();
    }

    friend bool
    operator==(const group_configuration&, const group_configuration&);
    // data
    brokers_t nodes;
    brokers_t learners;

    friend std::ostream&
    operator<<(std::ostream& o, const group_configuration& c);
};

struct offset_configuration {
    offset_configuration(model::offset o, group_configuration c)
      : offset(o)
      , cfg(std::move(c)) {}

    model::offset offset;
    group_configuration cfg;
    friend std::ostream& operator<<(std::ostream&, const offset_configuration&);
};
} // namespace raft
