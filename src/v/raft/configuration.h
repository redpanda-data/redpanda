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
#include "model/metadata.h"
#include "reflection/adl.h"
#include "utils/concepts-enabled.h"

#include <boost/range/join.hpp>

#include <algorithm>
#include <numeric>
#include <type_traits>

namespace raft {

enum class configuration_type : uint8_t { simple, joint };

struct group_nodes {
    std::vector<model::node_id> voters;
    std::vector<model::node_id> learners;

    bool contains(model::node_id id) const;
    friend std::ostream& operator<<(std::ostream&, const group_nodes&);
    friend bool operator==(const group_nodes&, const group_nodes&);
};

class group_configuration final {
public:
    static constexpr int8_t current_version = 0;
    /**
     * creates a configuration where all provided brokers are current
     * configuration voters
     */
    explicit group_configuration(std::vector<model::broker>);

    /**
     * creates joint configuration
     */
    group_configuration(
      std::vector<model::broker>,
      group_nodes,
      std::optional<group_nodes> = std::nullopt);

    group_configuration(const group_configuration&) = default;
    group_configuration(group_configuration&&) = default;
    group_configuration& operator=(const group_configuration&) = default;
    group_configuration& operator=(group_configuration&&) = default;

    bool has_voters();

    std::optional<model::broker> find(model::node_id id) const;
    bool contains_broker(model::node_id id) const;

    /**
     * Check if node with given id is allowed to request for votes
     */
    bool is_voter(model::node_id) const;

    /**
     * Configuration manipulation API. Each operation cause the configuration to
     * become joint configuration.
     */
    void add(std::vector<model::broker>);
    void remove(const std::vector<model::node_id>&);
    void replace(std::vector<model::broker>);

    /**
     * Updating broker configuration. This operation does not require entering
     * joint consensus as it never change majority
     */
    void update(model::broker);

    /**
     * Discards the old configuration, after this operation joint configuration
     * become simple
     */
    void discard_old_config();

    const group_nodes& current_config() const { return _current; }
    const std::optional<group_nodes>& old_config() const { return _old; }
    const std::vector<model::broker>& brokers() const { return _brokers; }

    configuration_type type() const;

    size_t unique_voter_count() const { return unique_voter_ids().size(); }

    template<typename Func>
    void for_each_broker(Func&& f) const;

    template<typename Func>
    void for_each_voter(Func&& f) const;

    template<typename Func>
    void for_each_learner(Func&& f) const;

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
      typename Ret = std::invoke_result_t<ValueProvider, model::node_id>>
    CONCEPT(requires requires(
        ValueProvider f,  model::node_id nid, Ret ret_a, Ret ret_b) {
        {f(nid)};
        { ret_a < ret_b } -> bool;
    })
    // clang-format on

    auto quorum_match(ValueProvider&& f) const;

    /**
     * Returns true if for majority of group_nodes predicate returns true
     */
    // clang-format off
    template<typename Predicate>
    CONCEPT(requires requires(Predicate f, model::node_id id) {
        { f(id) } -> bool;
    })
    // clang-format on
    bool majority(Predicate&& f) const;

    int8_t version() const { return _version; }

    void promote_to_voter(model::node_id id);

    friend bool
    operator==(const group_configuration&, const group_configuration&);

    friend std::ostream& operator<<(std::ostream&, const group_configuration&);

private:
    std::vector<model::node_id> unique_voter_ids() const;
    std::vector<model::node_id> unique_learner_ids() const;

    uint8_t _version = current_version;
    std::vector<model::broker> _brokers;
    group_nodes _current;
    std::optional<group_nodes> _old;
};

namespace details {

template<typename ValueProvider, typename Range>
auto quorum_match(ValueProvider&& f, Range&& range) {
    using ret_t = std::invoke_result_t<ValueProvider, model::node_id>;
    if (range.empty()) {
        return ret_t{};
    }

    std::vector<ret_t> values;
    values.reserve(range.size());
    std::transform(
      std::cbegin(range),
      std::cend(range),
      std::back_inserter(values),
      std::forward<ValueProvider>(f));

    size_t majority_match_idx = (values.size() - 1) / 2;
    std::nth_element(
      values.begin(),
      std::next(values.begin(), majority_match_idx),
      values.end());

    return values[majority_match_idx];
}

template<typename Predicate, typename Range>
bool majority(Predicate&& f, Range&& range) {
    if (range.empty()) {
        return true;
    }

    size_t cnt = std::count_if(
      std::cbegin(range), std::cend(range), std::forward<Predicate>(f));

    return cnt >= (range.size() / 2) + 1;
}
} // namespace details

template<typename Func>
void group_configuration::for_each_broker(Func&& f) const {
    std::for_each(
      std::cbegin(_brokers), std::cend(_brokers), std::forward<Func>(f));
}

template<typename Func, typename Ret>
auto group_configuration::quorum_match(Func&& f) const {
    if (!_old) {
        return details::quorum_match(std::forward<Func>(f), _current.voters);
    }
    return std::min(
      details::quorum_match(f, _current.voters),
      details::quorum_match(f, _old->voters));
}

template<typename Predicate>
bool group_configuration::majority(Predicate&& f) const {
    if (!_old) {
        return details::majority(std::forward<Predicate>(f), _current.voters);
    }
    return details::majority(f, _current.voters)
           && details::majority(f, _old->voters);
}

template<typename Func>
void group_configuration::for_each_voter(Func&& f) const {
    auto ids = unique_voter_ids();
    std::for_each(ids.begin(), ids.end(), std::forward<Func>(f));
}

template<typename Func>
void group_configuration::for_each_learner(Func&& f) const {
    auto ids = unique_learner_ids();
    std::for_each(ids.begin(), ids.end(), std::forward<Func>(f));
}

struct offset_configuration {
    offset_configuration(model::offset o, group_configuration c)
      : offset(o)
      , cfg(std::move(c)) {}

    model::offset offset;
    group_configuration cfg;
    friend std::ostream& operator<<(std::ostream&, const offset_configuration&);
};
} // namespace raft

namespace reflection {
template<>
struct adl<raft::group_configuration> {
    void to(iobuf&, raft::group_configuration);
    raft::group_configuration from(iobuf_parser&);
};
} // namespace reflection
