/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "utils/to_string.h"

#include <boost/range/join.hpp>

#include <algorithm>
#include <numeric>
#include <optional>
#include <type_traits>

namespace raft {

static constexpr model::revision_id no_revision{};
class vnode : public serde::envelope<vnode, serde::version<0>> {
public:
    constexpr vnode() = default;

    constexpr vnode(model::node_id nid, model::revision_id rev)
      : _node_id(nid)
      , _revision(rev) {}

    bool operator==(const vnode& other) const = default;
    bool operator!=(const vnode& other) const = default;

    friend std::ostream& operator<<(std::ostream& o, const vnode& r);

    template<typename H>
    friend H AbslHashValue(H h, const vnode& node) {
        return H::combine(std::move(h), node._node_id, node._revision);
    }

    constexpr model::node_id id() const { return _node_id; }
    constexpr model::revision_id revision() const { return _revision; }

    auto serde_fields() { return std::tie(_node_id, _revision); }

private:
    model::node_id _node_id;
    model::revision_id _revision;
};

enum class configuration_type : uint8_t { simple, joint };

std::ostream& operator<<(std::ostream& o, configuration_type t);

struct group_nodes {
    std::vector<vnode> voters;
    std::vector<vnode> learners;

    bool contains(vnode id) const;

    std::optional<vnode> find(model::node_id) const;

    friend std::ostream& operator<<(std::ostream&, const group_nodes&);
    friend bool operator==(const group_nodes&, const group_nodes&);
};

class group_configuration final {
public:
    static constexpr int8_t current_version = 3;
    /**
     * creates a configuration where all provided brokers are current
     * configuration voters
     */
    explicit group_configuration(
      std::vector<model::broker>, model::revision_id);

    /**
     * creates joint configuration
     */
    group_configuration(
      std::vector<model::broker>,
      group_nodes,
      model::revision_id,
      std::optional<group_nodes> = std::nullopt);

    group_configuration(const group_configuration&) = default;
    group_configuration(group_configuration&&) = default;
    group_configuration& operator=(const group_configuration&) = default;
    group_configuration& operator=(group_configuration&&) = default;
    ~group_configuration() = default;

    bool has_voters();

    std::optional<model::broker> find_broker(model::node_id id) const;
    bool contains_broker(model::node_id id) const;
    bool contains_address(const net::unresolved_address& address) const;

    bool contains(vnode) const;

    /**
     * Check if node is a voter
     */
    bool is_voter(vnode) const;

    /**
     * Check if node with given id is allowed to request for votes
     */
    bool is_allowed_to_request_votes(vnode) const;

    /**
     * Configuration manipulation API. Each operation cause the configuration to
     * become joint configuration.
     */
    void add(std::vector<model::broker>, model::revision_id);
    void remove(const std::vector<model::node_id>&);
    void replace(std::vector<model::broker>, model::revision_id);

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

    /**
     * Forcefully abort changing configuration. If current configuration in in
     * joint state it drops the new configuration part and allow raft to operate
     * with old quorum
     *
     * NOTE: may lead to data loss in some situations use only for cluster
     * recovery from critical failures
     */
    void abort_configuration_change(model::revision_id);

    /**
     * Reverts configuration change, the configuration is still in joint state
     * but the direction of change is being changed
     *
     */
    void cancel_configuration_change(model::revision_id);

    /**
     * demotes all voters if they were removed from current configuration,
     * returns false if no voters were demoted
     */
    bool maybe_demote_removed_voters();

    const group_nodes& current_config() const { return _current; }
    const std::optional<group_nodes>& old_config() const { return _old; }
    const std::vector<model::broker>& brokers() const { return _brokers; }

    configuration_type type() const;

    size_t unique_voter_count() const { return unique_voter_ids().size(); }

    template<typename Func>
    void for_each_broker(Func&& f) const;

    template<typename Func>
    void for_each_broker_id(Func&& f) const;

    template<typename Func>
    void for_each_voter(Func&& f) const;

    template<typename Func>
    void for_each_learner(Func&& f) const;

    void set_revision(model::revision_id new_revision) {
        vassert(
          new_revision >= _revision,
          "can not set revision to value {} which is smaller than current one "
          "{}",
          new_revision,
          _revision);

        _revision = new_revision;
    }
    /**
     * Return largest value for which every server in a quorum (majority) has a
     * value greater than or equal to.
     *
     *
     * This method is used to find an offset that was replicated by majority of
     * nodes.
     */
    template<
      typename ValueProvider,
      typename Ret = std::invoke_result_t<ValueProvider, vnode>>
    requires requires(ValueProvider&& f, vnode nid, Ret ret_a, Ret ret_b) {
        f(nid);
        { ret_a < ret_b } -> std::same_as<bool>;
    }
    auto quorum_match(ValueProvider&& f) const;

    /**
     * Returns true if for majority of group_nodes predicate returns true
     */
    template<typename Predicate>
    requires std::predicate<Predicate, vnode>
    bool majority(Predicate&& f) const;

    int8_t version() const { return _version; }

    void promote_to_voter(vnode id);
    model::revision_id revision_id() const { return _revision; }

    /**
     * Used to set initial revision for old configuration vnodes to maintain
     * backward compatibility.
     *
     * IMPORTANT: may be removed in future versions
     */
    void maybe_set_initial_revision(model::revision_id r);

    friend bool
    operator==(const group_configuration&, const group_configuration&);

    friend std::ostream& operator<<(std::ostream&, const group_configuration&);

private:
    std::vector<vnode> unique_voter_ids() const;
    std::vector<vnode> unique_learner_ids() const;

    uint8_t _version = current_version;
    std::vector<model::broker> _brokers;
    group_nodes _current;
    std::optional<group_nodes> _old;
    model::revision_id _revision;
};

namespace details {

template<typename ValueProvider, typename Range>
auto quorum_match(ValueProvider&& f, Range&& range) {
    using ret_t = std::invoke_result_t<ValueProvider, vnode>;
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

template<typename Func>
void group_configuration::for_each_broker_id(Func&& f) const {
    auto voters = unique_voter_ids();
    auto learners = unique_learner_ids();
    auto joined = boost::join(voters, learners);
    std::for_each(
      std::cbegin(joined), std::cend(joined), std::forward<Func>(f));
}

template<typename Func, typename Ret>
requires requires(Func&& f, vnode nid, Ret ret_a, Ret ret_b) {
    f(nid);
    { ret_a < ret_b } -> std::same_as<bool>;
}
auto group_configuration::quorum_match(Func&& f) const {
    if (!_old) {
        return details::quorum_match(std::forward<Func>(f), _current.voters);
    }
    /**
     * relay only on the old configuration if current configuration doesn't yet
     * have any voters
     */
    if (_current.voters.empty()) {
        return details::quorum_match(f, _old->voters);
    }

    /**
     * we must check if old voters are there, if not we do not include old
     * quorum into decision about majority
     */
    if (_old->voters.empty()) {
        return details::quorum_match(f, _current.voters);
    }

    return std::min(
      details::quorum_match(f, _current.voters),
      details::quorum_match(f, _old->voters));
}

template<typename Predicate>
requires std::predicate<Predicate, vnode>
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
struct adl<raft::vnode> {
    void to(iobuf&, raft::vnode);
    raft::vnode from(iobuf_parser&);
};
template<>
struct adl<raft::group_configuration> {
    void to(iobuf&, raft::group_configuration);
    raft::group_configuration from(iobuf_parser&);
};
} // namespace reflection
