// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_configuration.h"

#include "config/config_store.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"

#include <absl/container/flat_hash_set.h>
#include <bits/stdint-uintn.h>
#include <boost/range/join.hpp>

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

namespace raft {
/**
 * Strategy representing the old way of changing configuration i.e. Joint
 * consensus based
 */
class configuration_change_strategy_v3
  : public group_configuration::configuration_change_strategy {
public:
    explicit configuration_change_strategy_v3(group_configuration& cfg)
      : _cfg(cfg) {}

    void add_broker(model::broker, model::revision_id) final;
    void remove_broker(model::node_id) final;
    void
      replace_brokers(std::vector<broker_revision>, model::revision_id) final;

    void add(vnode, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v3 configuration version");
    }
    void replace(std::vector<vnode>, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v3 configuration version");
    }
    void remove(vnode, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v3 configuration version");
    }
    void discard_old_config() final;
    void abort_configuration_change(model::revision_id) final;
    void cancel_configuration_change(model::revision_id) final;
    void finish_configuration_transition() final {}

private:
    group_configuration& _cfg;
};

class configuration_change_strategy_v4
  : public group_configuration::configuration_change_strategy {
public:
    explicit configuration_change_strategy_v4(group_configuration& cfg)
      : _cfg(cfg) {}

    void add_broker(model::broker, model::revision_id) final;
    void remove_broker(model::node_id) final;
    void
      replace_brokers(std::vector<broker_revision>, model::revision_id) final;

    void add(vnode, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v4 configuration version");
    }
    void replace(std::vector<vnode>, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v4 configuration version");
    }
    void remove(vnode, model::revision_id) final {
        vassert(
          false,
          "node id based api is not supported in v4 configuration version");
    }
    void discard_old_config() final;
    void abort_configuration_change(model::revision_id) final;
    void cancel_configuration_change(model::revision_id) final;
    void finish_configuration_transition() final;

private:
    void cancel_update_in_transitional_state();
    void cancel_update_in_joint_state();
    group_configuration& _cfg;
};

class configuration_change_strategy_v5
  : public group_configuration::configuration_change_strategy {
public:
    explicit configuration_change_strategy_v5(group_configuration& cfg)
      : _cfg(cfg) {}

    void add_broker(model::broker, model::revision_id) final {
        vassert(
          false,
          "broker based api is not supported in configuration version 5");
    }
    void remove_broker(model::node_id) final {
        vassert(
          false,
          "broker based api is not supported in configuration version 5");
    }
    void
    replace_brokers(std::vector<broker_revision>, model::revision_id) final {
        vassert(
          false,
          "broker based api is not supported in configuration version 5");
    }

    void add(vnode, model::revision_id) final;
    void replace(std::vector<vnode>, model::revision_id) final;
    void remove(vnode, model::revision_id) final;

    void discard_old_config() final;
    void abort_configuration_change(model::revision_id) final;
    void cancel_configuration_change(model::revision_id) final;
    void finish_configuration_transition() final;

private:
    void cancel_update_in_transitional_state();
    void cancel_update_in_joint_state();
    group_configuration& _cfg;
};

namespace {

configuration_update calculate_configuration_update(
  const std::vector<vnode>& current_nodes,
  const std::vector<broker_revision>& requested_brokers) {
    configuration_update update;
    for (const auto& vn : current_nodes) {
        auto it = std::find_if(
          requested_brokers.begin(),
          requested_brokers.end(),
          [&vn](const broker_revision& br) {
              return br.broker.id() == vn.id() && br.rev == vn.revision();
          });
        if (it == requested_brokers.end()) {
            update.replicas_to_remove.push_back(vn);
        }
    }

    for (const auto& br : requested_brokers) {
        auto it = std::find_if(
          current_nodes.begin(), current_nodes.end(), [&br](const vnode& vn) {
              return br.broker.id() == vn.id() && br.rev == vn.revision();
          });
        if (it == current_nodes.end()) {
            update.replicas_to_add.emplace_back(br.broker.id(), br.rev);
        }
    }

    return update;
}

configuration_update calculate_configuration_update(
  const std::vector<vnode>& current_nodes,
  const std::vector<vnode>& requested_nodes) {
    configuration_update update;
    for (const auto& vn : current_nodes) {
        auto it = std::find(requested_nodes.begin(), requested_nodes.end(), vn);
        if (it == requested_nodes.end()) {
            update.replicas_to_remove.push_back(vn);
        }
    }

    for (const auto& vn : requested_nodes) {
        auto it = std::find(current_nodes.begin(), current_nodes.end(), vn);
        if (it == current_nodes.end()) {
            update.replicas_to_add.push_back(vn);
        }
    }

    return update;
}

std::optional<vnode>
find_by_id(const std::vector<vnode>& nodes, model::node_id id) {
    auto it = std::find_if(nodes.begin(), nodes.end(), [id](const vnode& vn) {
        return vn.id() == id;
    });
    if (it != nodes.end()) {
        return *it;
    }
    return std::nullopt;
}

} // namespace

bool configuration_update::is_to_add(const vnode& id) const {
    return std::find(replicas_to_add.begin(), replicas_to_add.end(), id)
           != replicas_to_add.end();
}

bool configuration_update::is_to_remove(const vnode& id) const {
    return std::find(replicas_to_remove.begin(), replicas_to_remove.end(), id)
           != replicas_to_remove.end();
}

bool group_nodes::contains(const vnode& id) const {
    auto v_it = std::find(voters.cbegin(), voters.cend(), id);
    if (v_it != voters.cend()) {
        return true;
    }
    auto l_it = std::find(learners.cbegin(), learners.cend(), id);
    return l_it != learners.cend();
}

std::optional<vnode> group_nodes::find(model::node_id id) const {
    auto v_it = std::find_if(
      voters.cbegin(), voters.cend(), [id](const vnode& rni) {
          return rni.id() == id;
      });

    if (v_it != voters.cend()) {
        return *v_it;
    }
    auto l_it = std::find_if(
      learners.cbegin(), learners.cend(), [id](const vnode& rni) {
          return rni.id() == id;
      });

    return l_it != learners.cend() ? std::make_optional(*l_it) : std::nullopt;
}

group_configuration::group_configuration(
  std::vector<model::broker> brokers, model::revision_id revision)
  : _version(v_4)
  , _brokers(std::move(brokers))
  , _revision(revision) {
    _current.voters.reserve(_brokers.size());
    std::transform(
      _brokers.cbegin(),
      _brokers.cend(),
      std::back_inserter(_current.voters),
      [revision](const model::broker& br) { return vnode(br.id(), revision); });
}

group_configuration::group_configuration(
  std::vector<vnode> initial_nodes, model::revision_id rev)
  : _version(v_5)
  , _revision(rev) {
    _current.voters = std::move(initial_nodes);
}

group_configuration::group_configuration(
  std::vector<model::broker> brokers,
  group_nodes current,
  model::revision_id revision,
  std::optional<configuration_update> update,
  std::optional<group_nodes> old)
  : _version(v_4)
  , _brokers(std::move(brokers))
  , _current(std::move(current))
  , _configuration_update(std::move(update))
  , _old(std::move(old))
  , _revision(revision) {}

group_configuration::group_configuration(
  group_nodes current,
  model::revision_id revision,
  std::optional<configuration_update> update,
  std::optional<group_nodes> old)
  : _version(current_version)
  , _current(std::move(current))
  , _configuration_update(std::move(update))
  , _old(std::move(old))
  , _revision(revision) {}

std::unique_ptr<group_configuration::configuration_change_strategy>
group_configuration::make_change_strategy() {
    if (_version >= v_5) {
        return std::make_unique<configuration_change_strategy_v5>(*this);
    } else if (_version == v_4) {
        return std::make_unique<configuration_change_strategy_v4>(*this);
    } else {
        return std::make_unique<configuration_change_strategy_v3>(*this);
    }
}

bool group_configuration::has_voters() const {
    return !(_current.voters.empty() || (_old && _old->voters.empty()));
}

bool group_configuration::is_voter(vnode id) const {
    auto it = std::find(_current.voters.cbegin(), _current.voters.cend(), id);

    if (it != _current.voters.cend()) {
        return true;
    }
    if (!_old) {
        return false;
    }
    auto old_it = std::find(_old->voters.cbegin(), _old->voters.cend(), id);

    return old_it != _old->voters.cend();
}

bool group_configuration::is_allowed_to_request_votes(vnode id) const {
    // either current voter
    auto it = std::find(_current.voters.cbegin(), _current.voters.cend(), id);

    if (it != _current.voters.cend()) {
        return true;
    }
    if (!_old) {
        return false;
    }
    // or present in old configuration
    auto old_it = std::find(_old->voters.cbegin(), _old->voters.cend(), id);

    // present in old voters
    if (old_it != _old->voters.cend()) {
        return true;
    }
    // look in learners
    old_it = std::find(_old->learners.cbegin(), _old->learners.cend(), id);

    return old_it != _old->learners.cend();
}

bool group_configuration::contains_broker(model::node_id id) const {
    vassert(
      _version < v_5,
      "contains broker method is not supported in configuration version {}",
      _version);
    auto it = std::find_if(
      _brokers.cbegin(), _brokers.cend(), [id](const model::broker& broker) {
          return id == broker.id();
      });

    return it != _brokers.cend();
}

bool group_configuration::contains_address(
  const net::unresolved_address& address) const {
    vassert(
      _version < v_5,
      "contains address method is not supported in configuration version {}",
      _version);
    return std::any_of(
      _brokers.cbegin(),
      _brokers.cend(),
      [&address](const model::broker& broker) {
          return address == broker.rpc_address();
      });
}

configuration_state group_configuration::get_state() const {
    if (_old) {
        return configuration_state::joint;
    }
    if (_configuration_update) {
        return configuration_state::transitional;
    }
    return configuration_state::simple;
};

std::vector<vnode>
unique_ids(const std::vector<vnode>& current, const std::vector<vnode>& old) {
    absl::flat_hash_set<vnode> unique_ids;
    unique_ids.reserve(current.size());

    for (auto& id : current) {
        unique_ids.insert(id);
    }
    for (auto& id : old) {
        unique_ids.insert(id);
    }
    std::vector<vnode> ret;
    ret.reserve(unique_ids.size());
    std::copy(unique_ids.begin(), unique_ids.end(), std::back_inserter(ret));
    return ret;
}

bool group_configuration::contains(vnode id) const {
    return _current.contains(id) || (_old && _old->contains(id));
}

std::vector<vnode> group_configuration::unique_voter_ids() const {
    auto old_voters = _old ? _old->voters : std::vector<vnode>();
    return unique_ids(_current.voters, old_voters);
}
std::vector<vnode> group_configuration::unique_learner_ids() const {
    auto old_learners = _old ? _old->learners : std::vector<vnode>();
    return unique_ids(_current.learners, old_learners);
}

void erase_id(std::vector<vnode>& v, model::node_id id) {
    auto it = std::find_if(
      v.cbegin(), v.cend(), [id](const vnode& rni) { return id == rni.id(); });

    if (it != v.cend()) {
        v.erase(it);
    }
}

void group_configuration::add_broker(
  model::broker broker, model::revision_id rev) {
    vassert(
      get_state() == configuration_state::simple,
      "can not add node to configuration when update is in progress - {}",
      *this);

    make_change_strategy()->add_broker(std::move(broker), rev);
}

void group_configuration::remove_broker(model::node_id id) {
    vassert(
      get_state() == configuration_state::simple,
      "can not remove node from configuration when update is in progress - {}",
      *this);
    make_change_strategy()->remove_broker(id);
}

void group_configuration::replace_brokers(
  std::vector<broker_revision> brokers, model::revision_id rev) {
    vassert(
      get_state() == configuration_state::simple,
      "can not replace configuration when update is in progress - {}",
      *this);
    make_change_strategy()->replace_brokers(std::move(brokers), rev);
}

void group_configuration::add(vnode node, model::revision_id rev) {
    vassert(
      get_state() == configuration_state::simple,
      "can not add node to configuration when update is in progress - {}",
      *this);

    make_change_strategy()->add(node, rev);
}

void group_configuration::remove(vnode node, model::revision_id rev) {
    vassert(
      get_state() == configuration_state::simple,
      "can not remove node from configuration when update is in progress - {}",
      *this);
    make_change_strategy()->remove(node, rev);
}

void group_configuration::replace(
  std::vector<vnode> nodes, model::revision_id rev) {
    vassert(
      get_state() == configuration_state::simple,
      "can not replace configuration when update is in progress - {}",
      *this);
    make_change_strategy()->replace(std::move(nodes), rev);
}

void group_configuration::discard_old_config() {
    vassert(
      get_state() == configuration_state::joint,
      "can only discard old configuration when in joint state - {}",
      *this);
    make_change_strategy()->discard_old_config();
}

void group_configuration::abort_configuration_change(model::revision_id rev) {
    vassert(
      get_state() != configuration_state::simple,
      "can not abort configuration change if it is of simple type - {}",
      *this);
    make_change_strategy()->abort_configuration_change(rev);
}

void group_configuration::cancel_configuration_change(model::revision_id rev) {
    vassert(
      get_state() != configuration_state::simple,
      "can not cancel configuration change if it is of simple type - {}",
      *this);
    make_change_strategy()->cancel_configuration_change(rev);
}

void group_configuration::finish_configuration_transition() {
    make_change_strategy()->finish_configuration_transition();
}

void group_configuration::promote_to_voter(vnode id) {
    auto it = std::find(
      _current.learners.cbegin(), _current.learners.cend(), id);
    // do nothing
    if (it == _current.learners.cend()) {
        return;
    }
    // add to voters
    _current.learners.erase(it);
    _current.voters.push_back(id);
}

bool group_configuration::maybe_demote_removed_voters() {
    vassert(
      get_state() == configuration_state::joint,
      "can not demote removed voters as configuration is of simple type - {}",
      *this);

    // no voters are present, do nothing
    if (_old->voters.empty()) {
        return false;
    }
    // if voter was removed, make it a learner
    auto it = std::stable_partition(
      _old->voters.begin(), _old->voters.end(), [this](const vnode& v) {
          return _current.contains(v);
      });

    // nothing to remove
    if (it == _old->voters.end()) {
        return false;
    }

    std::move(it, _old->voters.end(), std::back_inserter(_old->learners));
    _old->voters.erase(it, _old->voters.end());

    return true;
}

void group_configuration::update(model::broker broker) {
    auto it = std::find_if(
      _brokers.begin(), _brokers.end(), [id = broker.id()](model::broker& b) {
          return id == b.id();
      });

    if (it == _brokers.end()) {
        throw std::invalid_argument(fmt::format(
          "broker {} does not exists in configuration {}", broker.id(), *this));
    }

    *it = std::move(broker);
}

std::optional<vnode>
group_configuration::find_by_node_id(model::node_id id) const {
    auto res = find_by_id(_current.voters, id);
    if (res) {
        return res;
    }
    res = find_by_id(_current.learners, id);
    if (res) {
        return res;
    }
    if (_old) {
        res = find_by_id(_old->voters, id);
        if (res) {
            return res;
        }
        res = find_by_id(_old->learners, id);
        if (res) {
            return res;
        }
    }

    return std::nullopt;
}

std::vector<vnode> group_configuration::all_nodes() const {
    std::vector<vnode> ret;

    auto const copy_unique = [&ret](const std::vector<vnode>& source) {
        std::copy_if(
          source.begin(),
          source.end(),
          std::back_inserter(ret),
          [&ret](const vnode& vn) {
              return std::find(ret.begin(), ret.end(), vn) == ret.end();
          });
    };

    copy_unique(_current.voters);
    copy_unique(_current.learners);
    if (_old) {
        copy_unique(_old->voters);
        copy_unique(_old->learners);
    }

    return ret;
}

/**
 * Update strategy for v3 configuration
 */

void configuration_change_strategy_v3::add_broker(
  model::broker broker, model::revision_id rev) {
    _cfg._revision = rev;

    auto it = std::find_if(
      _cfg._brokers.cbegin(),
      _cfg._brokers.cend(),
      [id = broker.id()](const model::broker& n) { return id == n.id(); });
    if (unlikely(it != _cfg._brokers.cend())) {
        throw std::invalid_argument(fmt::format(
          "broker {} already present in current configuration {}",
          broker.id(),
          _cfg));
    }

    _cfg._old = _cfg._current;

    _cfg._current.learners.emplace_back(broker.id(), rev);
    _cfg._brokers.push_back(std::move(broker));
}

void configuration_change_strategy_v3::remove_broker(model::node_id id) {
    auto broker_it = std::find_if(
      _cfg._brokers.cbegin(),
      _cfg._brokers.cend(),
      [id](const model::broker& n) { return id == n.id(); });

    if (unlikely(broker_it == _cfg._brokers.cend())) {
        throw std::invalid_argument(fmt::format(
          "broker {} not found in current configuration {}", id, _cfg));
    }

    auto new_cfg = _cfg._current;
    // we do not yet remove brokers as we have to know each of them until
    // configuration will be advanced to simple mode

    erase_id(new_cfg.learners, id);
    erase_id(new_cfg.voters, id);

    _cfg._old = std::move(_cfg._current);
    _cfg._current = std::move(new_cfg);
}

void configuration_change_strategy_v3::replace_brokers(
  std::vector<broker_revision> brokers, model::revision_id rev) {
    _cfg._revision = rev;

    /**
     * If configurations are identical do nothing. For identical configuration
     * we assume that brokers list hasn't changed (1) and current configuration
     * contains all brokers in either voters of learners (2).
     */
    // check list of brokers (1)

    // check if all brokers are assigned to current configuration (2)
    bool brokers_are_equal
      = brokers.size() == _cfg._brokers.size()
        && std::all_of(
          brokers.begin(), brokers.end(), [this](const broker_revision& b) {
              // we may do linear lookup in _brokers collection as number of
              // brokers is usually very small f.e. 3 or 5
              auto it = std::find_if(
                _cfg._brokers.begin(),
                _cfg._brokers.end(),
                [&b](const model::broker& existing) {
                    return b.broker == existing;
                });

              return _cfg._current.contains(vnode(b.broker.id(), b.rev))
                     && it != _cfg._brokers.end();
          });

    // configurations are identical, do nothing
    if (brokers_are_equal) {
        return;
    }

    _cfg._old = _cfg._current;
    _cfg._current.learners.clear();
    _cfg._current.voters.clear();

    for (auto& br : brokers) {
        // check if broker is already a voter. voter will stay a voter
        auto v_it = std::find_if(
          _cfg._old->voters.cbegin(),
          _cfg._old->voters.cend(),
          [&br](const vnode& rni) {
              return rni.id() == br.broker.id() && rni.revision() == br.rev;
          });

        if (v_it != _cfg._old->voters.cend()) {
            _cfg._current.voters.push_back(*v_it);
            continue;
        }

        // check if broker was a learner. learner will stay a learner
        auto l_it = std::find_if(
          _cfg._old->learners.cbegin(),
          _cfg._old->learners.cend(),
          [&br](const vnode& rni) {
              return rni.id() == br.broker.id() && rni.revision() == br.rev;
          });

        if (l_it != _cfg._old->learners.cend()) {
            _cfg._current.learners.push_back(*l_it);
            continue;
        }

        // new broker, use broker revision
        _cfg._current.learners.emplace_back(br.broker.id(), br.rev);
    }

    // if both current and previous configurations are exactly the same, we do
    // not need to enter joint consensus
    if (
      _cfg._current.voters == _cfg._old->voters
      && _cfg._current.learners == _cfg._old->learners) {
        _cfg._old.reset();
    }

    for (auto& b : brokers) {
        if (!_cfg.contains_broker(b.broker.id())) {
            _cfg._brokers.push_back(std::move(b.broker));
        }
    }
}

void configuration_change_strategy_v3::abort_configuration_change(
  model::revision_id rev) {
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _cfg._old->learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _cfg._old->voters) {
        physical_node_ids.insert(id.id());
    }
    std::erase_if(_cfg._brokers, [&physical_node_ids](model::broker& b) {
        return !physical_node_ids.contains(b.id());
    });
    _cfg._current = *_cfg._old;
    _cfg._old.reset();

    // make sure that all nodes are voters
    for (auto id : _cfg._current.learners) {
        _cfg.promote_to_voter(id);
    }
    _cfg._revision = rev;
}

void configuration_change_strategy_v3::cancel_configuration_change(
  model::revision_id rev) {
    auto tmp = _cfg._current;
    _cfg._current = *_cfg._old;
    _cfg._old = std::move(tmp);
    _cfg._revision = rev;
}

void configuration_change_strategy_v3::discard_old_config() {
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _cfg._current.learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _cfg._current.voters) {
        physical_node_ids.insert(id.id());
    }
    // remove unused brokers from brokers set
    auto it = std::stable_partition(
      _cfg._brokers.begin(),
      _cfg._brokers.end(),
      [physical_node_ids](const model::broker& b) {
          return physical_node_ids.contains(b.id());
      });
    // we are only interested in current brokers
    _cfg._brokers.erase(it, _cfg._brokers.end());
    _cfg._old.reset();
}

/**
 * Update strategy for v4 configuration
 */
void configuration_change_strategy_v4::add_broker(
  model::broker broker, model::revision_id rev) {
    _cfg._revision = rev;

    auto it = std::find_if(
      _cfg._brokers.cbegin(),
      _cfg._brokers.cend(),
      [id = broker.id()](const model::broker& n) { return id == n.id(); });
    if (unlikely(it != _cfg._brokers.cend())) {
        throw std::invalid_argument(fmt::format(
          "broker {} already present in current configuration {}",
          broker.id(),
          _cfg));
    }

    _cfg._configuration_update = configuration_update{};

    _cfg._current.learners.emplace_back(broker.id(), rev);
    _cfg._configuration_update->replicas_to_add.emplace_back(broker.id(), rev);
    _cfg._brokers.push_back(std::move(broker));
}

void configuration_change_strategy_v4::remove_broker(model::node_id id) {
    auto broker_it = std::find_if(
      _cfg._brokers.cbegin(),
      _cfg._brokers.cend(),
      [id](const model::broker& n) { return id == n.id(); });
    if (unlikely(broker_it == _cfg._brokers.cend())) {
        throw std::invalid_argument(fmt::format(
          "broker {} not found in current configuration {}", id, _cfg));
    }

    auto new_cfg = _cfg._current;
    _cfg._configuration_update = configuration_update{};
    // we do not yet remove brokers as we have to know each of them until
    // configuration will be advanced to simple mode
    auto lit = std::find_if(
      new_cfg.learners.begin(), new_cfg.learners.end(), [id](const vnode& vn) {
          return vn.id() == id;
      });

    if (lit != new_cfg.learners.end()) {
        _cfg._configuration_update->replicas_to_remove.push_back(*lit);
        new_cfg.learners.erase(lit);
    }

    auto vit = std::find_if(
      new_cfg.voters.begin(), new_cfg.voters.end(), [id](const vnode& vn) {
          return vn.id() == id;
      });

    if (vit != new_cfg.voters.end()) {
        _cfg._configuration_update->replicas_to_remove.push_back(*vit);
        new_cfg.voters.erase(vit);
    }

    _cfg._old = std::move(_cfg._current);
    _cfg._current = std::move(new_cfg);
}

void configuration_change_strategy_v4::replace_brokers(
  std::vector<broker_revision> brokers, model::revision_id rev) {
    _cfg._revision = rev;

    /**
     * If configurations are identical do nothing. For identical configuration
     * we assume that brokers list hasn't changed (1) and current configuration
     * contains all brokers in either voters of learners (2).
     */
    // check list of brokers (1)

    // check if all brokers are assigned to current configuration (2)
    bool brokers_are_equal
      = brokers.size() == _cfg._brokers.size()
        && std::all_of(
          brokers.begin(), brokers.end(), [this](const broker_revision& b) {
              // we may do linear lookup in _brokers collection as number of
              // brokers is usually very small f.e. 3 or 5
              auto it = std::find_if(
                _cfg._brokers.begin(),
                _cfg._brokers.end(),
                [&b](const model::broker& existing) {
                    return b.broker == existing;
                });

              return _cfg._current.contains(vnode(b.broker.id(), b.rev))
                     && it != _cfg._brokers.end();
          });

    // configurations are identical, do nothing
    if (brokers_are_equal) {
        return;
    }
    // calculate configuration update
    _cfg._configuration_update = calculate_configuration_update(
      _cfg._current.voters, brokers);

    // add replicas to current configuration
    for (auto& br : brokers) {
        vnode vn(br.broker.id(), br.rev);
        if (_cfg._configuration_update->is_to_add(vn)) {
            _cfg._brokers.push_back(std::move(br.broker));
            _cfg._current.learners.push_back(vn);
        }
    }

    // optimization: when there are only nodes to be deleted we may go straight
    // to the joint configuration
    if (_cfg._configuration_update->replicas_to_add.empty()) {
        finish_configuration_transition();
    }
}

void configuration_change_strategy_v4::finish_configuration_transition() {
    // if there are no nodes to remove there is no need to enter joint consensus
    if (_cfg._configuration_update->replicas_to_remove.empty()) {
        _cfg._configuration_update.reset();
        return;
    }

    // enter joint consensus
    _cfg._old = _cfg._current;

    // remove nodes from current voters
    std::erase_if(_cfg._current.voters, [this](const vnode& voter) {
        return _cfg._configuration_update->is_to_remove(voter);
    });
}

void configuration_change_strategy_v4::abort_configuration_change(
  model::revision_id rev) {
    // collect all node ids
    absl::flat_hash_set<vnode> all_node_ids;

    _cfg.for_each_learner(
      [&all_node_ids](const vnode& learner) { all_node_ids.insert(learner); });

    _cfg.for_each_voter(
      [&all_node_ids](const vnode& voter) { all_node_ids.insert(voter); });

    // clear the configuration
    _cfg._current.voters.clear();
    _cfg._current.learners.clear();
    _cfg._old.reset();

    // rebuild configuration

    for (auto& vn : all_node_ids) {
        if (!_cfg._configuration_update->is_to_add(vn)) {
            _cfg._current.voters.push_back(vn);
        }
    }
    std::erase_if(_cfg._brokers, [this](const model::broker& br) {
        return !std::any_of(
          _cfg._current.voters.begin(),
          _cfg._current.voters.end(),
          [id = br.id()](vnode vn) { return vn.id() == id; });
    });
    // finally reset configuration update and set the revision
    _cfg._configuration_update.reset();
    _cfg._revision = rev;
}

void configuration_change_strategy_v4::cancel_configuration_change(
  model::revision_id rev) {
    switch (_cfg.get_state()) {
    case configuration_state::simple:
        vassert(
          false,
          "can not cancel, configuration change is not in progress - {}",
          _cfg);
    case configuration_state::transitional:
        cancel_update_in_transitional_state();
        break;
    case configuration_state::joint:
        cancel_update_in_joint_state();
        break;
    }
    _cfg._revision = rev;
}

void configuration_change_strategy_v4::cancel_update_in_transitional_state() {
    /**
     * in transitional state the only change applied to configuration are nodes
     * which are added to the replica set. We may simply remove learners but for
     * the learners that were already promoted to voter we must use joint
     * consensus.
     */

    _cfg._configuration_update->replicas_to_remove.clear();

    for (auto& to_add : _cfg._configuration_update->replicas_to_add) {
        if (_cfg.is_voter(to_add)) {
            _cfg._configuration_update->replicas_to_remove.push_back(to_add);
        }
    }

    std::erase_if(_cfg._brokers, [this](const model::broker& br) {
        return std::any_of(
          _cfg._current.learners.begin(),
          _cfg._current.learners.end(),
          [id = br.id()](const vnode& vn) { return vn.id() == id; });
    });

    // all the other nodes that are to add are learners, remove them all
    _cfg._current.learners.clear();
    _cfg._configuration_update->replicas_to_add.clear();

    // optimization: when there are only nodes to be deleted we may go straight
    // to the joint configuration
    finish_configuration_transition();
}

void configuration_change_strategy_v4::cancel_update_in_joint_state() {
    /**
     * After group configuration reached the joint state we must move back to
     * transitional state and start adding nodes that are removed.
     */
    _cfg._current = *_cfg._old;
    _cfg._old.reset();

    auto tmp_u = _cfg._configuration_update->replicas_to_add;

    _cfg._configuration_update->replicas_to_add
      = _cfg._configuration_update->replicas_to_remove;
    _cfg._configuration_update->replicas_to_remove = std::move(tmp_u);
}

void configuration_change_strategy_v4::discard_old_config() {
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _cfg._current.learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _cfg._current.voters) {
        physical_node_ids.insert(id.id());
    }
    // remove unused brokers from brokers set
    auto it = std::stable_partition(
      _cfg._brokers.begin(),
      _cfg._brokers.end(),
      [physical_node_ids](const model::broker& b) {
          return physical_node_ids.contains(b.id());
      });
    // we are only interested in current brokers
    _cfg._brokers.erase(it, _cfg._brokers.end());
    _cfg._old.reset();
    _cfg._configuration_update.reset();
}

/**
 * Update strategy for v5 configuration
 */

void configuration_change_strategy_v5::add(vnode node, model::revision_id rev) {
    _cfg._revision = rev;

    if (unlikely(_cfg.contains(node))) {
        throw std::invalid_argument(fmt::format(
          "node {} already present in current configuration {}", node, _cfg));
    }

    _cfg._configuration_update = configuration_update{};

    _cfg._current.learners.push_back(node);
    _cfg._configuration_update->replicas_to_add.push_back(node);
}

void configuration_change_strategy_v5::remove(
  vnode id, model::revision_id rev) {
    if (!_cfg._current.contains(id)) {
        throw std::invalid_argument(fmt::format(
          "node {} not found in current configuration {}", id, _cfg));
    }

    auto new_cfg = _cfg._current;
    _cfg._configuration_update = configuration_update{};

    // learners can be removed immediately
    auto lit = std::find(new_cfg.learners.begin(), new_cfg.learners.end(), id);
    if (lit != new_cfg.learners.end()) {
        new_cfg.learners.erase(lit);
    }

    auto vit = std::find(new_cfg.voters.begin(), new_cfg.voters.end(), id);

    if (vit != new_cfg.voters.end()) {
        _cfg._configuration_update->replicas_to_remove.push_back(*vit);
        new_cfg.voters.erase(vit);
    }
    // if there are voters to remove we need to enter joint consensus, learners
    // can be removed immediately without the need for joint consensus step
    if (!_cfg._configuration_update->replicas_to_remove.empty()) {
        _cfg._old = std::move(_cfg._current);
    }

    _cfg._current = std::move(new_cfg);
    _cfg._revision = rev;
}

void configuration_change_strategy_v5::replace(
  std::vector<vnode> nodes, model::revision_id rev) {
    _cfg._revision = rev;

    /**
     * If configurations are identical do nothing. Configurations are considered
     * equal if requested nodes are voters
     */
    bool are_equal = _cfg._current.voters.size() == nodes.size()
                     && std::all_of(
                       nodes.begin(), nodes.end(), [this](const vnode& vn) {
                           return _cfg.contains(vn);
                       });

    // configurations are identical, do nothing
    if (are_equal) {
        return;
    }
    // calculate configuration update
    _cfg._configuration_update = calculate_configuration_update(
      _cfg._current.voters, nodes);

    // add replicas to current configuration
    for (auto& vn : nodes) {
        if (_cfg._configuration_update->is_to_add(vn)) {
            _cfg._current.learners.push_back(vn);
        }
    }

    // optimization: when there are only nodes to be deleted we may go straight
    // to the joint configuration
    if (_cfg._configuration_update->replicas_to_add.empty()) {
        finish_configuration_transition();
    }
}

void configuration_change_strategy_v5::finish_configuration_transition() {
    // if there are no nodes to remove there is no need to enter joint consensus
    if (_cfg._configuration_update->replicas_to_remove.empty()) {
        _cfg._configuration_update.reset();
        return;
    }

    // enter joint consensus
    _cfg._old = _cfg._current;

    // remove nodes from current voters
    std::erase_if(_cfg._current.voters, [this](const vnode& voter) {
        return _cfg._configuration_update->is_to_remove(voter);
    });
}

void configuration_change_strategy_v5::abort_configuration_change(
  model::revision_id rev) {
    configuration_change_strategy_v4{_cfg}.abort_configuration_change(rev);
}

void configuration_change_strategy_v5::cancel_configuration_change(
  model::revision_id rev) {
    configuration_change_strategy_v4{_cfg}.cancel_configuration_change(rev);
}

void configuration_change_strategy_v5::discard_old_config() {
    _cfg._old.reset();
    _cfg._configuration_update.reset();
}

std::vector<vnode> with_revisions_assigned(
  const std::vector<vnode>& vnodes, model::revision_id new_revision) {
    std::vector<vnode> with_rev;
    with_rev.reserve(vnodes.size());

    std::transform(
      vnodes.cbegin(),
      vnodes.cend(),
      std::back_inserter(with_rev),
      [new_revision](const vnode& n) {
          vassert(
            n.revision() == no_revision,
            "changing revision of nodes with current revision set should never "
            "happen, current revision: {}",
            n.revision());
          return vnode(n.id(), new_revision);
      });

    return with_rev;
}

bool have_no_revision(const std::vector<vnode>& vnodes) {
    return !vnodes.empty() && vnodes.begin()->revision() == no_revision;
}

void group_configuration::maybe_set_initial_revision(
  model::revision_id new_rev) {
    group_nodes new_current;
    // if configuration have no revision assigned, fix it
    if (
      have_no_revision(_current.learners)
      || have_no_revision(_current.voters)) {
        // current configuration
        _current.voters = with_revisions_assigned(_current.voters, new_rev);
        _current.learners = with_revisions_assigned(_current.learners, new_rev);

        // old configuration
        if (_old) {
            _old->voters = with_revisions_assigned(_old->voters, new_rev);
            _old->learners = with_revisions_assigned(_old->learners, new_rev);
        }
    }
}

std::ostream& operator<<(std::ostream& o, const group_configuration& c) {
    fmt::print(
      o,
      "{{current: {}, old:{}, revision: {}, update: {}, version: {}}}",
      c._current,
      c._old,
      c._revision,
      c._configuration_update,
      c._version);
    if (c._version < group_configuration::v_5) {
        fmt::print(o, ", brokers: {}}}", c._brokers);
    } else {
        fmt::print(o, "}}");
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const group_nodes& n) {
    fmt::print(o, "{{voters: {}, learners: {}}}", n.voters, n.learners);
    return o;
}

std::ostream& operator<<(std::ostream& o, const offset_configuration& c) {
    fmt::print(o, "{{offset: {}, group_configuration: {}}}", c.offset, c.cfg);
    return o;
}

std::ostream& operator<<(std::ostream& o, configuration_state t) {
    switch (t) {
    case configuration_state::simple:
        return o << "simple";
    case configuration_state::joint:
        return o << "joint";
    case configuration_state::transitional:
        return o << "transitional";
    }
    __builtin_unreachable();
}
std::ostream& operator<<(std::ostream& o, const configuration_update& u) {
    fmt::print(
      o,
      "{{to_add: {}, to_remove: {}}}",
      u.replicas_to_add,
      u.replicas_to_remove);
    return o;
}
} // namespace raft

namespace reflection {

void adl<raft::group_configuration>::to(
  iobuf& buf, raft::group_configuration cfg) {
    if (cfg.version() < raft::group_configuration::v_5) {
        serialize(
          buf,
          cfg.version(),
          cfg.brokers(),
          cfg.current_config(),
          cfg.old_config(),
          cfg.revision_id());

        // only serialize configuration update for version which is greater than
        // 4
        if (cfg.version() >= raft::group_configuration::v_4) {
            serialize(buf, cfg.get_configuration_update());
        }
    } else {
        serialize(
          buf,
          cfg.version(),
          cfg.current_config(),
          cfg.old_config(),
          cfg.revision_id(),
          cfg.get_configuration_update());
    }
}

std::vector<raft::vnode> make_vnodes(const std::vector<model::node_id>& ids) {
    std::vector<raft::vnode> ret;
    ret.reserve(ids.size());
    std::transform(
      ids.begin(), ids.end(), std::back_inserter(ret), [](model::node_id id) {
          return raft::vnode(id, raft::no_revision);
      });
    return ret;
}

struct group_nodes_v0 {
    std::vector<model::node_id> voters;
    std::vector<model::node_id> learners;

    raft::group_nodes to_v2() {
        raft::group_nodes ret;
        ret.voters = make_vnodes(voters);
        ret.learners = make_vnodes(learners);
        return ret;
    }
};

raft::group_configuration
adl<raft::group_configuration>::from(iobuf_parser& p) {
    auto version = adl<raft::group_configuration::version_t>{}.from(p);
    // currently we support only versions up to 1
    vassert(
      version <= raft::group_configuration::current_version,
      "Version {} is not supported. We only support versions up to {}",
      version,
      raft::group_configuration::current_version);

    /**
     * we use versions field to maintain backward compatibility
     *
     * version 0 - base
     * version 1 - introduced revision id
     * version 2 - introduced raft::vnode
     * version 3 - model::broker with multiple endpoints
     * version 4 - persist configuration update request
     * version 5 - no brokers
     */

    std::vector<model::broker> brokers;
    if (unlikely(version < raft::group_configuration::v_5)) {
        if (likely(version >= raft::group_configuration::v_3)) {
            brokers = adl<std::vector<model::broker>>{}.from(p);
        } else {
            auto brokers_v0
              = adl<std::vector<model::internal::broker_v0>>{}.from(p);
            std::transform(
              brokers_v0.begin(),
              brokers_v0.end(),
              std::back_inserter(brokers),
              [](const model::internal::broker_v0& broker) {
                  return broker.to_v3();
              });
        }
    }

    raft::group_nodes current;
    std::optional<raft::group_nodes> old;

    if (likely(version >= raft::group_configuration::version_t(2))) {
        current = adl<raft::group_nodes>{}.from(p);
        old = adl<std::optional<raft::group_nodes>>{}.from(p);
    } else {
        // no raft::vnodes
        auto current_v0 = adl<group_nodes_v0>{}.from(p);
        auto old_v0 = adl<std::optional<group_nodes_v0>>{}.from(p);

        current = current_v0.to_v2();
        if (old_v0) {
            old = old_v0->to_v2();
        }
    }
    model::revision_id revision = raft::no_revision;
    if (version > raft::group_configuration::version_t(0)) {
        revision = adl<model::revision_id>{}.from(p);
    }
    std::optional<raft::configuration_update> update;
    if (likely(version >= raft::group_configuration::version_t(4))) {
        update = adl<std::optional<raft::configuration_update>>{}.from(p);
    }
    if (likely(version >= raft::group_configuration::v_5)) {
        return {
          std::move(current), revision, std::move(update), std::move(old)};
    } else {
        raft::group_configuration cfg{
          std::move(brokers),
          std::move(current),
          revision,
          std::move(update),
          std::move(old)};

        cfg.set_version(version);

        return cfg;
    }
}

void adl<raft::vnode>::to(iobuf& buf, raft::vnode id) {
    serialize(buf, id.id(), id.revision());
}

raft::vnode adl<raft::vnode>::from(iobuf_parser& p) {
    auto id = adl<model::node_id>{}.from(p);
    auto rev = adl<model::revision_id>{}.from(p);
    return {id, rev};
}

void adl<raft::configuration_update>::to(
  iobuf& out, raft::configuration_update update) {
    reflection::serialize(
      out, update.replicas_to_add, update.replicas_to_remove);
}
raft::configuration_update
adl<raft::configuration_update>::from(iobuf_parser& in) {
    auto to_add = adl<std::vector<raft::vnode>>{}.from(in);
    auto to_remove = adl<std::vector<raft::vnode>>{}.from(in);

    return raft::configuration_update{
      .replicas_to_add = std::move(to_add),
      .replicas_to_remove = std::move(to_remove),
    };
}

} // namespace reflection
