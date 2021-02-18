// Copyright 2020 Vectorized, Inc.
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
bool group_nodes::contains(vnode id) const {
    auto v_it = std::find(std::cbegin(voters), std::cend(voters), id);
    if (v_it != voters.cend()) {
        return true;
    }
    auto l_it = std::find(std::cbegin(learners), std::cend(learners), id);
    return l_it != learners.cend();
}

std::optional<vnode> group_nodes::find(model::node_id id) const {
    auto v_it = std::find_if(
      std::cbegin(voters), std::cend(voters), [id](const vnode& rni) {
          return rni.id() == id;
      });

    if (v_it != voters.cend()) {
        return *v_it;
    }
    auto l_it = std::find_if(
      std::cbegin(learners), std::cend(learners), [id](const vnode& rni) {
          return rni.id() == id;
      });

    return l_it != learners.cend() ? std::make_optional(*l_it) : std::nullopt;
}

group_configuration::group_configuration(
  std::vector<model::broker> brokers, model::revision_id revision)
  : _brokers(std::move(brokers))
  , _revision(revision) {
    _current.voters.resize(brokers.size());
    std::transform(
      std::cbegin(_brokers),
      std::cend(_brokers),
      std::back_inserter(_current.voters),
      [revision](const model::broker& br) { return vnode(br.id(), revision); });
}

/**
 * Creates joint configuration
 */
group_configuration::group_configuration(
  std::vector<model::broker> brokers,
  group_nodes current,
  model::revision_id revision,
  std::optional<group_nodes> old)
  : _brokers(std::move(brokers))
  , _current(std::move(current))
  , _old(std::move(old))
  , _revision(revision) {}

std::optional<model::broker>
group_configuration::find_broker(model::node_id id) const {
    auto it = std::find_if(
      std::cbegin(_brokers),
      std::cend(_brokers),
      [id](const model::broker& broker) { return id == broker.id(); });

    if (it != std::cend(_brokers)) {
        return *it;
    }
    return std::nullopt;
}

bool group_configuration::has_voters() {
    return !(_current.voters.empty() || (_old && _old->voters.empty()));
}

bool group_configuration::is_voter(vnode id) const {
    auto it = std::find(
      std::cbegin(_current.voters), std::cend(_current.voters), id);

    if (it != std::cend(_current.voters)) {
        return true;
    }
    if (!_old) {
        return false;
    }
    auto old_it = std::find(
      std::cbegin(_old->voters), std::cend(_old->voters), id);

    return old_it != std::cend(_old->voters);
}

bool group_configuration::contains_broker(model::node_id id) const {
    auto it = std::find_if(
      std::cbegin(_brokers),
      std::cend(_brokers),
      [id](const model::broker& broker) { return id == broker.id(); });

    return it != std::cend(_brokers);
}

configuration_type group_configuration::type() const {
    if (_old) {
        return configuration_type::joint;
    }
    return configuration_type::simple;
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
      std::cbegin(v), std::cend(v), [id](const vnode& rni) {
          return id == rni.id();
      });
    if (it != std::cend(v)) {
        v.erase(it);
    }
}

void group_configuration::add(
  std::vector<model::broker> brokers, model::revision_id rev) {
    vassert(!_old, "can not add broker to joint configuration - {}", *this);
    _revision = rev;
    for (auto& b : brokers) {
        auto it = std::find_if(
          std::cbegin(_brokers),
          std::cend(_brokers),
          [id = b.id()](const model::broker& n) { return id == n.id(); });
        if (unlikely(it != std::cend(_brokers))) {
            throw std::invalid_argument(fmt::format(
              "broker {} already present in current configuration {}",
              b.id(),
              *this));
        }
    }

    _old = _current;
    for (auto& b : brokers) {
        _current.learners.emplace_back(b.id(), rev);
        _brokers.push_back(std::move(b));
    }
}

void group_configuration::remove(const std::vector<model::node_id>& ids) {
    vassert(
      !_old, "can not remove broker from joint configuration - {}", *this);
    for (auto& id : ids) {
        auto broker_it = std::find_if(
          std::cbegin(_brokers),
          std::cend(_brokers),
          [id](const model::broker& n) { return id == n.id(); });
        if (unlikely(broker_it == _brokers.cend())) {
            throw std::invalid_argument(fmt::format(
              "broker {} not found in current configuration {}", id, *this));
        }
    }

    auto new_cfg = _current;
    // we do not yet remove brokers as we have to know each of them until
    // configuration will be advanced to simple mode
    for (auto& id : ids) {
        erase_id(new_cfg.learners, id);
        erase_id(new_cfg.voters, id);
    }

    _old = std::move(_current);
    _current = std::move(new_cfg);
}

void group_configuration::replace(
  std::vector<model::broker> brokers, model::revision_id rev) {
    vassert(!_old, "can not replace joint configuration - {}", *this);
    _revision = rev;

    /**
     * If configurations are identical do nothing. For identical configuration
     * we assume that brokers list hasn't changed (1) and current configuration
     * contains all brokers in either voters of learners (2).
     */
    // check list of brokers (1)
    if (brokers == _brokers) {
        // check if all brokers are assigned to current configuration (2)
        bool has_all = std::all_of(
          brokers.begin(), brokers.end(), [this, rev](model::broker& b) {
              return _current.contains(vnode(b.id(), rev));
          });
        // configurations are identical, do nothing
        if (has_all) {
            return;
        }
    }

    _old = _current;
    _current.learners.clear();
    _current.voters.clear();

    for (auto& br : brokers) {
        // brokers was a voter
        auto v_it = std::find_if(
          std::cbegin(_old->voters),
          std::cend(_old->voters),
          [&br](const vnode& rni) { return rni.id() == br.id(); });

        if (v_it != std::cend(_old->voters)) {
            _current.voters.push_back(*v_it);
            continue;
        }
        // brokers was a learner
        auto l_it = std::find_if(
          std::cbegin(_old->learners),
          std::cend(_old->learners),
          [&br](const vnode& rni) { return rni.id() == br.id(); });

        if (l_it != std::cend(_old->learners)) {
            _current.learners.push_back(*l_it);
            continue;
        }

        // new broker, use provided revision
        _current.learners.emplace_back(br.id(), rev);
    }

    for (auto& b : brokers) {
        if (!contains_broker(b.id())) {
            _brokers.push_back(std::move(b));
        }
    }
}

void group_configuration::promote_to_voter(vnode id) {
    auto it = std::find(
      std::cbegin(_current.learners), std::cend(_current.learners), id);
    // do nothing
    if (it == _current.learners.end()) {
        return;
    }
    // add to voters
    _current.learners.erase(it);
    _current.voters.push_back(id);
}

void group_configuration::discard_old_config() {
    vassert(
      _old,
      "can not discard old configuration as configuration is of simple type - "
      "{}",
      *this);
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _current.learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _current.voters) {
        physical_node_ids.insert(id.id());
    }
    // remove unused brokers from brokers set
    auto it = std::stable_partition(
      std::begin(_brokers),
      std::end(_brokers),
      [physical_node_ids](const model::broker& b) {
          return physical_node_ids.contains(b.id());
      });
    // we are only interested in current brokers
    _brokers.erase(it, std::end(_brokers));
    _old.reset();
}

void group_configuration::update(model::broker broker) {
    auto it = std::find_if(
      std::begin(_brokers),
      std::end(_brokers),
      [id = broker.id()](model::broker& b) { return id == b.id(); });

    if (it == std::cend(_brokers)) {
        throw std::invalid_argument(fmt::format(
          "broker {} does not exists in configuration {}", broker.id(), *this));
    }

    *it = std::move(broker);
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
      "{{current: {}, old:{}, revision: {}, brokers: {}}}",
      c._current,
      c._old,
      c._revision,
      c._brokers);
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

bool operator==(const group_nodes& a, const group_nodes& b) {
    return a.learners == b.learners && a.voters == b.voters;
}

bool operator==(const group_configuration& a, const group_configuration& b) {
    return a._brokers == b._brokers && a._current == b._current
           && a._old == b._old;
}
} // namespace raft

namespace reflection {

void adl<raft::group_configuration>::to(
  iobuf& buf, raft::group_configuration cfg) {
    serialize(
      buf,
      cfg.version(),
      cfg.brokers(),
      cfg.current_config(),
      cfg.old_config(),
      cfg.revision_id());
}

std::vector<raft::vnode> make_vnodes(const std::vector<model::node_id> ids) {
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
    auto version = adl<uint8_t>{}.from(p);
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
     */

    std::vector<model::broker> brokers;

    if (likely(version >= 3)) {
        brokers = adl<std::vector<model::broker>>{}.from(p);
    } else {
        auto brokers_v0 = adl<std::vector<model::internal::broker_v0>>{}.from(
          p);
        std::transform(
          brokers_v0.begin(),
          brokers_v0.end(),
          std::back_inserter(brokers),
          [](const model::internal::broker_v0& broker) {
              return broker.to_v3();
          });
    }

    raft::group_nodes current;
    std::optional<raft::group_nodes> old;

    if (likely(version >= 2)) {
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
    if (version > 0) {
        revision = adl<model::revision_id>{}.from(p);
    }
    return raft::group_configuration(
      std::move(brokers), std::move(current), revision, std::move(old));
}

void adl<raft::vnode>::to(iobuf& buf, raft::vnode id) {
    serialize(buf, id.id(), id.revision());
}

raft::vnode adl<raft::vnode>::from(iobuf_parser& p) {
    auto id = adl<model::node_id>{}.from(p);
    auto rev = adl<model::revision_id>{}.from(p);
    return raft::vnode(id, rev);
}

} // namespace reflection
