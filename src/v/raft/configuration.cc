// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/configuration.h"

#include "model/metadata.h"
#include "raft/consensus_utils.h"

#include <absl/container/flat_hash_set.h>
#include <bits/stdint-uintn.h>

#include <algorithm>
#include <iterator>
#include <optional>

namespace raft {

group_configuration::group_configuration(std::vector<model::broker> brokers)
  : _brokers(std::move(brokers)) {
    _current.voters.resize(brokers.size());
    std::transform(
      std::cbegin(_brokers),
      std::cend(_brokers),
      std::back_inserter(_current.voters),
      [](const model::broker& br) { return br.id(); });
}

/**
 * Creates joint configuration
 */
group_configuration::group_configuration(
  std::vector<model::broker> brokers,
  group_nodes current,
  std::optional<group_nodes> old)
  : _brokers(std::move(brokers))
  , _current(std::move(current))
  , _old(std::move(old)) {}

std::optional<model::broker>
group_configuration::find(model::node_id id) const {
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

bool group_configuration::is_voter(model::node_id id) const {
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

std::vector<model::node_id> unique_ids(
  const std::vector<model::node_id>& current,
  const std::vector<model::node_id>& old) {
    absl::flat_hash_set<model::node_id> unique_ids;
    unique_ids.reserve(current.size());

    for (auto& id : current) {
        unique_ids.insert(id);
    }
    for (auto& id : old) {
        unique_ids.insert(id);
    }
    std::vector<model::node_id> ret;
    ret.reserve(unique_ids.size());
    std::copy(unique_ids.begin(), unique_ids.end(), std::back_inserter(ret));
    return ret;
}

std::vector<model::node_id> group_configuration::unique_voter_ids() const {
    auto old_voters = _old ? _old->voters : std::vector<model::node_id>();
    return unique_ids(_current.voters, old_voters);
}
std::vector<model::node_id> group_configuration::unique_learner_ids() const {
    auto old_learners = _old ? _old->learners : std::vector<model::node_id>();
    return unique_ids(_current.learners, old_learners);
}

void erase_id(std::vector<model::node_id>& v, model::node_id id) {
    auto it = std::find(std::cbegin(v), std::cend(v), id);
    if (it != std::cend(v)) {
        v.erase(it);
    }
}

void group_configuration::add(std::vector<model::broker> brokers) {
    vassert(!_old, "can not add broker to joint configuration - {}", *this);

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

    // FIXME: add to learners when we will implement learners promotion
    _old = _current;
    for (auto& b : brokers) {
        _current.voters.push_back(b.id());
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

void group_configuration::replace(std::vector<model::broker> brokers) {
    vassert(
      !_old, "can not remove broker from joint configuration - {}", *this);

    // add missing brokers, brokers have to contain both the new and old nodes
    _old = _current;
    _current.learners.clear();
    _current.voters.clear();
    std::transform(
      std::cbegin(brokers),
      std::cend(brokers),
      std::back_inserter(_current.voters),
      [](const model::broker& broker) { return broker.id(); });

    for (auto& b : brokers) {
        if (!contains_broker(b.id())) {
            _brokers.push_back(std::move(b));
        }
    }
}

void group_configuration::discard_old_config() {
    vassert(
      _old,
      "can not discard old configuration as configuration is of simple type - "
      "{}",
      *this);
    absl::flat_hash_set<model::node_id> ids;

    for (auto& id : _current.learners) {
        ids.insert(id);
    }

    for (auto& id : _current.voters) {
        ids.insert(id);
    }
    // remove unused brokers from brokers set
    auto it = std::stable_partition(
      std::begin(_brokers), std::end(_brokers), [ids](const model::broker& b) {
          return ids.contains(b.id());
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

std::ostream& operator<<(std::ostream& o, const group_configuration& c) {
    fmt::print(
      o,
      "{{current: {}, old:{}, brokers: {}}}",
      c._current,
      c._old,
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
      cfg.old_config());
}

raft::group_configuration
adl<raft::group_configuration>::from(iobuf_parser& p) {
    auto version = adl<uint8_t>{}.from(p);
    // currently we support only version 1
    vassert(
      version == raft::group_configuration::current_version,
      "Version {} is not supported. We only support version {}",
      version,
      raft::group_configuration::current_version);

    auto brokers = adl<std::vector<model::broker>>{}.from(p);
    auto current = adl<raft::group_nodes>{}.from(p);
    auto old = adl<std::optional<raft::group_nodes>>{}.from(p);
    return raft::group_configuration(
      std::move(brokers), std::move(current), std::move(old));
}

} // namespace reflection
