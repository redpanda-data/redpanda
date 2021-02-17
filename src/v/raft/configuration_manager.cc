// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/configuration_manager.h"

#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "raft/consensus_utils.h"
#include "raft/types.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "vlog.h"

#include <absl/container/btree_map.h>
#include <boost/range/irange.hpp>

namespace raft {

configuration_manager::configuration_manager(
  group_configuration initial_cfg,
  raft::group_id group,
  storage::api& storage,
  ctx_log& log)
  : _group(group)
  , _storage(storage)
  , _ctxlog(log) {
    vlog(_ctxlog.trace, "Initial configuration: {}", initial_cfg);
    _configurations.emplace(model::offset{}, std::move(initial_cfg));
}

ss::future<> configuration_manager::truncate(model::offset offset) {
    vlog(_ctxlog.trace, "Truncating configurations at {}", offset);
    if (unlikely(offset <= _configurations.begin()->first)) {
        return ss::make_exception_future<>(std::invalid_argument(fmt::format(
          "can not truncate with offsets, lower or equal than the first one {} "
          "included in the manager ",
          _configurations.begin()->first)));
    }

    return _lock.with([this, offset] {
        auto it = _configurations.lower_bound(offset);
        _configurations.erase(it, _configurations.end());

        _highest_known_offset = std::min(offset, _highest_known_offset);
        return store_highest_known_offset().then(
          [this] { return store_configurations(); });
    });
}

ss::future<> configuration_manager::prefix_truncate(model::offset offset) {
    vlog(_ctxlog.trace, "Prefix truncating configurations at {}", offset);
    return _lock.with([this, offset] {
        auto it = _configurations.lower_bound(offset);
        if (it == _configurations.end()) {
            // we can not prefix truncate all configurations
            return ss::make_exception_future<>(
              std::invalid_argument(fmt::format(
                "can not prefix truncate configuration manager at {} as last "
                "available configuration has offset {}",
                offset,
                get_latest_offset())));
        }
        _configurations.erase(_configurations.begin(), it);
        _highest_known_offset = std::max(offset, _highest_known_offset);
        return store_highest_known_offset().then(
          [this] { return store_configurations(); });
    });
}

void configuration_manager::add_configuration(
  model::offset offset, group_configuration cfg) {
    auto [_, success] = _configurations.try_emplace(offset, std::move(cfg));
    if (!success) {
        throw std::invalid_argument(fmt::format(
          "Unable to add configuration at offset {} as it "
          "already exists",
          offset));
    }
}

ss::future<>
configuration_manager::add(std::vector<offset_configuration> configurations) {
    return _lock.with([this,
                       configurations = std::move(configurations)]() mutable {
        for (auto& co : configurations) {
            // handling backward compatibility i.e. revisionless configurations
            co.cfg.maybe_set_initial_revision(_initial_revision);
            vlog(
              _ctxlog.trace,
              "Adding configuration: {}, offset: {}",
              co.cfg,
              co.offset);
            add_configuration(co.offset, std::move(co.cfg));
            _highest_known_offset = std::max(_highest_known_offset, co.offset);
        }
        _config_changed.broadcast();
        return store_configurations().then(
          [this] { return store_highest_known_offset(); });
    });
}

ss::future<>
configuration_manager::add(model::offset offset, group_configuration cfg) {
    // handling backward compatibility i.e. revisionless configurations
    cfg.maybe_set_initial_revision(_initial_revision);

    vlog(_ctxlog.trace, "Adding configuration: {}, offset: {}", cfg, offset);
    return _lock.with([this, cfg = std::move(cfg), offset]() mutable {
        auto it = _configurations.find(offset);
        // we already have this configuration, do nothing
        // this may happen if configuration is the last batch of the snapshot
        if (it != _configurations.end() && it->second == cfg) {
            return ss::now();
        }

        add_configuration(offset, std::move(cfg));
        _highest_known_offset = std::max(offset, _highest_known_offset);
        _config_changed.broadcast();
        return store_configurations().then(
          [this] { return store_highest_known_offset(); });
    });
}

const group_configuration& configuration_manager::get_latest() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one configuration");
    return _configurations.rbegin()->second;
}

model::offset configuration_manager::get_latest_offset() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one configuration");
    return _configurations.rbegin()->first;
}

std::optional<group_configuration>
configuration_manager::get(model::offset offset) const {
    auto it = _configurations.lower_bound(offset);
    if (it != _configurations.end() && it->first == offset) {
        return it->second;
    }
    // we are returning previous configuration as this is the one that was
    // active for requested offset
    if (it != _configurations.begin()) {
        return std::prev(it)->second;
    }

    return std::nullopt;
}

ss::future<iobuf> serialize_configurations(
  const absl::btree_map<model::offset, group_configuration>& cfgs) {
    return ss::do_with(iobuf(), [&cfgs](iobuf& ret) {
        reflection::adl<uint64_t>{}.to(ret, cfgs.size());
        return ss::do_for_each(
                 cfgs.cbegin(),
                 cfgs.cend(),
                 [&ret](const auto& p) mutable {
                     reflection::serialize(ret, p.first, p.second);
                 })
          .then([&ret] { return std::move(ret); });
    });
}

ss::future<absl::btree_map<model::offset, group_configuration>>
deserialize_configurations(iobuf&& buf) {
    using ret_t = absl::btree_map<model::offset, group_configuration>;
    return ss::do_with(
      iobuf_parser(std::move(buf)),
      ret_t{},
      [](iobuf_parser& parser, ret_t& configs) {
          auto size = reflection::adl<uint64_t>{}.from(parser);
          return ss::do_with(
            boost::irange<uint64_t>(0, size),
            [&configs, &parser](boost::integer_range<uint64_t>& range) {
                return ss::do_for_each(
                         range,
                         [&parser, &configs](uint64_t) mutable {
                             auto key = reflection::adl<model::offset>{}.from(
                               parser);
                             auto value = reflection::adl<group_configuration>{}
                                            .from(parser);
                             auto [_, success] = configs.try_emplace(
                               key, std::move(value));
                             vassert(
                               success,
                               "Duplicated configuration key at offset {}",
                               key);
                         })
                  .then([&configs]() mutable { return std::move(configs); });
            });
      });
}

bytes configuration_manager::configurations_map_key() {
    iobuf buf;
    reflection::serialize(buf, metadata_key::config_map, _group);
    return iobuf_to_bytes(buf);
}

bytes configuration_manager::highest_known_offset_key() {
    iobuf buf;
    reflection::serialize(
      buf, metadata_key::config_latest_known_offset, _group);
    return iobuf_to_bytes(buf);
}

ss::future<> configuration_manager::store_configurations() {
    return serialize_configurations(_configurations).then([this](iobuf buf) {
        return _storage.kvs().put(
          storage::kvstore::key_space::consensus,
          configurations_map_key(),
          std::move(buf));
    });
}

ss::future<> configuration_manager::store_highest_known_offset() {
    return _storage.kvs().put(
      storage::kvstore::key_space::consensus,
      highest_known_offset_key(),
      reflection::to_iobuf(_highest_known_offset));
}

ss::future<> configuration_manager::stop() {
    _config_changed.broken();
    return ss::now();
}

ss::future<>
configuration_manager::start(bool reset, model::revision_id initial_revision) {
    _initial_revision = initial_revision;
    if (reset) {
        return _storage.kvs()
          .remove(
            storage::kvstore::key_space::consensus, configurations_map_key())
          .then([this] {
              return _storage.kvs().remove(
                storage::kvstore::key_space::consensus,
                highest_known_offset_key());
          });
    }

    auto map_buf = _storage.kvs().get(
      storage::kvstore::key_space::consensus, configurations_map_key());
    return _lock.with([this, map_buf = std::move(map_buf)]() mutable {
        auto f = ss::now();

        if (map_buf) {
            f = deserialize_configurations(std::move(*map_buf))
                  .then([this](underlying_t cfgs) {
                      _configurations = std::move(cfgs);
                      if (!_configurations.empty()) {
                          _highest_known_offset
                            = _configurations.rbegin()->first;
                      }
                  });
        }

        auto offset_buf = _storage.kvs().get(
          storage::kvstore::key_space::consensus, highest_known_offset_key());
        if (offset_buf) {
            f = f.then([this, buf = std::move(*offset_buf)]() mutable {
                auto offset = reflection::from_iobuf<model::offset>(
                  std::move(buf));

                _highest_known_offset = std::max(_highest_known_offset, offset);
            });
        }

        return f.then([this] {
            for (auto& [o, cfg] : _configurations) {
                cfg.maybe_set_initial_revision(_initial_revision);
            }
        });
    });
}

ss::future<> configuration_manager::maybe_store_highest_known_offset(
  model::offset offset, size_t bytes) {
    _highest_known_offset = offset;
    _bytes_since_last_offset_update += bytes;

    if (_bytes_since_last_offset_update < offset_update_treshold) {
        return ss::now();
    }

    _bytes_since_last_offset_update = 0;
    return store_highest_known_offset();
}

ss::future<offset_configuration> configuration_manager::wait_for_change(
  model::offset last_seen_offset, ss::abort_source& as) {
    auto latest_offset = get_latest_offset();
    if (latest_offset > last_seen_offset) {
        auto cfg = get(last_seen_offset);
        return ss::make_ready_future<offset_configuration>(
          offset_configuration(latest_offset, get_latest()));
    }
    // we can wake up all waiters as they will check the predicate and continue
    // waiting
    auto sub = as.subscribe([this]() noexcept { _config_changed.broadcast(); });
    if (!sub) {
        // already aborted
        return ss::make_exception_future<offset_configuration>(
          ss::abort_requested_exception{});
    }
    // store it in shared_ptr so we can keep subscription alive
    auto s = ss::make_lw_shared<ss::abort_source::subscription>(
      std::move(*sub));
    return _config_changed
      .wait([this, last_seen_offset, &as, s] {
          return get_latest_offset() > last_seen_offset || as.abort_requested();
      })
      .then([this, &as] {
          if (unlikely(as.abort_requested())) {
              return ss::make_exception_future<offset_configuration>(
                ss::abort_requested_exception{});
          }
          return ss::make_ready_future<offset_configuration>(
            offset_configuration(get_latest_offset(), get_latest()));
      });
}

ss::future<> configuration_manager::remove_persistent_state() {
    return _storage.kvs()
      .remove(storage::kvstore::key_space::consensus, configurations_map_key())
      .then([this] {
          return _storage.kvs().remove(
            storage::kvstore::key_space::consensus, highest_known_offset_key());
      });
}

model::revision_id configuration_manager::get_latest_revision() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one "
      "configuration");
    return _configurations.rbegin()->second.revision_id();
}

std::ostream& operator<<(std::ostream& o, const configuration_manager& m) {
    o << "{configurations: ";
    for (const auto& p : m._configurations) {
        o << "{ offset: " << p.first << " cfg: " << p.second << " } ";
    }

    return o << " }";
}

} // namespace raft
