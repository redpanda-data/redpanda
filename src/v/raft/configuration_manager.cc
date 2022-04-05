// Copyright 2020 Redpanda Data, Inc.
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
    auto [it, _] = _configurations.emplace(
      model::offset{},
      indexed_configuration(std::move(initial_cfg), _next_index++));
    vlog(
      _ctxlog.trace,
      "Initial configuration: {}, idx: {}",
      it->second.cfg,
      it->second.idx);
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
        if (it != _configurations.end()) {
            _next_index = it->second.idx;
        }
        _configurations.erase(it, _configurations.end());

        _highest_known_offset = std::min(offset, _highest_known_offset);
        return store_highest_known_offset().then(
          [this] { return store_configurations(); });
    });
}

ss::future<> configuration_manager::prefix_truncate(model::offset offset) {
    vlog(_ctxlog.trace, "Prefix truncating configurations at {}", offset);
    return _lock.with([this, offset] {
        vassert(
          !_configurations.empty(),
          "Configuration manager should always have at least one "
          "configuration");
        /**
         * When prefix truncation would remove all the configuration we instert
         * the last configuration from before requested offset at the offset.
         * This way we preserver last know configuration and indexing.
         *
         *                          200
         *                           │
         *                           │
         *                           │ prefix truncate
         *                           │
         * ┌───────┬───────┐         ▼
         * │   0   │  100  │...........................
         * └───────┴───┬───┘
         *             │              ┌───────┐
         *             └─────────────►│  200  │...............
         *                            └───────┘
         *                   move last know configuration to truncate offset
         *
         * Another situation is when prefix truncate happen in between the
         * configurations f.e:
         *            200
         *             │
         *             │
         *             │ prefix truncate
         *             │
         * ┌───────┐   ▼     ┌───────┐
         * │   0   │........ │  800  │...........................
         * └───────┘         └───────┘
         *     │                      ┌───────┐       ┌───────┐
         *     └─────────────────────►│  200  │.......│  800  │....
         *                            └───────┘       └───────┘
         *
         *  NOTE: box with number represent an entry in configuration manager
         */

        // special case, do nothig if we are asked to truncate before or exactly
        // at the beggining
        if (_configurations.begin()->first >= offset) {
            return ss::now();
        }

        auto it = _configurations.upper_bound(offset);

        auto config = std::move(std::prev(it)->second);
        _configurations.erase(_configurations.begin(), it);
        const auto [_, success] = _configurations.emplace(
          offset, std::move(config));
        vassert(
          success,
          "Inserting configuration after prefix truncate must succeed, "
          "truncation offset: {}, current state: {}",
          offset,
          *this);

        _highest_known_offset = std::max(offset, _highest_known_offset);

        /**
         * store index of first configuration to recover indexing
         */
        auto next_index = _configurations.begin()->second.idx;
        return store_highest_known_offset()
          .then([this, next_index] {
              return _storage.kvs().put(
                storage::kvstore::key_space::consensus,
                next_configuration_idx_key(),
                reflection::to_iobuf(next_index));
          })
          .then([this] { return store_configurations(); });
    });
}

void configuration_manager::add_configuration(
  model::offset offset, group_configuration cfg) {
    auto idx = _next_index++;
    vlog(
      _ctxlog.trace,
      "Adding configuration at offset {} index {}: {}",
      offset,
      idx,
      cfg);
    auto [_, success] = _configurations.try_emplace(
      offset, indexed_configuration(std::move(cfg), idx));
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

    return _lock.with([this, cfg = std::move(cfg), offset]() mutable {
        auto it = _configurations.find(offset);
        // we already have this configuration, do nothing
        // this may happen if configuration is the last batch of the snapshot
        if (it != _configurations.end() && it->second.cfg == cfg) {
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
    return _configurations.rbegin()->second.cfg;
}

model::offset configuration_manager::get_latest_offset() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one configuration");
    return _configurations.rbegin()->first;
}

configuration_manager::configuration_idx
configuration_manager::get_latest_index() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one configuration");
    return _configurations.rbegin()->second.idx;
}
std::optional<group_configuration>
configuration_manager::get(model::offset offset) const {
    auto it = _configurations.lower_bound(offset);
    if (it != _configurations.end() && it->first == offset) {
        return it->second.cfg;
    }
    // we are returning previous configuration as this is the one that was
    // active for requested offset
    if (it != _configurations.begin()) {
        return std::prev(it)->second.cfg;
    }

    return std::nullopt;
}

ss::future<iobuf>
serialize_configurations(const configuration_manager::underlying_t& cfgs) {
    return ss::do_with(iobuf(), [&cfgs](iobuf& ret) {
        reflection::adl<uint64_t>{}.to(ret, cfgs.size());
        return ss::do_for_each(
                 cfgs.cbegin(),
                 cfgs.cend(),
                 [&ret](const auto& p) mutable {
                     reflection::serialize(ret, p.first, p.second.cfg);
                 })
          .then([&ret] { return std::move(ret); });
    });
}

ss::future<configuration_manager::underlying_t> deserialize_configurations(
  configuration_manager::configuration_idx initial, iobuf&& buf) {
    using ret_t = configuration_manager::underlying_t;
    return ss::do_with(
      iobuf_parser(std::move(buf)),
      ret_t{},
      [initial](iobuf_parser& parser, ret_t& configs) {
          auto size = reflection::adl<uint64_t>{}.from(parser);
          return ss::do_with(
            boost::irange<uint64_t>(0, size),
            [&configs, &parser, initial](
              boost::integer_range<uint64_t>& range) {
                return ss::do_for_each(
                         range,
                         [&parser, &configs, initial](uint64_t i) mutable {
                             auto key = reflection::adl<model::offset>{}.from(
                               parser);
                             auto value = reflection::adl<group_configuration>{}
                                            .from(parser);
                             auto [_, success] = configs.try_emplace(
                               key,
                               configuration_manager::indexed_configuration(
                                 std::move(value),
                                 initial
                                   + configuration_manager::configuration_idx(
                                     i)));
                             vassert(
                               success,
                               "Duplicated configuration key at offset {}",
                               key);
                         })
                  .then([&configs]() mutable { return std::move(configs); });
            });
      });
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
    auto idx_buf = _storage.kvs().get(
      storage::kvstore::key_space::consensus, next_configuration_idx_key());
    return _lock.with([this,
                       map_buf = std::move(map_buf),
                       idx_buf = std::move(idx_buf)]() mutable {
        auto f = ss::now();

        if (map_buf) {
            _next_index = configuration_idx(0);
            if (idx_buf) {
                _next_index = reflection::from_iobuf<configuration_idx>(
                  std::move(*idx_buf));
            }
            f = deserialize_configurations(_next_index, std::move(*map_buf))
                  .then([this](underlying_t cfgs) {
                      _configurations = std::move(cfgs);
                      if (!_configurations.empty()) {
                          _highest_known_offset
                            = _configurations.rbegin()->first;
                          _next_index = _configurations.rbegin()->second.idx
                                        + configuration_idx(1);
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
            for (auto& [o, icfg] : _configurations) {
                icfg.cfg.maybe_set_initial_revision(_initial_revision);
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
      .remove(
        storage::kvstore::key_space::consensus, highest_known_offset_key())
      .then([this] {
          return _storage.kvs().remove(
            storage::kvstore::key_space::consensus,
            next_configuration_idx_key());
      })
      .then([this] {
          return _storage.kvs().remove(
            storage::kvstore::key_space::consensus, configurations_map_key());
      });
}

model::revision_id configuration_manager::get_latest_revision() const {
    vassert(
      !_configurations.empty(),
      "Configuration manager should always have at least one "
      "configuration");
    return _configurations.rbegin()->second.cfg.revision_id();
}

int64_t configuration_manager::offset_delta(model::offset o) const {
    auto it = lower_bound(o);

    if (it == begin()) {
        /**
         * iterator points to the first configuration with offset greater
         * than then requsted one. Knowing an index of that configuration we
         * know that there was exactly (index -1) configurations with offset
         * lower than the current one. We can simply subtract one from
         * index.
         */
        return std::max<int64_t>(0, it->second.idx() - 1);
    }

    return std::prev(it)->second.idx();
}

ss::future<> configuration_manager::adjust_configuration_idx(
  configuration_idx new_initial_idx) {
    return _storage.kvs()
      .put(
        storage::kvstore::key_space::consensus,
        next_configuration_idx_key(),
        reflection::to_iobuf(new_initial_idx))
      .then([this, new_initial_idx] {
          auto idx = new_initial_idx;
          for (auto& [_, cfg] : _configurations) {
              cfg.idx = idx++;
          }
          _next_index = idx;
      });
}

std::ostream& operator<<(std::ostream& o, const configuration_manager& m) {
    o << "{configurations: ";
    for (const auto& p : m._configurations) {
        o << "{ offset: " << p.first << ", idx: " << p.second.idx
          << ",cfg: " << p.second.cfg << " } " << std::endl;
    }

    return o << " }";
}

} // namespace raft
