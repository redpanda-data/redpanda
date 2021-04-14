/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/coproc_slim_fixture.h"

#include "coproc/types.h"
#include "model/namespace.h"
#include "storage/kvstore.h"
#include "storage/ntp_config.h"
#include "storage/types.h"

coproc_slim_fixture::coproc_slim_fixture() {
    std::filesystem::create_directory(_data_dir);
}

coproc_slim_fixture::~coproc_slim_fixture() {
    stop().get();
    std::filesystem::remove_all(_data_dir);
}

ss::future<> coproc_slim_fixture::setup(log_layout_map llm) {
    return start().then([this, llm = std::move(llm)]() mutable {
        return ss::do_with(std::move(llm), [this](const auto& llm) {
            return ss::do_for_each(llm, [this](const auto& item) {
                return add_ntps(item.first, item.second);
            });
        });
    });
}

ss::future<std::set<ss::shard_id>>
coproc_slim_fixture::shards_for_topic(const model::topic& t) {
    model::topic_namespace tn(model::kafka_namespace, t);
    return _storage.map_reduce0(
      [tn](storage::api& api) -> std::optional<ss::shard_id> {
          return api.log_mgr().get(tn).empty()
                   ? std::nullopt
                   : std::optional<ss::shard_id>(ss::this_shard_id());
      },
      std::set<ss::shard_id>(),
      [](std::set<ss::shard_id> acc, std::optional<ss::shard_id> opt) {
          if (opt) {
              acc.insert(*opt);
          }
          return acc;
      });
}

ss::future<> coproc_slim_fixture::start() {
    return _storage.start(kvstore_config(), log_config())
      .then([this] { return _storage.invoke_on_all(&storage::api::start); })
      .then([this] {
          return _pacemaker.start(
            unresolved_address("127.0.0.1", 43189), std::ref(_storage));
      })
      .then(
        [this] { return _pacemaker.invoke_on_all(&coproc::pacemaker::start); })
      .then([this] {
          _script_dispatcher
            = std::make_unique<coproc::wasm::script_dispatcher>(
              _pacemaker, _abort_src);
      });
}

ss::future<> coproc_slim_fixture::stop() {
    _abort_src.request_abort();
    { auto sd = std::move(_script_dispatcher); }
    return _pacemaker.stop().then([this] { return _storage.stop(); });
}

ss::future<>
coproc_slim_fixture::add_ntps(const model::topic& topic, size_t n) {
    auto r = boost::irange<size_t>(0, n);
    return ss::do_for_each(r, [this, topic](size_t i) {
        model::ntp ntp(model::kafka_namespace, topic, model::partition_id(i));
        auto shard_id = std::hash<model::ntp>{}(ntp) % ss::smp::count;
        return _storage.invoke_on(
          shard_id, [ntp, dir = _data_dir](storage::api& api) {
              return api.log_mgr()
                .manage(storage::ntp_config(ntp, dir.string()))
                .discard_result();
          });
    });
}

storage::kvstore_config coproc_slim_fixture::kvstore_config() const {
    return storage::kvstore_config(
      16_MiB,
      std::chrono::milliseconds(10),
      _data_dir.string(),
      storage::debug_sanitize_files::no);
}

storage::log_config coproc_slim_fixture::log_config() const {
    return storage::log_config(
      storage::log_config::storage_type::disk,
      _data_dir.string(),
      1_GiB,
      storage::debug_sanitize_files::no);
}
