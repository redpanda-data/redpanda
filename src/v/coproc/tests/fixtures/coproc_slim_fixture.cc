/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_slim_fixture.h"

#include "coproc/tests/fixtures/fixture_utils.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "storage/kvstore.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "test_utils/async.h"

#include <variant>

coproc_slim_fixture::coproc_slim_fixture() {
    std::filesystem::create_directory(_data_dir);
    ss::smp::invoke_on_all([this]() mutable {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(1000ms);
        config.get("data_directory")
          .set_value(config::data_directory_path{.path = _data_dir});
    }).get();
}

coproc_slim_fixture::~coproc_slim_fixture() {
    _abort_src.request_abort();
    stop().get();
    std::filesystem::remove_all(_data_dir);
}

ss::future<> coproc_slim_fixture::enable_coprocessors(
  std::vector<coproc_fixture_iface::deploy> data) {
    std::vector<coproc::enable_copros_request::data> copy;
    std::vector<coproc::enable_copros_request::data> d;
    copy.reserve(data.size());
    d.reserve(data.size());
    for (const auto& e : data) {
        copy.emplace_back(coproc::enable_copros_request::data{
          .id = coproc::script_id(e.id),
          .source_code = reflection::to_iobuf(e.data)});
        d.emplace_back(coproc::enable_copros_request::data{
          .id = coproc::script_id(e.id),
          .source_code = reflection::to_iobuf(e.data)});
    }
    _cached_requests.emplace_back(
      coproc::enable_copros_request{.inputs = std::move(copy)});
    return get_script_dispatcher()
      ->enable_coprocessors(
        coproc::enable_copros_request{.inputs = std::move(d)})
      .discard_result();
}

ss::future<>
coproc_slim_fixture::disable_coprocessors(std::vector<uint64_t> ids) {
    std::vector<coproc::script_id> d;
    d.reserve(ids.size());
    std::transform(
      ids.begin(), ids.end(), std::back_inserter(d), [](uint64_t id) {
          return coproc::script_id(id);
      });
    coproc::disable_copros_request req{.ids = std::move(d)};
    _cached_requests.push_back(req);
    return get_script_dispatcher()
      ->disable_coprocessors(std::move(req))
      .discard_result();
}

ss::future<> coproc_slim_fixture::setup(log_layout_map llm) {
    _llm = llm;
    return start();
}

ss::future<> coproc_slim_fixture::restart() {
    return stop().then([this] { return start(); });
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
          return ss::do_for_each(_llm, [this](const auto& item) {
              return add_ntps(item.first, item.second);
          });
      })
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
          return _script_dispatcher->disable_all_coprocessors()
            .discard_result();
      })
      .then([this] {
          return ss::do_for_each(_cached_requests, [this](auto& req) {
              if (std::holds_alternative<coproc::enable_copros_request>(req)) {
                  return get_script_dispatcher()
                    ->enable_coprocessors(
                      std::get<coproc::enable_copros_request>(std::move(req)))
                    .discard_result();
              } else {
                  return get_script_dispatcher()
                    ->disable_coprocessors(
                      std::get<coproc::disable_copros_request>(std::move(req)))
                    .discard_result();
              }
          });
      });
}

ss::future<> coproc_slim_fixture::stop() {
    { auto sd = std::move(_script_dispatcher); }
    return _pacemaker.stop().then([this] { return _storage.stop(); });
}

ss::shard_id shard_for_ntp(const model::ntp& ntp) {
    return std::hash<model::ntp>{}(ntp) % ss::smp::count;
}

ss::future<>
coproc_slim_fixture::add_ntps(const model::topic& topic, size_t n) {
    auto r = boost::irange<size_t>(0, n);
    return ss::do_for_each(r, [this, topic](size_t i) {
        model::ntp ntp(model::kafka_namespace, topic, model::partition_id(i));
        auto shard_id = shard_for_ntp(ntp);
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

ss::future<model::offset> coproc_slim_fixture::push(
  const model::ntp& ntp, model::record_batch_reader rbr) {
    /// TODO: Possible mutex for pushing to the same logs from different cores?
    vassert(
      !model::is_materialized_topic(ntp.tp.topic),
      "Cannot push to a materialized topic");
    auto shard_id = shard_for_ntp(ntp);
    return _storage.invoke_on(
      shard_id, [ntp, rbr = std::move(rbr)](storage::api& api) mutable {
          auto log = api.log_mgr().get(ntp);
          if (!log) {
              return ss::make_ready_future<model::offset>();
          }
          const storage::log_append_config write_cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout};
          return std::move(rbr)
            .for_each_ref(log->make_appender(write_cfg), model::no_timeout)
            .then([log](storage::append_result ar) { return ar.last_offset; });
      });
}

ss::future<std::optional<model::record_batch_reader::data_t>>
coproc_slim_fixture::drain(
  const model::ntp& ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    model::materialized_ntp mntp(ntp);
    auto shard_id = shard_for_ntp(mntp.source_ntp());
    return _storage.invoke_on(
      shard_id, [mntp, limit, offset, timeout](storage::api& api) {
          return tests::cooperative_spin_wait_with_timeout(
                   60s,
                   [&api, mntp] {
                       return api.log_mgr().get(mntp.input_ntp()).has_value();
                   })
            .then([&api, mntp, limit, offset, timeout] {
                auto log = api.log_mgr().get(mntp.input_ntp());
                return do_drain(
                         offset,
                         limit,
                         timeout,
                         [log = *log](model::offset next_offset) mutable {
                             return log.make_reader(log_rdr_cfg(next_offset));
                         })
                  .then([](auto data) {
                      return std::optional<model::record_batch_reader::data_t>(
                        std::move(data));
                  });
            });
      });
}
