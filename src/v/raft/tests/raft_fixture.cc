
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "raft/tests/raft_fixture.h"

#include "bytes/iobuf_parser.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/coordinated_recovery_throttle.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/heartbeat_manager.h"
#include "raft/heartbeats.h"
#include "raft/state_machine_manager.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "utils/prefix_logger.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/file.hh>

namespace raft {

static ss::logger test_log("raft-test");

channel::channel(raft_node_instance& node)
  : _node(node.weak_from_this()) {}

void channel::start() { start_dispatch_loop(); }

ss::future<> channel::stop() {
    if (!_as.abort_requested()) {
        _as.request_abort();
        _new_messages.broken();

        auto f = _gate.close();

        for (auto& m : _messages) {
            m.resp_data.set_exception(ss::abort_requested_exception());
        }
        co_await std::move(f);
    }
}

void channel::start_dispatch_loop() {
    ssx::spawn_with_gate(_gate, [this] { return dispatch_loop(); });
}

ss::future<iobuf> channel::exchange(msg_type type, iobuf request) {
    auto holder = _gate.hold();

    msg m{.type = type, .req_data = std::move(request)};
    auto f = m.resp_data.get_future();
    _messages.push_back(std::move(m));
    _new_messages.broadcast();

    co_return co_await std::move(f);
}
bool channel::is_valid() const { return _node && _node->raft() != nullptr; }

ss::lw_shared_ptr<consensus> channel::raft() {
    if (!_node || _node->raft() == nullptr) {
        throw std::runtime_error("no raft group");
    }
    return _node->raft();
}

ss::future<> channel::dispatch_loop() {
    while (!_as.abort_requested()) {
        co_await _new_messages.wait([this] { return !_messages.empty(); });

        auto msg = std::move(_messages.front());
        _messages.pop_front();
        iobuf_parser req_parser(std::move(msg.req_data));

        try {
            switch (msg.type) {
            case msg_type::vote: {
                auto req = co_await serde::read_async<vote_request>(req_parser);
                auto resp = co_await raft()->vote(std::move(req));
                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(resp));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::append_entries: {
                auto req = co_await serde::read_async<append_entries_request>(
                  req_parser);
                auto resp = co_await raft()->append_entries(std::move(req));
                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(resp));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::heartbeat: {
                auto req = co_await serde::read_async<heartbeat_request>(
                  req_parser);
                heartbeat_reply reply;
                for (auto& hb : req.heartbeats) {
                    auto resp = co_await raft()->append_entries(
                      append_entries_request(
                        hb.node_id,
                        hb.meta,
                        model::make_memory_record_batch_reader(
                          ss::circular_buffer<model::record_batch>{}),
                        flush_after_append::no));
                    reply.meta.push_back(resp);
                }

                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(reply));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::heartbeat_v2: {
                auto req = co_await serde::read_async<heartbeat_request_v2>(
                  req_parser);
                heartbeat_reply_v2 reply(raft()->self().id(), req.source());

                for (auto& hb : req.full_heartbeats()) {
                    auto resp = co_await raft()->full_heartbeat(
                      hb.group, req.source(), req.target(), hb.data);

                    reply.add(resp.group, resp.result, resp.data);
                }
                req.for_each_lw_heartbeat(
                  [this, &req, &reply](raft::group_id g) {
                      auto result = raft()->lightweight_heartbeat(
                        req.source(), req.target());
                      reply.add(g, result);
                  });

                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(reply));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::install_snapshot: {
                auto req = co_await serde::read_async<install_snapshot_request>(
                  req_parser);
                auto resp = co_await raft()->install_snapshot(std::move(req));
                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(resp));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::timeout_now: {
                auto req = co_await serde::read_async<timeout_now_request>(
                  req_parser);
                auto resp = co_await raft()->timeout_now(std::move(req));
                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(resp));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            case msg_type::transfer_leadership: {
                auto req
                  = co_await serde::read_async<transfer_leadership_request>(
                    req_parser);
                auto resp = co_await raft()->transfer_leadership(
                  std::move(req));
                iobuf resp_buf;
                co_await serde::write_async(resp_buf, std::move(resp));
                msg.resp_data.set_value(std::move(resp_buf));
                break;
            }
            }
        } catch (...) {
            msg.resp_data.set_to_current_exception();
        }
    }
}

in_memory_test_protocol::in_memory_test_protocol(
  raft_node_map& node_map, prefix_logger& logger)
  : _nodes(node_map)
  , _logger(logger) {
    std::ignore = _logger;
}

channel& in_memory_test_protocol::get_channel(model::node_id id) {
    auto it = _channels.find(id);
    if (it == _channels.end()) {
        auto node = _nodes.node_for(id);
        if (!node) {
            throw std::runtime_error(
              fmt::format("unable to find node {} in node map", id));
        }
        auto [new_it, _] = _channels.try_emplace(
          id, std::make_unique<channel>(node->get()));
        it = new_it;
        it->second->start();
    }
    return *it->second;
}

void in_memory_test_protocol::on_dispatch(dispatch_callback_t f) {
    _on_dispatch_handlers.push_back(std::move(f));
}

ss::future<> in_memory_test_protocol::stop() {
    auto f = _gate.close();
    for (auto& [_, ch] : _channels) {
        co_await ch->stop();
    }
    co_await std::move(f);
}

template<typename ReqT>
static constexpr msg_type map_msg_type() {
    if constexpr (std::is_same_v<ReqT, vote_request>) {
        return msg_type::vote;
    } else if constexpr (std::is_same_v<ReqT, append_entries_request>) {
        return msg_type::append_entries;
    } else if constexpr (std::is_same_v<ReqT, heartbeat_request>) {
        return msg_type::heartbeat;
    } else if constexpr (std::is_same_v<ReqT, heartbeat_request_v2>) {
        return msg_type::heartbeat_v2;
    } else if constexpr (std::is_same_v<ReqT, install_snapshot_request>) {
        return msg_type::install_snapshot;
    } else if constexpr (std::is_same_v<ReqT, timeout_now_request>) {
        return msg_type::timeout_now;
    } else if constexpr (std::is_same_v<ReqT, transfer_leadership_request>) {
        return msg_type::transfer_leadership;
    }
    __builtin_unreachable();
}

template<typename ReqT, typename RespT>
ss::future<result<RespT>>
in_memory_test_protocol::dispatch(model::node_id id, ReqT req) {
    _gate.hold();
    auto it = _channels.find(id);
    if (it == _channels.end()) {
        auto node = _nodes.node_for(id);
        if (!node) {
            co_return errc::node_does_not_exists;
        }
        auto [new_it, _] = _channels.try_emplace(
          id, std::make_unique<channel>(node->get()));
        it = new_it;
        it->second->start();
    }
    auto& node_channel = *it->second;

    if (!node_channel.is_valid()) {
        co_await node_channel.stop();
        _channels.erase(id);
        co_return errc::group_not_exists;
    }

    iobuf buffer;
    co_await serde::write_async(buffer, std::move(req));

    const auto msg_type = map_msg_type<ReqT>();
    for (const auto& f : _on_dispatch_handlers) {
        co_await f(id, msg_type);
    }

    try {
        auto resp = co_await node_channel.exchange(msg_type, std::move(buffer));
        iobuf_parser parser(std::move(resp));
        co_return co_await serde::read_async<RespT>(parser);
    } catch (const seastar::gate_closed_exception&) {
        co_return errc::shutting_down;
    }
}

ss::future<result<vote_reply>> in_memory_test_protocol::vote(
  model::node_id id, vote_request&& req, rpc::client_opts) {
    return dispatch<vote_request, vote_reply>(id, req);
};

ss::future<result<append_entries_reply>>
in_memory_test_protocol::append_entries(
  model::node_id id, append_entries_request&& req, rpc::client_opts, bool) {
    return dispatch<append_entries_request, append_entries_reply>(
      id, std::move(req));
};

ss::future<result<heartbeat_reply>> in_memory_test_protocol::heartbeat(
  model::node_id id, heartbeat_request&& req, rpc::client_opts) {
    return dispatch<heartbeat_request, heartbeat_reply>(id, std::move(req));
}

ss::future<result<heartbeat_reply_v2>> in_memory_test_protocol::heartbeat_v2(
  model::node_id id, heartbeat_request_v2&& req, rpc::client_opts) {
    return dispatch<heartbeat_request_v2, heartbeat_reply_v2>(
      id, std::move(req));
}

ss::future<result<install_snapshot_reply>>
in_memory_test_protocol::install_snapshot(
  model::node_id id, install_snapshot_request&& req, rpc::client_opts) {
    return dispatch<install_snapshot_request, install_snapshot_reply>(
      id, std::move(req));
}

ss::future<result<timeout_now_reply>> in_memory_test_protocol::timeout_now(
  model::node_id id, timeout_now_request&& req, rpc::client_opts) {
    return dispatch<timeout_now_request, timeout_now_reply>(id, std::move(req));
}

ss::future<result<transfer_leadership_reply>>
in_memory_test_protocol::transfer_leadership(
  model::node_id id, transfer_leadership_request&& req, rpc::client_opts) {
    return dispatch<transfer_leadership_request, transfer_leadership_reply>(
      id, std::move(req));
}

raft_node_instance::raft_node_instance(
  model::node_id id,
  model::revision_id revision,
  raft_node_map& node_map,
  ss::sharded<features::feature_table>& feature_table,
  leader_update_clb_t leader_update_clb,
  bool enable_longest_log_detection,
  config::binding<std::chrono::milliseconds> election_timeout,
  config::binding<std::chrono::milliseconds> heartbeat_interval)
  : raft_node_instance(
      id,
      revision,
      fmt::format(
        "test_raft_{}_{}", _id, random_generators::gen_alphanum_string(12)),
      node_map,
      feature_table,
      std::move(leader_update_clb),
      enable_longest_log_detection,
      std::move(election_timeout),
      std::move(heartbeat_interval)) {}

raft_node_instance::raft_node_instance(
  model::node_id id,
  model::revision_id revision,
  ss::sstring base_directory,
  raft_node_map& node_map,
  ss::sharded<features::feature_table>& feature_table,
  leader_update_clb_t leader_update_clb,
  bool enable_longest_log_detection,
  config::binding<std::chrono::milliseconds> election_timeout,
  config::binding<std::chrono::milliseconds> heartbeat_interval)
  : _id(id)
  , _revision(revision)
  , _logger(test_log, fmt::format("[node: {}]", _id))
  , _base_directory(std::move(base_directory))
  , _protocol(ss::make_shared<in_memory_test_protocol>(node_map, _logger))
  , _features(feature_table)
  , _recovery_mem_quota([] {
      return raft::recovery_memory_quota::configuration{
        .max_recovery_memory = config::mock_binding<std::optional<size_t>>(
          200_MiB),
        .default_read_buffer_size = config::mock_binding<size_t>(128_KiB),
      };
  })
  , _recovery_scheduler(
      config::mock_binding<size_t>(64), config::mock_binding(10ms))
  , _leader_clb(std::move(leader_update_clb))
  , _enable_longest_log_detection(enable_longest_log_detection)
  , _election_timeout(std::move(election_timeout))
  , _heartbeat_interval(std::move(heartbeat_interval)) {
    config::shard_local_cfg().disable_metrics.set_value(true);
}

ss::future<>
raft_node_instance::initialise(std::vector<raft::vnode> initial_nodes) {
    _hb_manager = std::make_unique<heartbeat_manager>(
      _heartbeat_interval,
      consensus_client_protocol(_protocol),
      _id,
      config::mock_binding<std::chrono::milliseconds>(1000ms),
      config::mock_binding<bool>(true),
      _features.local());
    co_await _hb_manager->start();

    co_await _recovery_throttle.start(
      config::mock_binding<size_t>(100_MiB), config::mock_binding<bool>(false));
    co_await _recovery_throttle.invoke_on_all(
      &coordinated_recovery_throttle::start);

    co_await _storage.start(
      [this]() {
          return storage::kvstore_config(
            8_MiB,
            config::mock_binding<std::chrono::milliseconds>(10ms),
            _base_directory,
            std::nullopt);
      },
      [this] { return storage::log_config(_base_directory, 8_MiB); },
      std::ref(_features));
    co_await _storage.invoke_on_all(&storage::api::start);
    storage::ntp_config ntp_cfg(ntp(), _base_directory);

    auto log = co_await _storage.local().log_mgr().manage(std::move(ntp_cfg));

    _raft = ss::make_lw_shared<consensus>(
      _id,
      test_group,
      raft::group_configuration(std::move(initial_nodes), _revision),
      timeout_jitter(_election_timeout),
      log,
      scheduling_config(
        ss::default_scheduling_group(), ss::default_priority_class()),
      config::mock_binding<std::chrono::milliseconds>(1s),
      config::mock_binding<bool>(_enable_longest_log_detection),
      consensus_client_protocol(_protocol),
      [this](leadership_status ls) { leadership_notification_callback(ls); },
      _storage.local(),
      _recovery_throttle.local(),
      _recovery_mem_quota,
      _recovery_scheduler,
      _features.local());
    co_await _hb_manager->register_group(_raft);
}

ss::future<> raft_node_instance::init_and_start(
  std::vector<vnode> initial_nodes,
  std::optional<raft::state_machine_manager_builder> builder) {
    co_await initialise(std::move(initial_nodes));
    co_await start(std::move(builder));
}

ss::future<> raft_node_instance::start(
  std::optional<raft::state_machine_manager_builder> builder) {
    co_await _raft->start(std::move(builder));
    started = true;
}

ss::future<> raft_node_instance::stop() {
    vlog(_logger.info, "stopping node");
    if (started) {
        co_await _hb_manager->deregister_group(_raft->group());
        vlog(_logger.debug, "stopping protocol");
        co_await _protocol->stop();
        vlog(_logger.debug, "stopping raft");
        co_await _raft->stop();
        vlog(_logger.debug, "stopping recovery throttle");
        co_await _recovery_throttle.stop();
        vlog(_logger.debug, "stopping log");
        co_await _storage.local().log_mgr().shutdown(ntp());
        vlog(_logger.debug, "stopping heartbeat manager");
        co_await _hb_manager->stop();
        vlog(_logger.debug, "stopping feature table");
        _raft = nullptr;
        vlog(_logger.debug, "stopping storage");
        co_await _storage.stop();
    }
    vlog(_logger.info, "node stopped");
}

ss::future<> raft_node_instance::remove_data() {
    return ss::recursive_remove_directory(
      std::filesystem::path(_base_directory));
}

void raft_node_instance::leadership_notification_callback(
  leadership_status status) {
    _logger.info(
      "Leadership notification: [current_leader: {}, term: {}]",
      status.current_leader,
      status.term);
    _leader_clb(status);
}

ss::future<ss::circular_buffer<model::record_batch>>
raft_node_instance::read_all_data_batches() {
    return read_batches_in_range(_raft->start_offset(), model::offset::max());
}

ss::future<ss::circular_buffer<model::record_batch>>
raft_node_instance::read_batches_in_range(
  model::offset min, model::offset max) {
    storage::log_reader_config cfg(min, max, ss::default_priority_class());

    auto rdr = co_await _raft->make_reader(cfg);

    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), default_timeout());

    ss::circular_buffer<model::record_batch> data_batches;
    data_batches.reserve(batches.size());
    /**
     * Filter out raft configurations as when reconfiguring nodes the one
     * leaving cluster may not have all configuration updates
     */
    for (auto& b : batches) {
        if (b.header().type != model::record_batch_type::raft_configuration) {
            data_batches.push_back(std::move(b));
        }
        co_await ss::coroutine::maybe_yield();
    }

    co_return data_batches;
}

ss::future<model::offset> raft_node_instance::random_batch_base_offset(
  model::offset max, std::optional<model::offset> min) {
    model::offset read_start(random_generators::get_int<int64_t>(
      min.value_or(_raft->start_offset()), max));

    model::offset last = model::next_offset(read_start);

    ss::circular_buffer<model::record_batch> batches;
    while (batches.empty() && last <= _raft->dirty_offset()) {
        vlog(
          test_log.info, "Reading batches in range: [{},{}]", read_start, last);
        batches = co_await read_batches_in_range(read_start, last);
        last++;
    }
    if (batches.empty()) {
        co_return model::offset{};
    }
    co_return batches.front().base_offset();
}

void raft_node_instance::on_dispatch(dispatch_callback_t f) {
    _protocol->on_dispatch(std::move(f));
}

seastar::future<> raft_fixture::TearDownAsync() {
    co_await ss::smp::invoke_on_all(
      []() { config::shard_local_cfg().for_each([](auto& p) { p.reset(); }); });

    co_await seastar::coroutine::parallel_for_each(
      _nodes, [](auto& pair) { return pair.second->stop(); });

    co_await seastar::coroutine::parallel_for_each(
      _nodes, [](auto& pair) { return pair.second->remove_data(); });

    co_await _features.stop();
}
seastar::future<> raft_fixture::SetUpAsync() {
    for (auto cpu : ss::smp::all_cpus()) {
        co_await ss::smp::submit_to(cpu, [] {
            config::shard_local_cfg().disable_metrics.set_value(true);
            config::shard_local_cfg().disable_public_metrics.set_value(true);
        });
    }

    co_await _features.start();
    co_await _features.invoke_on_all(
      [](features::feature_table& ft) { return ft.testing_activate_all(); });
}

raft_node_instance&
raft_fixture::add_node(model::node_id id, model::revision_id rev) {
    auto instance = std::make_unique<raft_node_instance>(
      id,
      rev,
      *this,
      _features,
      [id, this](leadership_status lst) {
          _leaders_view[id] = lst;
          if (_leader_clb) {
              _leader_clb.value()(id, lst);
          }
      },
      _enable_longest_log_detection,
      _election_timeout.bind(),
      _heartbeat_interval.bind());

    auto [it, success] = _nodes.emplace(id, std::move(instance));
    return *it->second;
}

raft_node_instance& raft_fixture::add_node(
  model::node_id id, model::revision_id rev, ss::sstring base_dir) {
    auto instance = std::make_unique<raft_node_instance>(
      id,
      rev,
      std::move(base_dir),
      *this,
      _features,
      [id, this](leadership_status lst) {
          _leaders_view[id] = lst;
          if (_leader_clb) {
              _leader_clb.value()(id, lst);
          }
      },
      _enable_longest_log_detection,
      _election_timeout.bind(),
      _heartbeat_interval.bind());

    auto [it, success] = _nodes.emplace(id, std::move(instance));
    return *it->second;
}

ss::future<>
raft_fixture::stop_node(model::node_id id, remove_data_dir remove) {
    co_await node(id).stop();
    _leaders_view.erase(id);
    if (remove) {
        co_await node(id).remove_data();
    }
    _nodes.erase(id);
}

raft_node_instance& raft_fixture::node(model::node_id id) {
    return *_nodes.at(id);
}

ss::future<model::node_id>
raft_fixture::wait_for_leader(model::timeout_clock::time_point deadline) {
    auto has_stable_leader = [this] {
        auto leader_id = get_leader();
        return leader_id && _nodes.contains(*leader_id)
               && node(*leader_id).raft()->is_leader();
    };
    while (!has_stable_leader()) {
        if (model::timeout_clock::now() > deadline) {
            throw std::runtime_error("Timeout waiting for leader");
        }
        co_await ss::sleep(std::chrono::milliseconds(5));
    }

    co_return get_leader().value();
}

ss::future<model::node_id> raft_fixture::wait_for_leader_change(
  model::timeout_clock::time_point deadline, model::term_id term) {
    auto has_new_leader = [this, term] {
        auto leader_id = get_leader();
        if (leader_id && _nodes.contains(*leader_id)) {
            auto& leader_node = node(*leader_id);
            return leader_node.raft()->is_leader()
                   && leader_node.raft()->term() > term;
        }
        return false;
    };

    while (!has_new_leader()) {
        if (model::timeout_clock::now() > deadline) {
            throw std::runtime_error("Timeout waiting for leader");
        }
        co_await ss::sleep(std::chrono::milliseconds(5));
    }

    co_return get_leader().value();
}
ss::future<model::node_id>
raft_fixture::wait_for_leader(std::chrono::milliseconds timeout) {
    return wait_for_leader(timeout + model::timeout_clock::now());
}

std::optional<model::node_id> raft_fixture::get_leader() const {
    model::term_id current_term(-1);
    std::optional<model::node_id> leader;
    for (auto& [_, l_st] : _leaders_view) {
        if (l_st.term >= current_term && l_st.current_leader) {
            leader = l_st.current_leader->id();
            current_term = l_st.term;
        }
    }

    return leader;
}

ss::future<> raft_fixture::create_simple_group(size_t number_of_nodes) {
    for (size_t id = 0; id < number_of_nodes; ++id) {
        add_node(
          model::node_id(static_cast<int32_t>(id)), model::revision_id{0});
    }

    co_await ss::coroutine::parallel_for_each(_nodes, [this](auto& pair) {
        return pair.second->init_and_start(all_vnodes());
    });
}

ss::future<> raft_fixture::wait_for_committed_offset(
  model::offset offset, std::chrono::milliseconds timeout) {
    RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, [this, offset] {
        return std::all_of(
          nodes().begin(), nodes().end(), [offset](auto& pair) {
              return pair.second->raft()->committed_offset() >= offset;
          });
    });
}
ss::future<> raft_fixture::wait_for_visible_offset(
  model::offset offset, std::chrono::milliseconds timeout) {
    RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, [this, offset] {
        return std::all_of(
          nodes().begin(), nodes().end(), [offset](auto& pair) {
              return pair.second->raft()->last_visible_index() >= offset;
          });
    });
}

void raft_fixture::notify_replicas_on_config_change() const {
    for (const auto& [_, node] : _nodes) {
        node->raft()->notify_config_update();
    }
}

ss::future<> raft_fixture::disable_background_flushing() const {
    co_await ss::smp::invoke_on_all([]() {
        config::shard_local_cfg()
          .raft_replica_max_pending_flush_bytes.set_value(
            std::numeric_limits<size_t>::max());
        config::shard_local_cfg().raft_replica_max_flush_delay_ms.set_value(
          60min);
    });
    notify_replicas_on_config_change();
}

ss::future<> raft_fixture::reset_background_flushing() const {
    co_await ss::smp::invoke_on_all([]() {
        config::shard_local_cfg().raft_replica_max_pending_flush_bytes.reset();
        config::shard_local_cfg().raft_replica_max_flush_delay_ms.reset();
    });
    notify_replicas_on_config_change();
}

ss::future<> raft_fixture::set_write_caching(bool value) const {
    auto mode = value ? model::write_caching_mode::default_true
                      : model::write_caching_mode::default_false;
    co_await ss::smp::invoke_on_all([mode]() {
        config::shard_local_cfg().write_caching_default.set_value(mode);
    });
    notify_replicas_on_config_change();
}

std::ostream& operator<<(std::ostream& o, msg_type type) {
    switch (type) {
    case msg_type::append_entries:
        o << "append_entry";
        return o;
    case msg_type::vote:
        o << "vote";
        return o;
    case msg_type::heartbeat:
        o << "hb";
        return o;
    case msg_type::heartbeat_v2:
        o << "hb_v2";
        return o;
    case msg_type::install_snapshot:
        o << "install_snapshot";
        return o;
    case msg_type::timeout_now:
        o << "timeout_now";
        return o;
    case msg_type::transfer_leadership:
        o << "transfer_leadership";
        return o;
    }
}

} // namespace raft
