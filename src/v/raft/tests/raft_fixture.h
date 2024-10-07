
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

#pragma once
#include "bytes/random.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus_client_protocol.h"
#include "raft/coordinated_recovery_throttle.h"
#include "raft/fwd.h"
#include "raft/heartbeat_manager.h"
#include "raft/recovery_memory_quota.h"
#include "raft/state_machine_manager.h"
#include "raft/types.h"
#include "ssx/sformat.h"
#include "storage/api.h"
#include "test_utils/test.h"
#include "utils/prefix_logger.h"

#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/node_hash_map.h>
#include <boost/range/irange.hpp>

#include <ranges>
namespace raft {

inline constexpr raft::group_id test_group(123);

enum class msg_type {
    append_entries,
    vote,
    heartbeat,
    heartbeat_v2,
    install_snapshot,
    timeout_now,
    transfer_leadership,
};

struct msg {
    msg_type type;
    iobuf req_data;
    ss::promise<iobuf> resp_data;
};
class raft_node_instance;

struct channel {
    explicit channel(raft_node_instance&);
    void start();
    ss::future<> stop();

    ss::future<iobuf> exchange(msg_type type, iobuf request);

    void start_dispatch_loop();
    ss::future<> dispatch_loop();
    bool is_valid() const;

private:
    ss::lw_shared_ptr<consensus> raft();
    ss::weak_ptr<raft_node_instance> _node;
    ss::chunked_fifo<msg> _messages;
    ss::gate _gate;
    ss::condition_variable _new_messages;
    ss::abort_source _as;
};

struct raft_node_map {
    virtual ~raft_node_map() = default;
    virtual std::optional<std::reference_wrapper<raft_node_instance>>
      node_for(model::node_id) = 0;
};

using dispatch_callback_t
  = ss::noncopyable_function<ss::future<>(model::node_id, msg_type)>;

class in_memory_test_protocol : public consensus_client_protocol::impl {
public:
    explicit in_memory_test_protocol(raft_node_map&, prefix_logger&);

    ss::future<result<vote_reply>>
    vote(model::node_id, vote_request&&, rpc::client_opts) final;

    ss::future<result<append_entries_reply>> append_entries(
      model::node_id,
      append_entries_request&&,
      rpc::client_opts,
      bool use_all_serde_encoding) final;

    ss::future<result<heartbeat_reply>>
    heartbeat(model::node_id, heartbeat_request&&, rpc::client_opts) final;

    ss::future<result<heartbeat_reply_v2>> heartbeat_v2(
      model::node_id, heartbeat_request_v2&&, rpc::client_opts) final;

    ss::future<result<install_snapshot_reply>> install_snapshot(
      model::node_id, install_snapshot_request&&, rpc::client_opts) final;

    ss::future<result<timeout_now_reply>>
    timeout_now(model::node_id, timeout_now_request&&, rpc::client_opts) final;

    ss::future<result<transfer_leadership_reply>> transfer_leadership(
      model::node_id, transfer_leadership_request&&, rpc::client_opts) final;

    // TODO: move those methods out of Raft protocol.
    ss::future<> reset_backoff(model::node_id) final { co_return; }

    ss::future<bool> ensure_disconnect(model::node_id) final {
        co_return true;
    };

    channel& get_channel(model::node_id id);

    void on_dispatch(dispatch_callback_t f);

    ss::future<> stop();

private:
    template<typename ReqT, typename RespT>
    ss::future<result<RespT>> dispatch(model::node_id, ReqT req);
    ss::gate _gate;
    absl::flat_hash_map<model::node_id, std::unique_ptr<channel>> _channels;
    std::vector<dispatch_callback_t> _on_dispatch_handlers;
    raft_node_map& _nodes;
    prefix_logger& _logger;
};

inline model::timeout_clock::time_point default_timeout() {
    return model::timeout_clock::now() + 30s;
}

/**
 * Node hosting Raft replica
 */
class raft_node_instance : public ss::weakly_referencable<raft_node_instance> {
public:
    using leader_update_clb_t
      = ss::noncopyable_function<void(leadership_status)>;
    raft_node_instance(
      model::node_id id,
      model::revision_id revision,
      ss::sstring base_directory,
      raft_node_map& node_map,
      ss::sharded<features::feature_table>& feature_table,
      leader_update_clb_t leader_update_clb,
      bool enable_longest_log_detection,
      config::binding<std::chrono::milliseconds> election_timeout,
      config::binding<std::chrono::milliseconds> heartbeat_interval);

    raft_node_instance(
      model::node_id id,
      model::revision_id revision,
      raft_node_map& node_map,
      ss::sharded<features::feature_table>& feature_table,
      leader_update_clb_t leader_update_clb,
      bool enable_longest_log_detection,
      config::binding<std::chrono::milliseconds> election_timeout,
      config::binding<std::chrono::milliseconds> heartbeat_interval);

    raft_node_instance(const raft_node_instance&) = delete;
    raft_node_instance(raft_node_instance&&) noexcept = delete;
    raft_node_instance& operator=(raft_node_instance&&) = delete;
    raft_node_instance& operator=(const raft_node_instance&) = delete;
    ~raft_node_instance() = default;

    ss::sstring base_directory() { return _base_directory; }

    ss::lw_shared_ptr<consensus> raft() { return _raft; }

    ss::sharded<features::feature_table>& get_feature_table() {
        return _features;
    }

    // Initialise the node instance and create the consensus instance
    ss::future<> initialise(std::vector<raft::vnode> initial_nodes);

    // Start the node instance with an optionally provided state machine builder
    ss::future<>
    start(std::optional<raft::state_machine_manager_builder> builder);

    // Initialise and start the node instance
    ss::future<> init_and_start(
      std::vector<raft::vnode> initial_nodes,
      std::optional<raft::state_machine_manager_builder> builder
      = std::nullopt);

    ss::future<> stop();

    ss::future<> remove_data();

    void leadership_notification_callback(leadership_status);

    model::ntp ntp() {
        return {
          model::kafka_namespace,
          model::topic_partition(
            model::topic(fmt::format("node_{}", _id)), model::partition_id(0))};
    }

    vnode get_vnode() const { return {_id, _revision}; }

    ss::future<ss::circular_buffer<model::record_batch>>
    read_all_data_batches();
    ss::future<ss::circular_buffer<model::record_batch>>
    read_batches_in_range(model::offset min, model::offset max);

    ss::future<model::offset> random_batch_base_offset(
      model::offset max, std::optional<model::offset> min = std::nullopt);

    /// \brief Sets a callback function to be invoked when the leader dispatches
    /// a message to followers.
    ///
    /// It is invoked once for each follower. The dispatch process will proceed
    /// once the returned future from the callback function is resolved.
    ///
    /// This method is handy for failure injection.
    ///
    //// \param f The callback function to be invoked when a message is
    /// dispatched.
    void on_dispatch(dispatch_callback_t);

    ss::shared_ptr<in_memory_test_protocol> get_protocol() { return _protocol; }

    storage::kvstore& get_kvstore() { return _storage.local().kvs(); }

private:
    model::node_id _id;
    model::revision_id _revision;
    prefix_logger _logger;
    ss::sstring _base_directory;
    ss::shared_ptr<in_memory_test_protocol> _protocol;
    ss::sharded<storage::api> _storage;
    ss::sharded<features::feature_table>& _features;
    ss::sharded<coordinated_recovery_throttle> _recovery_throttle;
    recovery_memory_quota _recovery_mem_quota;
    recovery_scheduler _recovery_scheduler;
    std::unique_ptr<heartbeat_manager> _hb_manager;
    leader_update_clb_t _leader_clb;
    ss::lw_shared_ptr<consensus> _raft;
    bool started = false;
    bool _enable_longest_log_detection;
    config::binding<std::chrono::milliseconds> _election_timeout;
    config::binding<std::chrono::milliseconds> _heartbeat_interval;
};

class raft_fixture
  : public seastar_test
  , public raft_node_map {
public:
    using leader_update_clb_t
      = ss::noncopyable_function<void(model::node_id, leadership_status)>;
    raft_fixture()
      : _logger("raft-fixture") {}
    using raft_nodes_t = absl::
      flat_hash_map<model::node_id, std::unique_ptr<raft_node_instance>>;
    static constexpr raft::group_id group_id = raft::group_id(123);

    std::optional<std::reference_wrapper<raft_node_instance>>
    node_for(model::node_id id) final {
        auto it = _nodes.find(id);
        if (it == _nodes.end()) {
            return std::nullopt;
        }
        return *it->second;
    };
    using remove_data_dir = ss::bool_class<struct remove_data_dir_tag>;
    raft_node_instance& add_node(model::node_id id, model::revision_id rev);

    raft_node_instance&
    add_node(model::node_id id, model::revision_id rev, ss::sstring data_dir);

    ss::future<>
    stop_node(model::node_id id, remove_data_dir remove = remove_data_dir::no);

    raft_node_instance& node(model::node_id);

    std::optional<model::node_id> get_leader() const;

    ss::future<model::node_id> wait_for_leader(std::chrono::milliseconds);
    ss::future<model::node_id>
      wait_for_leader(model::timeout_clock::time_point);
    ss::future<model::node_id> wait_for_leader_change(
      model::timeout_clock::time_point deadline, model::term_id term);
    seastar::future<> TearDownAsync() override;
    seastar::future<> SetUpAsync() override;

    raft_nodes_t& nodes() { return _nodes; };

    absl::flat_hash_set<model::node_id> all_ids() const {
        absl::flat_hash_set<model::node_id> all_ids;
        for (const auto& [id, _] : _nodes) {
            all_ids.emplace(id);
        }
        return all_ids;
    }

    std::vector<vnode> all_vnodes() {
        std::vector<vnode> vnodes;
        vnodes.reserve(_nodes.size());
        for (const auto& [_, n] : _nodes) {
            vnodes.push_back(n->get_vnode());
        }
        return vnodes;
    }

    ss::future<> create_simple_group(size_t number_of_nodes);

    model::record_batch_reader
    make_batches(std::vector<std::pair<ss::sstring, ss::sstring>> batch_spec) {
        const auto sz = batch_spec.size();
        return make_batches(sz, [spec = std::move(batch_spec)](size_t idx) {
            auto [k, v] = spec[idx];
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, model::offset(0));
            builder.add_raw_kv(
              serde::to_iobuf(std::move(k)), serde::to_iobuf(std::move(v)));
            return std::move(builder).build();
        });
    }

    template<typename Generator>
    model::record_batch_reader
    make_batches(size_t batch_count, Generator&& generator) {
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(batch_count);
        for (auto b_idx : boost::irange(batch_count)) {
            batches.push_back(generator(b_idx));
        }

        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch_reader make_batches(
      size_t batch_count,
      size_t batch_record_count,
      size_t record_payload_size) {
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(batch_count);
        for (auto b_idx : boost::irange(batch_count)) {
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, model::offset(0));
            for (int r_idx : boost::irange(batch_record_count)) {
                builder.add_raw_kv(
                  serde::to_iobuf(ssx::sformat("r-{}-{}", b_idx, r_idx)),
                  serde::to_iobuf(
                    random_generators::get_bytes(record_payload_size)));
            }
            batches.push_back(std::move(builder).build());
        }

        return model::make_memory_record_batch_reader(std::move(batches));
    }

    ss::future<>
    assert_logs_equal(model::offset start_offset = model::offset{}) {
        std::vector<ss::circular_buffer<model::record_batch>> node_batches;
        for (auto& [id, n] : _nodes) {
            auto read_from = start_offset == model::offset{}
                               ? n->raft()->start_offset()
                               : start_offset;
            node_batches.push_back(co_await n->read_batches_in_range(
              read_from, model::offset::max()));
        }
        ASSERT_TRUE_CORO(std::all_of(
          node_batches.begin(),
          node_batches.end(),
          [&reference = node_batches.front()](const auto& batches) {
              return batches.size() == reference.size()
                     && std::equal(
                       batches.begin(), batches.end(), reference.begin());
          }));
    }

    ss::future<> wait_for_committed_offset(
      model::offset offset, std::chrono::milliseconds timeout);

    ss::future<> wait_for_visible_offset(
      model::offset offset, std::chrono::milliseconds timeout);

    template<typename Func>
    auto with_leader(std::chrono::milliseconds timeout, Func&& f) {
        return wait_for_leader(timeout).then(
          [this, f = std::forward<Func>(f)](model::node_id leader_id) mutable {
              return f(node(leader_id));
          });
    }

    template<typename Func>
    auto parallel_for_each_node(Func&& f) {
        return ss::parallel_for_each(
          _nodes,
          [f = std::forward<Func>(f)](auto& pair) { return f(*pair.second); });
    }

    template<typename E>
    struct retry_policy {
        static E timeout_error() { return E::timeout; }
        static bool should_retry(const E& err) {
            return err == E::timeout || err == E::not_leader;
        }
    };

    template<>
    struct retry_policy<bool> {
        static bool timeout_error() { return false; }
        static bool should_retry(const bool& err) { return !err; }
    };

    template<typename Func>
    auto retry_with_leader(
      model::timeout_clock::time_point deadline,
      std::chrono::milliseconds backoff,
      Func&& f) {
        using futurator
          = ss::futurize<std::invoke_result_t<Func, raft_node_instance&>>;
        using ret_t = futurator::value_type;
        // some functions return bare error code, we'll cast to result anyway
        using result_t = std::
          conditional_t<is_result_v<ret_t>, const ret_t&, result<void, ret_t>>;
        using error_t = std::remove_cvref_t<result_t>::error_type;
        using policy = retry_policy<error_t>;

        struct retry_state {
            ret_t result = policy::timeout_error();
            int retry = 0;

            auto result_with_value() { return static_cast<result_t>(result); }

            // assume, as result_t may be e.g. result<void, bool>
            error_t get_error() { return result_with_value().assume_error(); }

            bool ready() {
                if (!result_with_value().has_error()) {
                    return true;
                }
                return !policy::should_retry(get_error());
            }
        };
        return ss::do_with(
          retry_state{},
          std::forward<Func>(f),
          [this, deadline, backoff](
            retry_state& state, std::remove_reference_t<Func>& f) {
              return ss::do_until(
                       [&state, deadline] {
                           return model::timeout_clock::now() > deadline
                                  || state.ready();
                       },
                       [this, &state, &f, deadline, backoff]() {
                           vlog(
                             _logger.info,
                             "Executing action with leader, current retry: "
                             "{}",
                             state.retry);

                           return wait_for_leader(deadline).then(
                             [this, &f, &state, backoff](
                               model::node_id leader_id) mutable {
                                 return ss::futurize_invoke(f, node(leader_id))
                                   .then([this, &state, backoff](ret_t result) {
                                       state.result = std::move(result);
                                       // "success"
                                       if (state.ready()) {
                                           return ss::now();
                                       }
                                       vlog(
                                         _logger.info,
                                         "Leader action returned an error: "
                                         "{}",
                                         state.get_error());
                                       state.result = policy::timeout_error();
                                       state.retry++;

                                       return ss::sleep(backoff);
                                   });
                             });
                       })
                .then([&state] { return state.result; });
          });
    }
    template<typename Func>
    auto
    retry_with_leader(model::timeout_clock::time_point deadline, Func&& f) {
        return retry_with_leader(deadline, 100ms, std::forward<Func>(f));
    }

    ss::logger& logger() { return _logger; }

    void notify_replicas_on_config_change() const;
    ss::future<> disable_background_flushing() const;
    ss::future<> reset_background_flushing() const;
    ss::future<> set_write_caching(bool) const;

    void set_enable_longest_log_detection(bool value) {
        _enable_longest_log_detection = value;
    }

    void register_leader_callback(leader_update_clb_t clb) {
        _leader_clb = std::move(clb);
    }

    void set_election_timeout(std::chrono::milliseconds timeout) {
        _election_timeout.update(std::move(timeout));
    }
    void set_heartbeat_interval(std::chrono::milliseconds timeout) {
        _heartbeat_interval.update(std::move(timeout));
    }

private:
    void validate_leaders();

    raft_nodes_t _nodes;
    ss::logger _logger;

    absl::flat_hash_map<model::node_id, leadership_status> _leaders_view;

    ss::sharded<features::feature_table> _features;
    bool _enable_longest_log_detection = true;
    std::optional<leader_update_clb_t> _leader_clb;
    config::mock_property<std::chrono::milliseconds> _election_timeout{500ms};
    config::mock_property<std::chrono::milliseconds> _heartbeat_interval{50ms};
};

template<class... STM>
struct stm_raft_fixture : raft_fixture {
    using stm_shptrs_t = std::tuple<ss::shared_ptr<STM>...>;

    ss::future<> initialize_state_machines() {
        return initialize_state_machines(3);
    }

    ss::future<> initialize_state_machines(int node_cnt) {
        for (auto i = 0; i < node_cnt; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
        co_await start_nodes();
    }

    ss::future<> start_node(raft_node_instance& node) {
        co_await node.initialise(all_vnodes());
        raft::state_machine_manager_builder builder;
        stm_shptrs_t stm_shptrs = create_stms(builder, node);
        co_await node.start(std::move(builder));
        node_stms.emplace(node.get_vnode(), std::move(stm_shptrs));
    }

    ss::future<> start_nodes() {
        co_await parallel_for_each_node(
          [this](raft_node_instance& node) { return start_node(node); });
    }

    ss::future<> stop_and_recreate_nodes() {
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
            node_stms.erase(node->get_vnode());
        }

        co_await ss::parallel_for_each(
          std::views::keys(data_directories),
          [this](model::node_id id) { return stop_node(id); });

        for (auto& [id, data_dir] : data_directories) {
            add_node(id, model::revision_id(0), std::move(data_dir));
        }
    }

    ss::future<> restart_nodes() {
        co_await stop_and_recreate_nodes();
        co_await start_nodes();
    }

    // returns ss::shared_ptr<stm type>
    template<int stm_id>
    auto get_stm(raft_node_instance& node) {
        return std::get<stm_id>(node_stms[node.get_vnode()]);
    }

    template<int stm_id, typename Func>
    auto stm_retry_with_leader(std::chrono::milliseconds timeout, Func&& f) {
        return retry_with_leader(
          model::timeout_clock::now() + timeout,
          [this,
           f = std::forward<Func>(f)](raft_node_instance& leader_node) mutable {
              auto stm = get_stm<stm_id>(leader_node);
              return f(stm);
          });
    }

    absl::flat_hash_map<raft::vnode, stm_shptrs_t> node_stms;

    virtual stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node)
      = 0;
};

std::ostream& operator<<(std::ostream& o, msg_type type);
} // namespace raft
