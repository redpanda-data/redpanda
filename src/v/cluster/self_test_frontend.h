/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "cluster/self_test_rpc_service.h"
#include "cluster/self_test_rpc_types.h"
#include "rpc/connection_cache.h"
#include "rpc/fwd.h"

namespace cluster {

class self_test_exception : public std::runtime_error {
public:
    explicit self_test_exception(ss::sstring msg)
      : std::runtime_error(std::move(msg)) {}
};

/// Main orchestration tool for the redpanda self_test framework. This tool
/// sends requests to all peers that will start or stop individual self_test
/// jobs such as disk, network tests, etc. The status() endpoint can be used
/// to query the status of all self_tests running cluster-wide.
class self_test_frontend {
public:
    static constexpr auto shard = ss::shard_id(0);

    /// Holds reported result of a status() call from a single broker
    struct node_test_state {
        std::optional<get_status_response> response;
        self_test_status status() const;
        self_test_stage stage() const;
    };

    /// Holds reported self_test results aquired from all brokers in a test
    class global_test_state {
    public:
        using underlying_t
          = absl::flat_hash_map<model::node_id, node_test_state>;

        explicit global_test_state(std::vector<underlying_t::value_type>&&);

        /// true if there are no active participants and test has started
        bool finished() const;

        /// returns node_ids of participants that are still running jobs
        std::vector<model::node_id> active_participant_ids() const;

        /// returns const ref to internal data
        const underlying_t& results() const { return _participants; }

    private:
        underlying_t _participants;
    };

    self_test_frontend(
      model::node_id self,
      ss::sharded<cluster::members_table>& members,
      ss::sharded<cluster::self_test_backend>& this_runner,
      ss::sharded<rpc::connection_cache>& connections);

    ss::future<> start();
    ss::future<> stop();

    /// Plan + test parameters
    ///
    /// Launches the indicated self_tests on the specified nodes
    ss::future<uuid_t>
    start_test(start_test_request req, std::vector<model::node_id> ids);

    ss::future<global_test_state> stop_test();
    ss::future<global_test_state> status();

private:
    /// The following abstractions are responsible for invoking an individual
    /// self test on either a remote node.
    class invoke_wrapper {
    public:
        explicit invoke_wrapper(model::node_id node_id)
          : _node_id(node_id) {}

        virtual ~invoke_wrapper() = default;

        virtual ss::future<node_test_state> start_test(start_test_request) = 0;
        virtual ss::future<node_test_state> stop_test() = 0;
        virtual ss::future<node_test_state> get_status() = 0;

        model::node_id node_id() const { return _node_id; }

    private:
        model::node_id _node_id;
    };

    /// Starts/stops/queries status of a single self-test if the node_id is
    /// not 'this' node. Makes RPC call to remote node to achive this.
    class remote_invoke : public invoke_wrapper {
    public:
        remote_invoke(
          model::node_id node_id,
          model::node_id self,
          ss::sharded<rpc::connection_cache>& connections)
          : invoke_wrapper(node_id)
          , _self(self)
          , _connections(connections) {}

        ss::future<node_test_state> start_test(start_test_request) override;
        ss::future<node_test_state> stop_test() override;
        ss::future<node_test_state> get_status() override;

    private:
        template<typename Func>
        ss::future<result<rpc::client_context<get_status_response>>>
        send_self_test_rpc(Func&& f) {
            return _connections.local()
              .with_node_client<self_test_rpc_client_protocol>(
                _self, ss::this_shard_id(), node_id(), 10s, f);
        }

    private:
        model::node_id _self;
        ss::sharded<rpc::connection_cache>& _connections;
    };

    /// Starts/stops/queries status of a single self-test if the node_id is
    /// 'this' node. Manually invokes the self_test_backend to achive this
    class local_invoke : public invoke_wrapper {
    public:
        local_invoke(
          model::node_id node_id,
          ss::sharded<cluster::self_test_backend>& self_be)
          : invoke_wrapper(node_id)
          , _self_be(self_be) {}

        ss::future<node_test_state> start_test(start_test_request) override;
        ss::future<node_test_state> stop_test() override;
        ss::future<node_test_state> get_status() override;

    private:
        ss::sharded<cluster::self_test_backend>& _self_be;
    };

    /// Send RPC (or locally invoke) a self test endpoint to one or more nodes
    ///
    /// @param f Functor that returns a single nodes state and takes a
    ///   unique_ptr reference of an `invoke_wrapper` subclass instance.
    /// @returns The current reported status from all nodes
    template<typename Func>
    ss::future<global_test_state> invoke_on_all_nodes(Func f);

private:
    ss::gate _gate;

    model::node_id _self;

    ss::sharded<cluster::members_table>& _members;
    ss::sharded<cluster::self_test_backend>&
      _self_be; // only invoked to execute the self test on 'this' node, the
                // leader node where this frontend will be active as well
    ss::sharded<rpc::connection_cache>& _connections;
};
} // namespace cluster
