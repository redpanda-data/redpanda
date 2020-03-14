#pragma once

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "kafka/controller_dispatcher.h"
#include "kafka/groups/group_router.h"
#include "kafka/quota_manager.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "rpc/server.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <optional>
#include <vector>

namespace cluster {
class metadata_cache;
}

namespace kafka {
struct request_header;
class controller_dispatcher;
} // namespace kafka

namespace kafka {

class protocol final : public rpc::server::protocol {
public:
    using sequence_id = named_type<uint64_t, struct kafka_protocol_sequence>;

    protocol(
      ss::smp_service_group,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<controller_dispatcher>&,
      ss::sharded<quota_manager>&,
      ss::sharded<kafka::group_router_type>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coordinator_ntp_mapper>& coordinator_mapper) noexcept;

    ~protocol() noexcept override = default;
    protocol(const protocol&) = delete;
    protocol& operator=(const protocol&&) = delete;
    protocol(protocol&&) noexcept = default;
    protocol& operator=(protocol&&) noexcept = delete;

    const char* name() const final { return "kafka rpc protocol"; }
    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(rpc::server::resources) final;

private:
    using map_t = absl::flat_hash_map<sequence_id, ss::scattered_message<char>>;

    class connection_context final
      : public ss::enable_lw_shared_from_this<connection_context> {
    public:
        connection_context(protocol& p, rpc::server::resources&& r) noexcept
          : _proto(p)
          , _rs(std::move(r)) {}
        ~connection_context() noexcept = default;
        connection_context(const connection_context&) = delete;
        connection_context(connection_context&&) = delete;
        connection_context& operator=(const connection_context&) = delete;
        connection_context& operator=(connection_context&&) = delete;

        ss::future<> process_one_request();
        bool is_finished_parsing() const;

    private:
        ss::future<> dispatch_method_once(request_header, size_t sz);
        ss::future<> process_next_response();
        ss::future<> do_process(request_context);

    private:
        protocol& _proto;
        rpc::server::resources _rs;
        sequence_id _next_response;
        sequence_id _seq_idx;
        map_t _responses;
    };
    friend connection_context;

private:
    ss::smp_service_group _smp_group;

    // services needed by kafka proto
    ss::sharded<controller_dispatcher>& _cntrl_dispatcher;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::sharded<kafka::group_router_type>& _group_router;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
};

} // namespace kafka
