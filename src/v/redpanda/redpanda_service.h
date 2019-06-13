#pragma once
#include "chain_replication/chain_replication_service.h"
#include "filesystem/write_ahead_log.h"
#include "redpanda/redpanda.smf.fb.h"
#include "redpanda/redpanda_cfg.h"

#include <smf/log.h>

class redpanda_service : public redpanda_api {
public:
    explicit redpanda_service(
      const redpanda_cfg* _cfg, seastar::distributed<write_ahead_log>* w)
      : cfg(THROW_IFNULL(_cfg))
      , _wal(THROW_IFNULL(w))
      , _cr(std::make_unique<chains::chain_replication_service>(w)) {
    }

    virtual seastar::future<smf::rpc_typed_envelope<wal_topic_create_reply>>
    create_topic(smf::rpc_recv_typed_context<wal_topic_create_request>&&) final;

    inline virtual seastar::future<
      smf::rpc_typed_envelope<chains::chain_put_reply>>
    put(smf::rpc_recv_typed_context<chains::chain_put_request>&& r) final {
        return _cr->put(std::move(r));
    }

    inline virtual seastar::future<
      smf::rpc_typed_envelope<chains::chain_get_reply>>
    get(smf::rpc_recv_typed_context<chains::chain_get_request>&& r) final {
        // return _cr->get(std::move(r));
        LOG_THROW("Should have called raw_get() instead");
    }

    inline virtual seastar::future<smf::rpc_envelope>
    raw_get(smf::rpc_recv_context&& c) final {
        return _cr->raw_get(std::move(c));
    }

    const redpanda_cfg* cfg;

private:
    seastar::distributed<write_ahead_log>* _wal;
    std::unique_ptr<chains::chain_replication_service> _cr;
};
