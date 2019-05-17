#pragma once
#include <smf/log.h>

#include "chain_replication/chain_replication_service.h"
#include "filesystem/write_ahead_log.h"
#include "redpanda.smf.fb.h"

// redpanda
#include "redpanda_cfg.h"

class redpanda_service : public redpanda_api {
 public:
  explicit redpanda_service(const redpanda_cfg *_cfg,
                            seastar::distributed<write_ahead_log> *w)
    : cfg(THROW_IFNULL(_cfg)), wal_(THROW_IFNULL(w)),
      cr_(std::make_unique<chains::chain_replication_service>(w)) {}

  virtual seastar::future<smf::rpc_typed_envelope<wal_topic_create_reply>>
  create_topic(smf::rpc_recv_typed_context<wal_topic_create_request> &&) final;

  inline virtual seastar::future<
    smf::rpc_typed_envelope<chains::chain_put_reply>>
  put(smf::rpc_recv_typed_context<chains::chain_put_request> &&r) final {
    return cr_->put(std::move(r));
  }

  inline virtual seastar::future<
    smf::rpc_typed_envelope<chains::chain_get_reply>>
  get(smf::rpc_recv_typed_context<chains::chain_get_request> &&r) final {
    // return cr_->get(std::move(r));
    LOG_THROW("Should have called raw_get() instead");
  }

  inline virtual seastar::future<smf::rpc_envelope>
  raw_get(smf::rpc_recv_context &&c) final {
    return cr_->raw_get(std::move(c));
  }

  const redpanda_cfg *cfg;

 private:
  seastar::distributed<write_ahead_log> *wal_;
  std::unique_ptr<chains::chain_replication_service> cr_;
};

