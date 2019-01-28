#pragma once
#include <smf/log.h>

#include "chain_replication.smf.fb.h"
#include "filesystem/write_ahead_log.h"

namespace rp {
namespace chains {
class chain_replication_service : public chain_replication {
 public:
  explicit chain_replication_service(
    seastar::distributed<rp::write_ahead_log> *w)
    : wal_(THROW_IFNULL(w)) {}

  virtual seastar::future<smf::rpc_typed_envelope<chain_put_reply>>
  put(smf::rpc_recv_typed_context<chain_put_request> &&) final;

  virtual seastar::future<smf::rpc_typed_envelope<chain_get_reply>>
  get(smf::rpc_recv_typed_context<chain_get_request> &&) final;

  // override smf-serialization w/ custom one
  virtual seastar::future<smf::rpc_envelope>
  raw_get(smf::rpc_recv_context &&c) final;

 private:
  /// \brief same as get but with added semaphore so we do not OOM
  seastar::future<smf::rpc_typed_envelope<chain_get_reply>>
  do_get(smf::rpc_recv_typed_context<chain_get_request> &&);

 private:
  seastar::distributed<rp::write_ahead_log> *wal_;
};

}  // end namespace chains
}  // end namespace rp
