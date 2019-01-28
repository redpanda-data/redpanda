#include "raft_service.h"

#include <smf/log.h>

namespace rp {

raft_service::raft_service(raft::serverT opts,
                           seastar::distributed<rp::write_ahead_log> *w)
  : cfg_(std::move(opts)), wal_(THROW_IFNULL(w)) {}

}  // namespace rp
