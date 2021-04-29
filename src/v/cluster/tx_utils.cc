#include "cluster/logger.h"
#include "cluster/types.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "model/record.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"

namespace cluster {

model::record_batch make_fence_batch(
  model::control_record_version version, model::producer_identity pid) {
    iobuf key;
    kafka::response_writer w(key);
    w.write(version);

    storage::record_batch_builder builder(
      tx_fence_batch_type, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kw(
      std::move(key), std::nullopt, std::vector<model::record_header>());

    return std::move(builder).build();
}

} // namespace cluster
