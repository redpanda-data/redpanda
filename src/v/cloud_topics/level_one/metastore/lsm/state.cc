/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state.h"

#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
std::deque<volatile_row> share_rows(std::deque<volatile_row>& rows) {
    std::deque<volatile_row> copy;
    for (auto& r : rows) {
        copy.push_back(
          volatile_row{
            .seqno = r.seqno,
            .row = write_batch_row{
              .key = r.row.key, .value = r.row.value.share()}});
    }
    return copy;
}
} // namespace

lsm_state::serialized_manifest lsm_state::serialized_manifest::share() {
    return serialized_manifest{
      .buf = buf.share(),
      .last_seqno = last_seqno,
      .database_epoch = database_epoch,
    };
}

lsm_state lsm_state::share() {
    std::optional<serialized_manifest> manifest_copy;
    if (persisted_manifest.has_value()) {
        manifest_copy = persisted_manifest->share();
    }
    return lsm_state{
      .domain_uuid = domain_uuid,
      .seqno_delta = seqno_delta,
      .db_epoch_delta = db_epoch_delta,
      .volatile_buffer = share_rows(volatile_buffer),
      .persisted_manifest = std::move(manifest_copy),
    };
}

model::term_id lsm_state::to_term(lsm::internal::database_epoch e) const {
    return model::term_id(e() - db_epoch_delta);
}

lsm::internal::database_epoch lsm_state::to_epoch(model::term_id t) const {
    return lsm::internal::database_epoch(t() + db_epoch_delta);
}

model::offset lsm_state::to_offset(lsm::sequence_number s) const {
    return model::offset(s() - seqno_delta);
}

lsm::sequence_number lsm_state::to_seqno(model::offset o) const {
    return lsm::sequence_number(o() + seqno_delta);
}

} // namespace cloud_topics::l1
