#include "kafka/server/group_stm.h"

#include "cluster/logger.h"
#include "kafka/types.h"

namespace kafka {

void group_stm::overwrite_metadata(group_metadata_value&& metadata) {
    _metadata = std::move(metadata);
    _is_loaded = true;
}

void group_stm::remove_offset(const model::topic_partition& key) {
    _offsets.erase(key);
}

void group_stm::update_offset(
  const model::topic_partition& key,
  model::offset offset,
  offset_metadata_value&& meta) {
    _offsets[key] = logged_metadata{
      .log_offset = offset, .metadata = std::move(meta)};
}

void group_stm::update_prepared(
  model::offset offset, group_log_prepared_tx val) {
    auto tx = group::prepared_tx{.pid = val.pid, .tx_seq = val.tx_seq};

    auto [prepared_it, inserted] = _prepared_txs.try_emplace(
      tx.pid.get_id(), tx);
    if (!inserted && prepared_it->second.pid.epoch > tx.pid.epoch) {
        vlog(
          cluster::txlog.warn,
          "a logged tx {} is fenced off by prev logged tx {}",
          val.pid,
          prepared_it->second.pid);
        return;
    } else if (!inserted && prepared_it->second.pid.epoch < tx.pid.epoch) {
        vlog(
          cluster::txlog.warn,
          "a logged tx {} overwrites prev logged tx {}",
          val.pid,
          prepared_it->second.pid);
        prepared_it->second.pid = tx.pid;
        prepared_it->second.offsets.clear();
    }

    const auto now = model::timestamp::now();
    for (const auto& tx_offset : val.offsets) {
        group::offset_metadata md{
          .log_offset = offset,
          .offset = tx_offset.offset,
          .metadata = tx_offset.metadata.value_or(""),
          .committed_leader_epoch = kafka::leader_epoch(tx_offset.leader_epoch),
          .commit_timestamp = now,
          .expiry_timestamp = std::nullopt,
        };
        prepared_it->second.offsets[tx_offset.tp] = md;
    }
}

void group_stm::commit(model::producer_identity pid) {
    auto prepared_it = _prepared_txs.find(pid.get_id());
    if (prepared_it == _prepared_txs.end()) {
        // missing prepare may happen when the consumer log gets truncated
        vlog(cluster::txlog.trace, "can't find ongoing tx {}", pid);
        return;
    } else if (prepared_it->second.pid.epoch != pid.epoch) {
        vlog(
          cluster::txlog.warn,
          "a comitting tx {} doesn't match ongoing tx {}",
          pid,
          prepared_it->second.pid);
        return;
    }

    for (const auto& [tp, md] : prepared_it->second.offsets) {
        offset_metadata_value val{
          .offset = md.offset,
          .leader_epoch
          = kafka::invalid_leader_epoch, // we never use leader_epoch down the
                                         // stack
          .metadata = md.metadata,
          .commit_timestamp = md.commit_timestamp,
          .expiry_timestamp = md.expiry_timestamp.value_or(
            model::timestamp(-1)),
        };

        _offsets[tp] = logged_metadata{
          .log_offset = md.log_offset, .metadata = std::move(val)};
    }

    _prepared_txs.erase(prepared_it);
    _tx_data.erase(pid);
    _timeouts.erase(pid);
}

void group_stm::abort(
  model::producer_identity pid, [[maybe_unused]] model::tx_seq tx_seq) {
    auto prepared_it = _prepared_txs.find(pid.get_id());
    if (prepared_it == _prepared_txs.end()) { // NOLINT(bugprone-branch-clone)
        return;
    } else if (prepared_it->second.pid.epoch != pid.epoch) {
        return;
    }
    _prepared_txs.erase(prepared_it);
    _tx_data.erase(pid);
    _timeouts.erase(pid);
}

} // namespace kafka
