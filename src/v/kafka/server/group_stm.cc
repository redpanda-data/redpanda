#include "kafka/server/group_stm.h"

#include "cluster/logger.h"
#include "kafka/server/group_metadata.h"
#include "kafka/types.h"
#include "model/record.h"

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

void group_stm::update_tx_offset(
  model::offset offset, group_tx::offsets_metadata offset_md) {
    auto it = _ongoing_txs.find(offset_md.pid);
    // if an ongoing transaction doesn't exists we ignore the update
    if (it == _ongoing_txs.end()) {
        vlog(
          cluster::txlog.warn,
          "ongoing transaction for producer {} not found, skipping offsets "
          "update",
          offset_md.pid);
        return;
    }

    const auto now = model::timestamp::now();
    for (const auto& tx_offset : offset_md.offsets) {
        group::offset_metadata md{
          .log_offset = offset,
          .offset = tx_offset.offset,
          .metadata = tx_offset.metadata.value_or(""),
          .committed_leader_epoch = kafka::leader_epoch(tx_offset.leader_epoch),
          .commit_timestamp = now,
          .expiry_timestamp = std::nullopt,
        };
        it->second.offsets[tx_offset.tp] = md;
    }
}

void group_stm::commit(model::producer_identity pid) {
    auto prepared_it = _ongoing_txs.find(pid);
    if (prepared_it == _ongoing_txs.end()) {
        // missing prepare may happen when the consumer log gets truncated
        vlog(
          cluster::txlog.warn,
          "unable to find ongoing transaction for producer: {}, skipping "
          "commit",
          pid);
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

    _ongoing_txs.erase(prepared_it);
}

void group_stm::abort(
  model::producer_identity pid, [[maybe_unused]] model::tx_seq tx_seq) {
    _ongoing_txs.erase(pid);
}

void group_stm::try_set_fence(
  model::producer_id id, model::producer_epoch epoch) {
    auto [fence_it, _] = _fence_pid_epoch.try_emplace(id, epoch);
    if (fence_it->second < epoch) {
        fence_it->second = epoch;
    }
}

void group_stm::try_set_fence(
  model::producer_id id,
  model::producer_epoch epoch,
  model::tx_seq txseq,
  model::timeout_clock::duration transaction_timeout_ms,
  model::partition_id tm_partition) {
    auto [fence_it, _] = _fence_pid_epoch.try_emplace(id, epoch);
    if (fence_it->second <= epoch) {
        fence_it->second = epoch;
        _ongoing_txs.try_emplace(
          model::producer_identity(id, epoch),
          ongoing_tx{
            .tx_seq = txseq,
            .tm_partition = tm_partition,
            .timeout = transaction_timeout_ms,
            .offsets = {},
          });
    }
}

} // namespace kafka
