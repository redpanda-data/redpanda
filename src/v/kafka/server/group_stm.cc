#include "kafka/server/group_stm.h"

#include "cluster/logger.h"

namespace kafka {

void group_stm::overwrite_metadata(group_log_group_metadata&& metadata) {
    _metadata = std::move(metadata);
    _is_loaded = true;
}

void group_stm::remove_offset(const model::topic_partition& key) {
    _offsets.erase(key);
}

void group_stm::update_offset(
  const model::topic_partition& key,
  model::offset offset,
  group_log_offset_metadata&& meta) {
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

    for (const auto& tx_offset : val.offsets) {
        group::offset_metadata md{
          .log_offset = offset,
          .offset = tx_offset.offset,
          .metadata = tx_offset.metadata.value_or(""),
        };
        // BUG: support leader_epoch (KIP-320)
        // https://github.com/vectorizedio/redpanda/issues/1181
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
        group_log_offset_metadata val{
          .offset = md.offset,
          .leader_epoch = 0, // we never use leader_epoch down the stack
          .metadata = md.metadata};

        _offsets[tp] = logged_metadata{
          .log_offset = md.log_offset, .metadata = std::move(val)};
    }

    _prepared_txs.erase(prepared_it);
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
}

std::ostream& operator<<(std::ostream& os, const group_log_offset_key& key) {
    fmt::print(
      os,
      "group {} topic {} partition {}",
      key.group(),
      key.topic(),
      key.partition());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const group_log_offset_metadata& md) {
    fmt::print(os, "offset {}", md.offset());
    return os;
}

} // namespace kafka
