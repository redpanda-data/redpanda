#include "raft/configuration_bootstrap_state.h"

#include "rpc/deserialize.h"

namespace raft {
void configuration_bootstrap_state::process_configuration_in_thread(
  model::record_batch b) {
    _config_batches_seen++;
    if (__builtin_expect(b.type() != configuration_batch_type, false)) {
        throw std::runtime_error(fmt::format(
          "Logic error. Asked a configuration tracker to process an unknown "
          "record_batch_type: {}",
          b.type()));
    }
    if (__builtin_expect(b.compressed(), false)) {
        throw std::runtime_error(
          "Compressed configuration records are unsupported");
    }
    if (_log_config_offset_tracker < b.last_offset()) {
        _log_config_offset_tracker = b.last_offset();
        for (model::record& rec : b) {
            _config = std::move(rpc::deserialize<group_configuration>(
                                  rec.share_packed_value_and_headers())
                                  .get0());
        }
    }
}
void configuration_bootstrap_state::process_data_offsets_in_thread(
  model::record_batch b) {
    _data_batches_seen++;
    if (__builtin_expect(b.type() == configuration_batch_type, false)) {
        throw std::runtime_error(fmt::format(
          "Logic error. Asked a data tracker to process "
          "configuration_batch_type "
          "record_batch_type: {}",
          b.type()));
    }
    // happy path
    if (b.last_offset() > _commit_index) {
        _prev_log_index = _commit_index;
        _prev_log_term = _term;
        _commit_index = b.last_offset();
        _commit_index_base_batch_offset = b.base_offset();
        return;
    }
    // we need to test how to find prev term
    if (
      b.base_offset() < _commit_index_base_batch_offset
      && b.last_offset() >= _commit_index_base_batch_offset) {
        _prev_log_index = _commit_index;
        _prev_log_term = _term;
        return;
    }
    // ignore the rest of the records, only interested in the last 2
}

void configuration_bootstrap_state::process_batch_in_thread(
  model::record_batch b) {
    switch (b.type()) {
    case configuration_batch_type:
        process_configuration_in_thread(std::move(b));
        break;
    default:
        process_data_offsets_in_thread(std::move(b));
        break;
    }
}
} // namespace raft
