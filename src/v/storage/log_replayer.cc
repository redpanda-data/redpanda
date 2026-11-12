// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_replayer.h"

#include "base/vlog.h"
#include "hashing/crc32c.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "storage/segment.h"

namespace storage {
class checksumming_consumer final : public batch_consumer {
public:
    checksumming_consumer(
      segment* s,
      log_replayer::checkpoint& c,
      const config_batch_term_parser& term_parser)
      : _seg(s)
      , _cfg(c)
      , _term_parser(term_parser) {
        // we'll reconstruct the state manually
        _seg->index().reset();
    }
    checksumming_consumer(const checksumming_consumer&) = delete;
    checksumming_consumer& operator=(const checksumming_consumer&) = delete;
    checksumming_consumer(checksumming_consumer&&) noexcept = delete;
    checksumming_consumer& operator=(checksumming_consumer&&) noexcept = delete;
    ~checksumming_consumer() noexcept override = default;

    consume_result
    accept_batch_start(const model::record_batch_header&) const final {
        return batch_consumer::consume_result::accept_batch;
    }
    void skip_batch_start(model::record_batch_header, size_t, size_t) override {
    }

    void consume_batch_start(
      model::record_batch_header header,
      size_t physical_base_offset,
      size_t size_on_disk) override {
        _header = header;
        _file_pos_to_end_of_batch = size_on_disk + physical_base_offset;
        _crc = crc::crc32c();
        model::crc_record_batch_header(_crc, header);
    }

    void consume_records(iobuf&& records) override {
        crc_extend_iobuf(_crc, records);
        if (
          _term_parser
          && _header.type == model::record_batch_type::raft_configuration) {
            _records = std::move(records);
        }
    }

    ss::future<stop_parser> consume_batch_end() override {
        if (is_valid_batch_crc()) {
            _cfg.last_offset = _header.last_offset();
            _cfg.truncate_file_pos = _file_pos_to_end_of_batch;
            _cfg.last_max_timestamp = std::max(
              _header.first_timestamp, _header.max_timestamp);
            const auto physical_base_offset = _file_pos_to_end_of_batch
                                              - _header.size_bytes;
            // new_broker_ts is nullopt, in the happy case the index was
            // recovered and it's not use to set it to now, in the bad case we
            // have no good way to recover it so just fallback to max_timestamp
            _seg->index().maybe_track(
              _header, std::nullopt, physical_base_offset);
            maybe_record_term_transition();
            _header = {};
            co_return stop_parser::no;
        }
        co_return stop_parser::yes;
    }

    /// Term transitions are recoverable from log data alone: every term
    /// begins with a raft configuration batch, and configurations >= v_8
    /// carry the replication term in their payload.
    void maybe_record_term_transition() {
        if (
          !_term_parser
          || _header.type != model::record_batch_type::raft_configuration) {
            return;
        }
        auto batch = model::record_batch(
          _header, std::move(_records), model::record_batch::tag_ctor_ng{});
        auto term = _term_parser(batch);
        if (term.has_value()) {
            _cfg.term_transitions.emplace_back(
              *term, batch.header().base_offset);
        }
    }

    bool is_valid_batch_crc() const {
        // crc is calculated as a uint32_t but because of kafka we carry around
        // a signed type in the batch structure
        return (uint32_t)_header.crc == _crc.value();
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(
          it, "storage::checksumming_consumer segment {}", *_seg);
    }

private:
    model::record_batch_header _header;
    iobuf _records;
    segment* _seg;
    log_replayer::checkpoint& _cfg;
    const config_batch_term_parser& _term_parser;
    crc::crc32c _crc;
    size_t _file_pos_to_end_of_batch{0};
};

// Called in the context of a ss::thread
log_replayer::checkpoint log_replayer::recover_in_thread() {
    vlog(stlog.debug, "Recovering segment {}", *_seg);
    // explicitly not using the index to recover the full file
    auto data_stream = _seg->reader().data_stream(0).get();
    auto consumer = std::make_unique<checksumming_consumer>(
      _seg, _ckpt, _term_parser);
    auto parser = continuous_batch_parser(
      std::move(consumer), std::move(data_stream), true);
    try {
        parser.consume().get();
    } catch (...) {
        vlog(
          stlog.warn,
          "{} partial recovery to {}, with: {}",
          _seg->reader().filename(),
          _ckpt,
          std::current_exception());
    }
    parser.close().get();
    return std::move(_ckpt);
}

fmt::iterator log_replayer::checkpoint::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{last_offset: {}, truncate_file_pos: {}}}",
      last_offset,
      truncate_file_pos);
}

} // namespace storage
