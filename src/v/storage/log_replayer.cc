#include "storage/log_replayer.h"

#include "hashing/crc32c.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "utils/vint.h"
#include "vlog.h"

#include <limits>
#include <type_traits>

namespace storage {
class checksumming_consumer final : public batch_consumer {
public:
    static constexpr size_t max_segment_size = static_cast<size_t>(
      std::numeric_limits<uint32_t>::max());
    checksumming_consumer(segment* s, log_replayer::checkpoint& c)
      : _seg(s)
      , _cfg(c) {}

    consume_result consume_batch_start(
      model::record_batch_header header,
      size_t physical_base_offset,
      size_t size_on_disk) override {
        const auto filesize = _seg->reader()->file_size();
        if (header.base_offset() < 0) {
            vlog(
              stlog.info,
              "Invalid base offset detected:{}, stopping parser",
              header.base_offset());
            return stop_parser::yes;
        }
        if (size_on_disk > max_segment_size) {
            vlog(
              stlog.info,
              "Invalid batch size:{}, file_size:{}, stopping parser",
              size_on_disk,
              filesize);
            return stop_parser::yes;
        }
        if (!header.attrs.is_valid_compression()) {
            vlog(
              stlog.info,
              "Invalid compression:{}. stopping parser",
              header.attrs);
            return stop_parser::yes;
        }
        if ((header.size_bytes + physical_base_offset) > filesize) {
            vlog(
              stlog.info,
              "offset + batch_size:{} exceeds filesize:{}, Stopping parsing",
              (header.size_bytes + physical_base_offset),
              filesize);
            return stop_parser::yes;
        }
        _seg->index()->maybe_track(header, physical_base_offset);
        _current_batch_crc = header.crc;
        _file_pos_to_end_of_batch = size_on_disk + physical_base_offset;
        _last_offset = header.last_offset();
        _crc = crc32();
        model::crc_record_batch_header(_crc, header);
        return skip_batch::no;
    }

    consume_result consume_record(model::record r) override {
        model::crc_record(_crc, r);
        return skip_batch::no;
    }
    void consume_compressed_records(iobuf&& records) override {
        _crc.extend(records);
    }

    stop_parser consume_batch_end() override {
        if (is_valid_batch_crc()) {
            _cfg.last_offset = _last_offset;
            _cfg.truncate_file_pos = _file_pos_to_end_of_batch;
            return stop_parser::no;
        }
        return stop_parser::yes;
    }

    bool is_valid_batch_crc() const {
        return _current_batch_crc == _crc.value();
    }

    ~checksumming_consumer() noexcept override = default;

private:
    segment* _seg;
    log_replayer::checkpoint& _cfg;
    int32_t _current_batch_crc;
    crc32 _crc;
    model::offset _last_offset;
    size_t _file_pos_to_end_of_batch;
};

// Called in the context of a ss::thread
log_replayer::checkpoint
log_replayer::recover_in_thread(const ss::io_priority_class& prio) {
    stlog.debug("Recovering segment {}", *_seg);
    // explicitly not using the index to recover the full file
    auto data_stream = _seg->reader()->data_stream(0, prio);
    auto consumer = std::make_unique<checksumming_consumer>(_seg, _ckpt);
    auto parser = continuous_batch_parser(
      std::move(consumer), std::move(data_stream));
    try {
        parser.consume().get();
    } catch (...) {
        stlog.warn(
          "{} partial recovery to {}, with: {}",
          _seg->reader()->filename(),
          _ckpt,
          std::current_exception());
    }
    return _ckpt;
}

std::ostream& operator<<(std::ostream& o, const log_replayer::checkpoint& c) {
    o << "{ last_offset: ";
    if (c.last_offset) {
        o << *c.last_offset;
    } else {
        o << "null";
    }
    o << ", truncate_file_pos:";
    if (c.truncate_file_pos) {
        o << *c.truncate_file_pos;
    } else {
        o << "null";
    }
    return o << "}";
}
} // namespace storage
