#include "storage/log_replayer.h"

#include "hashing/crc32c.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "utils/vint.h"

#include <limits>
#include <type_traits>

namespace storage {
struct checksumming_cfg {
    std::optional<model::offset> last_offset;
    bool is_valid_crc{false};
};

class checksumming_consumer final : public batch_consumer {
public:
    static constexpr size_t max_segment_size = static_cast<size_t>(
      std::numeric_limits<uint32_t>::max());
    explicit checksumming_consumer(segment* s, checksumming_cfg& c)
      : _seg(s)
      , _cfg(c) {}

    consume_result consume_batch_start(
      model::record_batch_header header,
      size_t physical_base_offset,
      size_t size_on_disk) override {
        const auto filesize = _seg->reader()->file_size();
        if (header.base_offset() < 0) {
            _cfg.last_offset = {};
            stlog.info(
              "Invalid base offset detected:{}, stopping parser",
              header.base_offset());
            return stop_parser::yes;
        }
        if (size_on_disk > max_segment_size) {
            _cfg.last_offset = {};
            stlog.info(
              "Invalid batch size:{}, file_size:{}, stopping parser",
              size_on_disk,
              filesize);
            return stop_parser::yes;
        }
        if (!header.attrs.is_valid_compression()) {
            _cfg.last_offset = {};
            stlog.info("Invalid compression:{}. stopping parser", header.attrs);
            return stop_parser::yes;
        }
        if ((header.size_bytes + physical_base_offset) > filesize) {
            _cfg.last_offset = {};
            stlog.info(
              "offset + batch_size:{} exceeds filesize:{}, Stopping parsing",
              (header.size_bytes + physical_base_offset),
              header);
            return stop_parser::yes;
        }
        _seg->oindex()->maybe_track(
          header.base_offset, physical_base_offset, size_on_disk);
        _current_batch_crc = header.crc;
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
        _cfg.is_valid_crc = recovered();
        if (_cfg.is_valid_crc) {
            _cfg.last_offset = _last_offset;
            return stop_parser::no;
        }
        return stop_parser::yes;
    }

    bool recovered() const { return _current_batch_crc == _crc.value(); }

    ~checksumming_consumer() noexcept override = default;

private:
    segment* _seg;
    checksumming_cfg& _cfg;
    int32_t _current_batch_crc;
    crc32 _crc;
    model::offset _last_offset;
};

// Called in the context of a ss::thread
log_replayer::recovered
log_replayer::recover_in_thread(const ss::io_priority_class& prio) {
    stlog.debug("Recovering segment {}", *_seg);
    // explicitly not using the index to recover the full file
    checksumming_cfg cfg;
    auto data_stream = _seg->reader()->data_stream(0, prio);
    auto consumer = std::make_unique<checksumming_consumer>(_seg, cfg);
    auto parser = continuous_batch_parser(
      std::move(consumer), std::move(data_stream));
    try {
        parser.consume().get();
        return recovered{cfg.is_valid_crc, cfg.last_offset};
    } catch (const malformed_batch_stream_exception& e) {
        stlog.debug("Failed to recover segment {} with {}", *_seg, e);
    } catch (...) {
        stlog.warn(
          "Failed to recover segment {} with {}",
          *_seg,
          std::current_exception());
    }
    return recovered{false, std::nullopt};
}

} // namespace storage
