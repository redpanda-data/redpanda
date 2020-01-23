#include "storage/log_replayer.h"

#include "hashing/crc32c.h"
#include "model/record.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "utils/vint.h"

#include <seastar/util/defer.hh>

#include <limits>

namespace storage {

void crc_batch_header(
  crc32& crc, const model::record_batch_header& header, size_t num_records) {
    crc.extend(header.attrs.value());
    crc.extend(header.last_offset_delta);
    crc.extend(header.first_timestamp.value());
    crc.extend(header.max_timestamp.value());
    // Unused Kafka fields
    std::array<uint8_t, 14> unused{};
    unused.fill(-1);
    crc.extend(&unused[0], unused.size());
    crc.extend(int32_t(num_records));
}

void crc_record_header_and_key(
  crc32& crc,
  size_t size_bytes,
  const model::record_attributes& attributes,
  int32_t timestamp_delta,
  int32_t offset_delta,
  const iobuf& key) {
    crc.extend_vint(size_bytes);
    crc.extend(attributes.value());
    crc.extend_vint(timestamp_delta);
    crc.extend_vint(offset_delta);
    crc.extend_vint(key.size_bytes());
    crc.extend(key);
}

void crc_record_header_and_key(crc32& crc, const model::record& r) {
    crc.extend_vint(r.size_bytes());
    crc.extend_vint(r.attributes().value());
    crc.extend_vint(r.timestamp_delta());
    crc.extend_vint(r.offset_delta());
    crc.extend_vint(r.key().size_bytes());
    crc.extend(r.key());
}

class checksumming_consumer final : public batch_consumer {
public:
    static constexpr size_t max_segment_size = static_cast<size_t>(
      std::numeric_limits<uint32_t>::max());
    explicit checksumming_consumer(segment* s)
      : _seg(s) {}

    consume_result consume_batch_start(
      model::record_batch_header header,
      size_t num_records,
      size_t physical_base_offset,
      size_t size_on_disk) override {
        const auto filesize = _seg->reader()->file_size();
        if (
          // the following prevents malformed payload; see
          // log_replay_test.cc::malformed_segment
          header.base_offset() < 0 || size_on_disk >= max_segment_size
          || size_on_disk > filesize || !header.attrs.is_valid_compression()
          || (header.size_bytes + physical_base_offset) > filesize) {
            _last_valid_offset = {};
            stlog.info("checksumming_consumer::consume_batch_start:: invalid "
                       "record batch header. Stopping parsing");
            return stop_parser::yes;
        }
        _seg->oindex()->maybe_track(
          header.base_offset, physical_base_offset, size_on_disk);
        _current_batch_crc = header.crc;
        _last_offset = header.last_offset();
        _crc = crc32();
        crc_batch_header(_crc, header, num_records);
        return skip_batch::no;
    }

    consume_result consume_record_key(
      size_t size_bytes,
      model::record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key) override {
        crc_record_header_and_key(
          _crc, size_bytes, attributes, timestamp_delta, offset_delta, key);
        return skip_batch::no;
    }

    void consume_record_value(iobuf&& value_and_headers) override {
        _crc.extend(value_and_headers);
    }

    void consume_compressed_records(iobuf&& records) override {
        _crc.extend(records);
    }

    stop_parser consume_batch_end() override {
        if (recovered()) {
            _last_valid_offset = _last_offset;
            return stop_parser::no;
        }
        return stop_parser::yes;
    }

    bool recovered() const { return _current_batch_crc == _crc.value(); }

    std::optional<model::offset> last_valid_offset() const {
        return _last_valid_offset;
    }
    ~checksumming_consumer() noexcept override = default;

private:
    segment* _seg;
    uint32_t _current_batch_crc = -1;
    crc32 _crc;
    model::offset _last_offset;
    std::optional<model::offset> _last_valid_offset;
};

// Called in the context of a ss::thread
log_replayer::recovered
log_replayer::recover_in_thread(const ss::io_priority_class& prio) {
    stlog.debug("Recovering segment {}", *_seg);
    // explicitly not using the index to recover the full file
    auto data_stream = _seg->reader()->data_stream(0, prio);
    auto d = ss::defer([&data_stream] { data_stream.close().get(); });
    auto consumer = checksumming_consumer(_seg);
    auto parser = continuous_batch_parser(consumer, data_stream);
    try {
        parser.consume().get();
        return recovered{consumer.recovered(), consumer.last_valid_offset()};
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
