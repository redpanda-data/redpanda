#include "bytes/iobuf.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/disk_log_appender.h"
#include "storage/log_segment_appender_utils.h"
#include "storage/log_segment_reader.h"
#include "storage/parser.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

class test_consumer : public batch_consumer {
public:
    test_consumer(size_t batch_skips, size_t record_skips, bool stop_at_batch)
      : _batch_skips(batch_skips)
      , _record_skips(record_skips)
      , _stop_at_batch(stop_at_batch) {}

    virtual skip consume_batch_start(
      model::record_batch_header header,
      size_t num_records,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        _header = std::move(header);
        _num_records = num_records;
        if (_header.attrs.compression() == model::compression::none) {
            // Reset the variant.
            _records = model::record_batch::uncompressed_records();
        } else if (_batch_skips) {
            _batch_skips--;
            return skip::yes;
        }
        return skip::no;
    }

    virtual skip consume_record_key(
      size_t size_bytes,
      model::record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key) override {
        if (_record_skips) {
            _record_skips--;
            return skip::yes;
        }
        _record_size_bytes = size_bytes;
        _record_attributes = attributes;
        _record_timestamp_delta = timestamp_delta;
        _record_offset_delta = offset_delta;
        _record_key = std::move(key);
        return skip::no;
    }

    virtual void consume_record_value(iobuf&& value_and_headers) override {
        std::get<model::record_batch::uncompressed_records>(_records)
          .emplace_back(
            _record_size_bytes,
            _record_attributes,
            _record_timestamp_delta,
            _record_offset_delta,
            std::move(_record_key),
            std::move(value_and_headers));
    }

    virtual void consume_compressed_records(iobuf&& records) override {
        _records = model::record_batch::compressed_records(
          _num_records, std::move(records));
    }

    virtual ss::stop_iteration consume_batch_end() override {
        batches.emplace_back(std::move(_header), std::move(_records));
        return ss::stop_iteration(_stop_at_batch);
    }

    std::vector<model::record_batch> batches;

private:
    size_t _batch_skips;
    size_t _record_skips;
    bool _stop_at_batch;
    model::record_batch_header _header;
    size_t _num_records;
    size_t _record_size_bytes;
    model::record_attributes _record_attributes;
    int32_t _record_timestamp_delta;
    int32_t _record_offset_delta;
    iobuf _record_key;
    model::record_batch::records_type _records;
};

struct context {
    segment_reader_ptr log_seg;
    continuous_batch_parser_opt parser;
    ss::input_stream<char> in;

    void write(std::vector<model::record_batch>& batches, test_consumer& c) {
        auto fd = ss::open_file_dma(
                    "test", ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        fd = ss::file(make_shared(file_io_sanitizer(std::move(fd))));
        auto appender = log_segment_appender(
          fd, log_segment_appender::options(ss::default_priority_class()));
        for (auto& b : batches) {
            storage::write(appender, b).get();
        }
        appender.flush().get();
        log_seg = log_segment_reader(
          "test",
          std::move(fd),
          model::term_id(0),
          batches.begin()->base_offset(),
          appender.file_byte_offset(),
          128);
        in = log_seg->data_stream(0, ss::default_priority_class());
        parser = continuous_batch_parser(c, in);
    }

    bool eof() { return in.eof(); }

    ~context() {
        in.close().get();
        log_seg->close().get();
    }
};

void check_batches(
  std::vector<model::record_batch>& actual,
  std::vector<model::record_batch>& expected) {
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      actual.begin(), actual.end(), expected.begin(), expected.end());
}

SEASTAR_THREAD_TEST_CASE(test_can_parse_single_batch) {
    context ctx;
    test_consumer c(0, 0, false);
    auto batches = test::make_random_batches(model::offset(1), 1);
    ctx.write(batches, c);
    ctx.parser->consume().get();
    check_batches(c.batches, batches);
}

SEASTAR_THREAD_TEST_CASE(test_can_parse_multiple_batches) {
    context ctx;
    test_consumer c(0, 0, false);
    auto batches = test::make_random_batches();
    ctx.write(batches, c);
    ctx.parser->consume().get();
    check_batches(c.batches, batches);
}

SEASTAR_THREAD_TEST_CASE(test_can_parse_multiple_batches_one_at_a_time) {
    context ctx;
    test_consumer c(0, 0, true);
    auto batches = test::make_random_batches();
    ctx.write(batches, c);
    while (!ctx.eof()) {
        ctx.parser->consume().get();
    }
    check_batches(c.batches, batches);
}

SEASTAR_THREAD_TEST_CASE(test_skips) {
    context ctx;
    size_t batches_to_skip = 7;
    size_t records_to_skip = 32;
    test_consumer c(batches_to_skip, records_to_skip, true);
    auto batches = test::make_random_batches();
    ctx.write(batches, c);

    for (auto it = batches.begin(); it != batches.end();) {
        if (it->compressed()) {
            if (batches_to_skip) {
                it = batches.erase(it);
                batches_to_skip--;
                continue;
            }
        } else if (records_to_skip) {
            auto& rs = it->get_uncompressed_records_for_testing();
            auto n = std::min(records_to_skip, rs.size());
            records_to_skip -= n;
            rs.erase(rs.begin(), rs.begin() + n);
        }
        ++it;
    }

    while (!ctx.eof()) {
        ctx.parser->consume().get();
    }
    check_batches(c.batches, batches);
}
