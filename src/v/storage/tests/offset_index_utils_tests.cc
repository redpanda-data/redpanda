#include "bytes/iobuf_file.h"
#include "random/generators.h"
#include "storage/segment_index.h"
#include "test_utils/fixture.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/test/tools/old/interface.hpp>

struct context {
    context(model::offset base = model::offset(0)) {
        _base_offset = base;
        // index
        _idx = std::make_unique<storage::segment_index>(
          "In memoyr iobuf",
          ss::file(ss::make_shared(iobuf_file(_data))),
          _base_offset,
          storage::segment_index::default_data_buffer_step);
    }
    ~context() { _idx->close().get(); }

    const model::record_batch_header
    modify_get(model::offset o, int32_t batch_size) {
        _base_hdr.base_offset = o;
        _base_hdr.size_bytes = batch_size;
        return _base_hdr;
    }
    void index_entry_expect(uint32_t offset, size_t filepos) {
        auto o = model::offset(offset);
        auto p = _idx->find_nearest(o);
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->offset, o);
        BOOST_REQUIRE_EQUAL(p->filepos, filepos);
    }

    model::offset _base_offset;
    model::record_batch_header _base_hdr;
    storage::segment_index_ptr _idx;
    iobuf _data;
};
FIXTURE_TEST(index_round_trip, context) {
    BOOST_CHECK(true);
    info("index: {}", _idx);
    for (uint32_t i = 0; i < 1024; ++i) {
        model::offset o = _base_offset + model::offset(i);
        _idx->maybe_track(
          modify_get(o, storage::segment_index::default_data_buffer_step), i);
    }
    info("About to flush index");
    _idx->flush().get0();
    info("{} - serializing from bytes into mem: buffer{}", _idx, _data);
    auto raw_idx = storage::index_state::hydrate_from_buffer(
      _data.share(0, _data.size_bytes()));
    BOOST_REQUIRE(raw_idx != std::nullopt);
    info("verifying tracking info: {}", *raw_idx);
    BOOST_REQUIRE_EQUAL(raw_idx->max_offset(), 1023);
    BOOST_REQUIRE_EQUAL(raw_idx->relative_offset_index.size(), 1024);
}

FIXTURE_TEST(bucket_bug1, context) {
    info("index: {}", _idx);
    info("Testing bucket find");
    _idx->maybe_track(modify_get(model::offset{824}, 155103), 0); // indexed
    _idx->maybe_track(
      modify_get(model::offset{849}, 168865), 155103); // indexed
    _idx->maybe_track(
      modify_get(model::offset{879}, 134080), 323968); // indexed
    _idx->maybe_track(
      modify_get(model::offset{901}, 142073), 458048); // indexed
    _idx->maybe_track(
      modify_get(model::offset{926}, 126886), 600121); // indexed
    _idx->maybe_track(
      modify_get(model::offset{948}, 1667), 727007); // not indexed

    index_entry_expect(824, 0);
    index_entry_expect(849, 155103);
    index_entry_expect(879, 323968);
    index_entry_expect(901, 458048);
    index_entry_expect(926, 600121);
    {
        auto p = _idx->find_nearest(model::offset(947));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->offset, model::offset(926));
        BOOST_REQUIRE_EQUAL(p->filepos, 600121);
    }
}
FIXTURE_TEST(bucket_truncate, context) {
    info("index: {}", _idx);
    info("Testing bucket truncate");
    _idx->maybe_track(modify_get(model::offset{824}, 155103), 0); // indexed
    _idx->maybe_track(
      modify_get(model::offset{849}, 168865), 155103); // indexed
    _idx->maybe_track(
      modify_get(model::offset{879}, 134080), 323968); // indexed
    _idx->maybe_track(
      modify_get(model::offset{901}, 142073), 458048); // indexed
    _idx->maybe_track(
      modify_get(model::offset{926}, 126886), 600121); // indexed
    _idx->maybe_track(
      modify_get(model::offset{948}, 1667), 727007); // not indexed
    // test range truncation next
    _idx->truncate(model::offset(926)).get();
    index_entry_expect(879, 323968);
    index_entry_expect(901, 458048);
    {
        auto p = _idx->find_nearest(model::offset(926));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->offset, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->filepos, 458048);
    }
    {
        auto p = _idx->find_nearest(model::offset(947));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->offset, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->filepos, 458048);
    }
}
