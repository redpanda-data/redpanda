#include "random/generators.h"
#include "storage/segment_offset_index.h"
#include "storage/segment_offset_index_utils.h"
#include "test_utils/fixture.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>

#include <boost/test/tools/old/interface.hpp>

struct context {
    context(model::offset base = model::offset(0)) {
        auto name = "test." + random_generators::gen_alphanum_string(20);
        const auto flags = ss::open_flags::create | ss::open_flags::rw
                           | ss::open_flags::truncate;
        auto f = ss::open_file_dma(name, flags).get0();
        _base_offset = base;

        // index
        _idx = std::make_unique<storage::segment_offset_index>(
          name,
          f,
          _base_offset,
          storage::segment_offset_index::default_data_buffer_step);
    }
    ~context() { _idx->close().get(); }
    model::offset _base_offset;
    storage::segment_offset_index_ptr _idx;
};
FIXTURE_TEST(index_round_trip, context) {
    BOOST_CHECK(true);
    info("Writing tracking info");
    for (uint32_t i = 0; i < 1024; ++i) {
        model::offset o = _base_offset + model::offset(i);
        _idx->maybe_track(o, i, 4096);
    }
    info("About to flush index");
    _idx->flush().get0();
    info("re-reading the underlying file again");
    auto buf = read_fully_tmpbuf(_idx->filename()).get0();
    info("serializing from bytes into mem");
    auto raw_idx = storage::offset_index_from_buf(std::move(buf));
    info("verifying tracking info");
    BOOST_REQUIRE_EQUAL(raw_idx.size(), 1024 * 4096 / _idx->step());
}
FIXTURE_TEST(bucket_bug1, context) {
    info("Testing bucket find");
    _idx->maybe_track(model::offset{824}, 0, 155103);      // indexed
    _idx->maybe_track(model::offset{849}, 155103, 168865); // indexed
    _idx->maybe_track(model::offset{879}, 323968, 134080); // indexed
    _idx->maybe_track(model::offset{901}, 458048, 142073); // indexed
    _idx->maybe_track(model::offset{926}, 600121, 126886); // indexed
    _idx->maybe_track(model::offset{948}, 727007, 1667);   // not indexed
    {
        auto p = _idx->lower_bound_pair(model::offset(824));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(824));
        BOOST_REQUIRE_EQUAL(p->second, 0);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(849));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(849));
        BOOST_REQUIRE_EQUAL(p->second, 155103);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(879));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(879));
        BOOST_REQUIRE_EQUAL(p->second, 323968);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(901));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->second, 458048);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(926));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(926));
        BOOST_REQUIRE_EQUAL(p->second, 600121);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(947));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(926));
        BOOST_REQUIRE_EQUAL(p->second, 600121);
    }
}
FIXTURE_TEST(bucket_truncate, context) {
    info("Testing bucket truncate");
    _idx->maybe_track(model::offset{824}, 0, 155103);      // indexed
    _idx->maybe_track(model::offset{849}, 155103, 168865); // indexed
    _idx->maybe_track(model::offset{879}, 323968, 134080); // indexed
    _idx->maybe_track(model::offset{901}, 458048, 142073); // indexed
    _idx->maybe_track(model::offset{926}, 600121, 126886); // indexed
    _idx->maybe_track(model::offset{948}, 727007, 1667);   // not indexed
    // test range truncation next
    _idx->truncate(model::offset(926)).get();
    {
        auto p = _idx->lower_bound_pair(model::offset(879));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(879));
        BOOST_REQUIRE_EQUAL(p->second, 323968);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(901));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->second, 458048);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(926));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->second, 458048);
    }
    {
        auto p = _idx->lower_bound_pair(model::offset(947));
        BOOST_REQUIRE(bool(p));
        BOOST_REQUIRE_EQUAL(p->first, model::offset(901));
        BOOST_REQUIRE_EQUAL(p->second, 458048);
    }
}
