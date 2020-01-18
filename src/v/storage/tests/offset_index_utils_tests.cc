#include "random/generators.h"
#include "storage/segment_offset_index.h"
#include "storage/segment_offset_index_utils.h"
#include "test_utils/fixture.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>

#include <boost/test/tools/old/interface.hpp>

struct context {
    context() {
        auto name = random_generators::gen_alphanum_string(20);
        const auto flags = ss::open_flags::create | ss::open_flags::rw
                           | ss::open_flags::truncate;
        auto f = ss::open_file_dma(name, flags).get0();
        _base_offset = model::offset(random_generators::get_int<uint32_t>());

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
