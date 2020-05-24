#include "bytes/bytes.h"
#include "bytes/iobuf_file.h"
#include "bytes/iobuf_parser.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "test_utils/fixture.h"
#include "units.h"

struct compacted_topic_fixture {};
FIXTURE_TEST(format_verification, compacted_topic_fixture) {
    iobuf index_data;
    auto idx = storage::make_file_backed_compacted_index(
      "dummy name",
      ss::file(ss::make_shared(iobuf_file(index_data))),
      ss::default_priority_class(),
      1_KiB);
    const auto key = random_generators::get_bytes(1024);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);
    BOOST_REQUIRE_EQUAL(index_data.size_bytes(), 1047);
    iobuf_parser p(index_data.share(0, index_data.size_bytes()));
    (void)p.consume_type<uint16_t>(); // SIZE
    (void)p.consume_type<uint8_t>();  // TYPE
    auto [offset, _1] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(model::offset(offset), model::offset(42));
    auto [delta, _2] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(delta, 66);
    const auto key_result = p.read_bytes(1024);
    BOOST_REQUIRE_EQUAL(key, key_result);
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(p);
    info("{}", footer);
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.size,
      sizeof(uint16_t)
        + 1 /*type*/ + 1 /*offset*/ + 2 /*delta*/ + 1024 /*key*/);
    BOOST_REQUIRE_EQUAL(footer.version, 0);
    BOOST_REQUIRE(footer.crc != 0);
}
FIXTURE_TEST(format_verification_max_key, compacted_topic_fixture) {
    iobuf index_data;
    auto idx = storage::make_file_backed_compacted_index(
      "dummy name",
      ss::file(ss::make_shared(iobuf_file(index_data))),
      ss::default_priority_class(),
      1_MiB);
    const auto key = random_generators::get_bytes(1_MiB);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    BOOST_REQUIRE_EQUAL(
      index_data.size_bytes(),
      storage::compacted_index::footer_size
        + std::numeric_limits<uint16_t>::max());
    iobuf_parser p(index_data.share(0, index_data.size_bytes()));

    const size_t entry = p.consume_type<uint16_t>(); // SIZE
    BOOST_REQUIRE_EQUAL(
      entry, std::numeric_limits<uint16_t>::max() - sizeof(uint16_t));
}
