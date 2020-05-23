#include "bytes/bytes.h"
#include "bytes/iobuf_file.h"
#include "bytes/iobuf_parser.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_topic_index.h"
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
    (void)p.read_bool();
    auto [val, _] = p.read_varlong();
    const auto key_result = p.read_bytes(val);
    BOOST_REQUIRE_EQUAL(key, key_result);
    auto [offset, _1] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(model::offset(offset), model::offset(42));
    auto [delta, _2] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(delta, 66);
    auto footer
      = reflection::adl<storage::compacted_topic_index::footer>{}.from(p);
    info("{}", footer);
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.size,
      1 /*type*/ + 1024 /*actual key*/ + 2 /*key size*/ + 1 /*offset*/
        + 1 /*delta*/);
    BOOST_REQUIRE_EQUAL(footer.version, 0);
}
