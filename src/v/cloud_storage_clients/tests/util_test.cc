#include "cloud_storage_clients/util.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_all_paths_to_file) {
    using namespace cloud_storage_clients;

    auto result1 = util::all_paths_to_file(object_key{"a/b/c/log.txt"});
    auto expected1 = std::vector<object_key>{
      object_key{"a"},
      object_key{"a/b"},
      object_key{"a/b/c"},
      object_key{"a/b/c/log.txt"}};
    BOOST_REQUIRE_EQUAL(result1, expected1);

    auto result2 = util::all_paths_to_file(object_key{"a/b/c/"});
    BOOST_REQUIRE_EQUAL(result2, std::vector<object_key>{});

    auto result3 = util::all_paths_to_file(object_key{""});
    BOOST_REQUIRE_EQUAL(result3, std::vector<object_key>{});

    auto result4 = util::all_paths_to_file(object_key{"foo"});
    BOOST_REQUIRE_EQUAL(result4, std::vector<object_key>{object_key{"foo"}});
}
