#define BOOST_TEST_MODULE utils
#include "kafka/requests/topics/topic_utils.h"
#include "model/fundamental.h"

#include <boost/range/iterator_range.hpp>
#include <boost/test/unit_test.hpp>

using namespace kafka; // NOLINT
namespace {
struct test_request {
    model::topic_view topic;
    int32_t partitions;
    int16_t replication_factor;
};
std::vector<test_request> valid_requests() {
    return {
      {.topic = model::topic_view{"tp_1"},
       .partitions = 2,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_2"},
       .partitions = 8,
       .replication_factor = 5},
      {.topic = model::topic_view{"tp_3"},
       .partitions = 6,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_4"},
       .partitions = 4,
       .replication_factor = 1},
    };
}

std::vector<test_request> mixed_requests() {
    return {
      {.topic = model::topic_view{"tp_1"},
       .partitions = -2,
       .replication_factor = 3}, // invalid partition
      {.topic = model::topic_view{"tp_2"},
       .partitions = 8,
       .replication_factor = 5},
      {.topic = model::topic_view{"tp_3"},
       .partitions = 8,
       .replication_factor = 0}, // invalid replication_factor
      {.topic = model::topic_view{"tp_4"},
       .partitions = 0,
       .replication_factor = 1}, // invalid partition
      {.topic = model::topic_view{"tp_5"},
       .partitions = 7,
       .replication_factor = 3},
    };
}
std::vector<test_request> invalid_requests() {
    return {
      {.topic = model::topic_view{"tp_1"},
       .partitions = -2,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_2"},
       .partitions = 0,
       .replication_factor = 5},
      {.topic = model::topic_view{"tp_3"},
       .partitions = 8,
       .replication_factor = 0},
    };
}

std::vector<test_request> duplicated_requests() {
    return {
      {.topic = model::topic_view{"tp_1"},
       .partitions = 2,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_2"},
       .partitions = 8,
       .replication_factor = 5},
      {.topic = model::topic_view{"tp_3"},
       .partitions = 6,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_4"},
       .partitions = 4,
       .replication_factor = 1},
      {.topic = model::topic_view{"tp_1"},
       .partitions = 2,
       .replication_factor = 3},
      {.topic = model::topic_view{"tp_2"},
       .partitions = 8,
       .replication_factor = 5},
    };
}

struct partitions_validator {
    static constexpr kafka::error_code ec
      = kafka::error_code::invalid_partitions;
    static constexpr const char* error_message = "Partitions count is invalid";

    static bool is_valid(const test_request& r) {
        return r.partitions > 0 && r.partitions < 10;
    }
};

struct r_factor_validator {
    static constexpr kafka::error_code ec
      = kafka::error_code::invalid_replication_factor;
    static constexpr const char* error_message = "RF is invalid";

    static bool is_valid(const test_request& r) {
        return r.replication_factor > 0 && r.replication_factor <= 5;
    }
};

using test_validators = make_validator_types<
  test_request,
  partitions_validator,
  r_factor_validator>;

} // namespace

BOOST_AUTO_TEST_CASE(
  shall_fill_vector_with_errors_and_return_iterator_to_end_of_valid_range) {
    auto requests = mixed_requests();
    std::vector<topic_op_result> errs;
    auto valid_range_end = validate_requests_range(
      requests.begin(),
      requests.end(),
      std::back_inserter(errs),
      partitions_validator::ec,
      partitions_validator::error_message,
      partitions_validator::is_valid);
    BOOST_REQUIRE_EQUAL(errs.size(), 2);
    for (auto const& e : errs) {
        BOOST_TEST(
          (int8_t)e.ec == (int8_t)kafka::error_code::invalid_partitions);
        BOOST_REQUIRE_EQUAL(*(e.err_msg), "Partitions count is invalid");
    }

    BOOST_REQUIRE_EQUAL(std::distance(requests.begin(), valid_range_end), 3);
    for (auto r :
         boost::make_iterator_range(requests.begin(), valid_range_end)) {
        BOOST_REQUIRE_NE(r.topic, "tp_1");
        BOOST_REQUIRE_NE(r.topic, "tp_4");
    }
};

BOOST_AUTO_TEST_CASE(shall_return_no_errors) {
    auto requests = valid_requests();
    std::vector<topic_op_result> errs;
    auto valid_range_end = validate_requests_range(
      requests.begin(),
      requests.end(),
      std::back_inserter(errs),
      partitions_validator::ec,
      partitions_validator::error_message,
      partitions_validator::is_valid);
    BOOST_REQUIRE_EQUAL(errs.size(), 0);
    BOOST_REQUIRE_EQUAL(std::distance(requests.begin(), valid_range_end), 4);
};

BOOST_AUTO_TEST_CASE(shall_generate_errors_for_duplicated_topics) {
    auto requests = duplicated_requests();
    std::vector<topic_op_result> errs;
    auto valid_range_end = validate_range_duplicates(
      requests.begin(), requests.end(), std::back_inserter(errs));

    BOOST_REQUIRE_EQUAL(errs.size(), 4);
    BOOST_REQUIRE_EQUAL(std::distance(requests.begin(), valid_range_end), 2);
    for (auto r :
         boost::make_iterator_range(requests.begin(), valid_range_end)) {
        BOOST_REQUIRE_NE(r.topic, "tp_1");
        BOOST_REQUIRE_NE(r.topic, "tp_2");
    }
};

BOOST_AUTO_TEST_CASE(shall_validate_requests_with_all_validators) {
    auto requests = mixed_requests();
    std::vector<topic_op_result> errs;
    auto valid_range_end = validate_requests_range(
      requests.begin(),
      requests.end(),
      std::back_inserter(errs),
      test_validators{});

    BOOST_REQUIRE_EQUAL(errs.size(), 3);
    BOOST_REQUIRE_EQUAL(std::distance(requests.begin(), valid_range_end), 2);
    for (auto r :
         boost::make_iterator_range(requests.begin(), valid_range_end)) {
        BOOST_REQUIRE_NE(r.topic, "tp_1");
        BOOST_REQUIRE_NE(r.topic, "tp_3");
        BOOST_REQUIRE_NE(r.topic, "tp_4");
    }
};

BOOST_AUTO_TEST_CASE(shall_return_errors_for_all_requests) {
    auto requests = invalid_requests();
    std::vector<topic_op_result> errs;
    auto valid_range_end = validate_requests_range(
      requests.begin(),
      requests.end(),
      std::back_inserter(errs),
      test_validators{});

    BOOST_REQUIRE_EQUAL(errs.size(), 3);
    BOOST_REQUIRE_EQUAL(std::distance(requests.begin(), valid_range_end), 0);
};
