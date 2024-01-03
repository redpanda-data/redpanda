/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/seastarx.h"
#include "base/vlog.h"
#include "utils/filtered_lower_bound.h"

#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

#include <optional>
#include <random>

static ss::logger test_log("test_logger");

struct optional_filter {
    bool operator()(const std::optional<int>& opt) const {
        return opt.has_value();
    }
};

struct optional_compare {
    bool operator()(
      const std::optional<int>& lhs, const std::optional<int>& rhs) const {
        BOOST_CHECK(lhs.has_value());
        BOOST_CHECK(rhs.has_value());

        return *lhs < *rhs;
    }

    bool operator()(const std::optional<int>& lhs, int rhs) const {
        BOOST_CHECK(lhs.has_value());

        return *lhs < rhs;
    }
};

std::string print(const std::optional<int>& opt) {
    return opt ? std::to_string(opt.value()) : "'null'";
}

std::string print(std::vector<std::optional<int>>& to_search) {
    std::string res = "";
    for (size_t i = 0; i < to_search.size(); ++i) {
        if (i == 0) {
            res += "[" + print(to_search[i]) + ", ";
        } else if (i == to_search.size() - 1) {
            res += print(to_search[i]) + "]";
        } else {
            res += print(to_search[i]) + ", ";
        }
    }

    return res;
}

void validate(std::vector<std::optional<int>>& to_search, int searched_value) {
    auto found_iter = filtered_lower_bound(
      to_search.begin(),
      to_search.end(),
      searched_value,
      optional_compare{},
      optional_filter{});

    auto linear_search_result = to_search.end();
    for (auto iter = to_search.begin(); iter != to_search.end(); ++iter) {
        if (iter->has_value() && iter->value() >= searched_value) {
            linear_search_result = iter;
            break;
        }
    }

    auto found_dist = std::distance(to_search.begin(), found_iter);
    auto expected_dist = std::distance(to_search.begin(), linear_search_result);

    if (found_dist != expected_dist) {
        vlog(
          test_log.info,
          "Failure detected: to_search={} searched_value={}, "
          "expected_index={}, found_index={}",
          print(to_search),
          searched_value,
          expected_dist,
          found_dist);
    }

    BOOST_CHECK_EQUAL(found_dist, expected_dist);
}

BOOST_AUTO_TEST_CASE(filtered_lower_bound_test) {
    std::vector<std::optional<int>> case1 = {1, 3, 5, std::nullopt, 7, 9};
    for (auto val : {0, 1, 3, 5, 6, 7, 9, 100}) {
        validate(case1, val);
    }

    std::vector<std::optional<int>> case2 = {
      1, 3, 5, std::nullopt, std::nullopt, 7, 9};
    for (auto val : {0, 1, 3, 5, 6, 7, 9, 100}) {
        validate(case2, val);
    }

    std::vector<std::optional<int>> case3 = {
      1, 3, 5, std::nullopt, std::nullopt, std::nullopt, 7, 9};
    for (auto val : {0, 1, 3, 5, 6, 7, 9, 100}) {
        validate(case3, val);
    }

    std::vector<std::optional<int>> case4 = {
      1, std::nullopt, 3, 5, std::nullopt, std::nullopt, std::nullopt, 7, 9};
    for (auto val : {0, 1, 3, 5, 6, 7, 9, 100}) {
        validate(case4, val);
    }

    std::vector<std::optional<int>> case5 = {
      std::nullopt, std::nullopt, std::nullopt, std::nullopt};
    for (auto val : {0, 1}) {
        validate(case5, val);
    }

    std::vector<std::optional<int>> case6 = {
      0, std::nullopt, std::nullopt, std::nullopt, std::nullopt};
    for (auto val : {-1, 0, 1}) {
        validate(case6, val);
    }

    std::vector<std::optional<int>> case7 = {
      std::nullopt, std::nullopt, std::nullopt, std::nullopt, 1};
    for (auto val : {-1, 0, 1}) {
        validate(case7, val);
    }

    std::vector<std::optional<int>> case8 = {
      std::nullopt, 0, 1, std::nullopt, 2, 3, 4, std::nullopt, std::nullopt, 5};
    for (auto val : {-1, 0, 1, 2, 3, 4, 5, 6}) {
        validate(case8, val);
    }

    std::vector<std::optional<int>> case9 = {
      std::nullopt,
      0,
      std::nullopt,
      std::nullopt,
      1,
      2,
      3,
      std::nullopt,
      std::nullopt,
      4};
    validate(case9, -1);
}

BOOST_AUTO_TEST_CASE(fuzz_test) {
    std::vector<size_t> sizes = {10, 100, 1000};
    std::vector<double> null_probability = {0, 0.1, 0.25, 0.5, 0.9};

    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0.0, 1.0);

    vlog(test_log.info, "SEED={}", seed);

    for (size_t size : sizes) {
        for (auto prob : null_probability) {
            vlog(test_log.info, "SIZE={} PROB={}", size, prob);
            for (size_t i = 0; i < 100; ++i) {
                std::vector<std::optional<int>> values;
                int last_inserted = 0;
                for (size_t i = 0; i < size; ++i) {
                    auto local_prob = dis(gen);
                    if (local_prob < prob) {
                        values.push_back(std::nullopt);
                    } else {
                        values.push_back(last_inserted++);
                    }
                }

                vlog(test_log.info, "CASE={}", print(values));
                for (int i = -1; i <= last_inserted; ++i) {
                    validate(values, i);
                }
            }
        }
    }
}
