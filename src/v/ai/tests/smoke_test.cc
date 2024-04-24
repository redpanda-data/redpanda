/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "ai/service.h"
#include "container/zip.h"

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <vector>

namespace ai {

struct test_data {
    std::string prompt;
    std::string response;
    int max_tokens = 15;
};

void PrintTo(const test_data& d, std::ostream* os) { *os << d.prompt; }

class LLMTest : public testing::TestWithParam<test_data> {
public:
    inline static ss::sharded<service> s;

    static void SetUpTestSuite() {
        s.start().get();
        s.invoke_on_all(&ai::service::start).get();
    }
    static void TearDownTestSuite() { s.stop().get(); }
};

TEST_P(LLMTest, Smoke) {
    const auto& param = GetParam();
    auto response
      = s.local().compute_embeddings(param.prompt + param.response).get();
    EXPECT_EQ(response.size(), 384);
}

static const std::vector<test_data> testcases = {
  {
    .prompt = "My name is ",
    .response = "100% real. I am a 25 year old female",
  },
  {
    .prompt = "My name is",
    .response = " Katie and I am a 20 year old student from the UK",
  },
  {
    .prompt = "What is the meaning of life?",
    .response
    = " What is the meaning of death? What is the meaning of suffering? What",
  },
  {
    .prompt = "Tell me an interesting fact about llamas.",
    .response = "\nLlamas are not native to the Andes. They were brought",
  },
  {
    .prompt = "Recommend some interesting books to read.",
    .response = "\nI'm looking for some interesting books to read. I'm",
  },
  {
    .prompt = "How to get a job at Google?",
    .response = "\nHow to get a job at Google? Google is one of the most",
  },
  {
    .prompt = "I want to learn how to play the piano.",
    .response = " I want to learn how to play the piano. I want to learn how",
  },
};

float cosine_similarity(
  const std::vector<float>& lhs, const std::vector<float>& rhs) {
    float dot = 0.0, denom_a = 0.0, denom_b = 0.0;
    for (const auto& [a, b] : container::zip(lhs, rhs)) {
        dot += a * b;
        denom_a += a * a;
        denom_b += b * b;
    }
    return dot / (sqrt(denom_a) * sqrt(denom_b));
}

TEST_F(LLMTest, ParallelSmoke) {
    absl::flat_hash_map<ss::sstring, std::vector<float>> expected;
    for (const auto& tc : testcases) {
        auto response
          = s.local().compute_embeddings(tc.prompt + tc.response).get();
        expected.emplace(tc.response, response);
    }
    constexpr int iters = 5;
    for (int i = 0; i < iters; ++i) {
        std::vector<std::pair<test_data, ss::future<std::vector<float>>>> jobs;
        jobs.reserve(testcases.size() * iters);
        for (int j = 0; j < iters; ++j) {
            for (const auto& tc : testcases) {
                jobs.emplace_back(
                  tc, s.local().compute_embeddings(tc.prompt + tc.response));
            }
        }
        for (auto& [tc, job] : jobs) {
            std::vector<float> response = job.get();
            EXPECT_EQ(response.size(), 384);
            EXPECT_GT(cosine_similarity(response, expected[tc.response]), 0.9);
            for (const auto& other : testcases) {
                if (tc.prompt == other.prompt) {
                    continue;
                }
                EXPECT_LT(
                  cosine_similarity(response, expected[other.response]), 0.9);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  KnownLlama2Responses,
  LLMTest,
  testing::ValuesIn<std::vector<test_data>>(testcases));

} // namespace ai
