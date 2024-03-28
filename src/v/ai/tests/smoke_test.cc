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

#include <gtest/gtest.h>

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
        service::config service_config{
          .model_file
          = "/home/rockwood/code/llama.cpp/models/llama-2-7b.Q4_K_M.gguf",
        };
        s.invoke_on_all(
           [&service_config](service& s) { return s.start(service_config); })
          .get();
    }
    static void TearDownTestSuite() { s.stop().get(); }
};

TEST_P(LLMTest, Smoke) {
    const auto& param = GetParam();
    service::generate_text_options opts = {
      .max_tokens = param.max_tokens,
    };
    auto response = s.local().generate_text(param.prompt, opts).get();
    EXPECT_EQ(std::string(response), param.response);
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

TEST_F(LLMTest, ParallelSmoke) {
    constexpr int iters = 5;
    for (int i = 0; i < iters; ++i) {
        std::vector<std::pair<test_data, ss::future<ss::sstring>>> jobs;
        jobs.reserve(testcases.size() * iters);
        for (int j = 0; j < iters; ++j) {
            for (const auto& tc : testcases) {
                service::generate_text_options opts = {
                  .max_tokens = tc.max_tokens,
                };
                jobs.emplace_back(tc, s.local().generate_text(tc.prompt, opts));
            }
        }
        for (auto& [tc, job] : jobs) {
            std::string response = job.get();
            EXPECT_EQ(response, tc.response);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  KnownLlama2Responses,
  LLMTest,
  testing::ValuesIn<std::vector<test_data>>(testcases));

} // namespace ai
