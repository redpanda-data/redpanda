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
    inline static ai::model_id embd_id;
    inline static ai::model_id llm_id;

    static void SetUpTestSuite() {
        service::config service_config{
          .llm_path = "/tmp/ai/llm/",
          .embeddings_path = "/tmp/ai/embd/",
        };
        s.start(service_config).get();
        s.invoke_on_all(&ai::service::start).get();
        auto embd_file = huggingface_file{
          .repo = "leliuga/all-MiniLM-L12-v2-GGUF",
          .filename = "all-MiniLM-L12-v2.Q8_0.gguf",
        };
        // Tinyest LLM I could fine for testing. The results are awful.
        auto llm_file = huggingface_file{
          .repo = "Trelis/TinyLlama-1.1B-intermediate-step-480k-1T-GGUF",
          .filename = "TinyLlama-1.1B-intermediate-step-480k-1T.Q4_K.gguf",
        };
        auto models = s.local().list_models().get();
        for (const auto& info : models) {
            if (info.file == embd_file) {
                embd_id = info.id;
            }
            if (info.file == llm_file) {
                llm_id = info.id;
            }
        }
        if (embd_id == ai::model_id{}) {
            embd_id = s.local().deploy_embeddings_model(embd_file).get();
        }
        if (llm_id == ai::model_id{}) {
            llm_id = s.local().deploy_text_generation_model(llm_file).get();
        }
        std::cout << "embeddings id: " << embd_id << " llm_id: " << llm_id
                  << "\n";
    }
    static void TearDownTestSuite() { s.stop().get(); }
};

TEST_P(LLMTest, EmbeddingsSmoke) {
    const auto& param = GetParam();
    auto response = s.local()
                      .compute_embeddings(
                        embd_id, param.prompt + param.response)
                      .get();
    EXPECT_EQ(response.size(), 384);
}

TEST_P(LLMTest, LLMSmoke) {
    const auto& param = GetParam();
    auto response
      = s.local().generate_text(llm_id, param.prompt, {.max_tokens = 50}).get();
    EXPECT_EQ(std::string(response), param.response);
}

static const std::vector<test_data> testcases = {
  {
    .prompt = "My name is ",
    .response = ".\n\n  _Hamlet._\n\n  _Ham._\n\n  _Ham._\n\n  _Ham._\n\n  "
                "_Ham._\n\n  _Ham._\n\n  _Ham",
  },
  {
    .prompt = "My name is",
    .response = "matic. I'm a 100% human being. I'm a 100% human being. I'm a "
                "100% human being. I'm a 100% human being",
  },
  {
    .prompt = "What is the meaning of life?",
    .response = "ынээрэгэээээээээээээээээээээээээээээээээээээээээээ",
  },
  {
    .prompt = "Tell me an interesting fact about llamas.",
    .response = " interesting fact about llamas.\nLlamas are the only "
                "domesticated mammals that can produce their own milk.\nLlamas "
                "are the only domesticated mammals that can produce their own "
                "milk. Llamas are the only",
  },
  {
    .prompt = "Recommend some interesting books to read.",
    .response = "\nI have read some interesting books.\nI have read some "
                "interesting books.\nI have read some interesting books.\nI "
                "have read some interesting books.\nI have read some "
                "interesting books.\nI have read some interesting books.\nI",
  },
  {
    .prompt = "How to get a job at Google?",
    .response
    = "\xD0\xB3\xD0\xBE\xD0\xBD\xD0\xBD\xD0\xB0.\nHow to get a job at "
      "Google?\nHow to get a job at Google?\nHow to get a job at Google?\nHow "
      "to get a job at Google?\nHow to get a job at Google?\n",
  },
  {
    .prompt = "I want to learn how to play the piano.",
    .response
    = "a.\nI'm a 17 year old girl who loves to play the piano. I'm a very good "
      "student and I'm very happy to be here. I'm very happy to be here.\nI'",
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
        auto response = s.local()
                          .compute_embeddings(embd_id, tc.prompt + tc.response)
                          .get();
        expected.emplace(tc.response, response);
    }
    constexpr int iters = 5;
    for (int i = 0; i < iters; ++i) {
        std::vector<std::pair<test_data, ss::future<std::vector<float>>>> jobs;
        jobs.reserve(testcases.size() * iters);
        for (int j = 0; j < iters; ++j) {
            for (const auto& tc : testcases) {
                jobs.emplace_back(
                  tc,
                  s.local().compute_embeddings(
                    embd_id, tc.prompt + tc.response));
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
