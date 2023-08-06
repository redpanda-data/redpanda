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
#pragma once

#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <gtest/gtest.h>

class seastar_test_mixin {
public:
    static void init_seastar_test_runner();
    static void run(std::function<seastar::future<>()> task);
    static void run_async(std::function<void()> task);
};

#define GTEST_TEST_ASYNC_(test_suite_name, test_name, parent_class, parent_id) \
    static_assert(                                                             \
      sizeof(GTEST_STRINGIFY_(test_suite_name)) > 1,                           \
      "test_suite_name must not be empty");                                    \
    static_assert(                                                             \
      sizeof(GTEST_STRINGIFY_(test_name)) > 1, "test_name must not be empty"); \
    class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                   \
      : public seastar_test_mixin                                              \
      , public parent_class {                                                  \
    public:                                                                    \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() = default;        \
        ~GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() override         \
          = default;                                                           \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                     \
        (const GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &) = delete; \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &                   \
        operator=(const GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &)  \
          = delete; /* NOLINT */                                               \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                     \
        (GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &&) noexcept       \
          = delete;                                                            \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) & operator=(        \
          GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &&) noexcept      \
          = delete; /* NOLINT */                                               \
                                                                               \
    private:                                                                   \
        void TestBody() override {                                             \
            run_async([this] { TestBodyWrapped(); });                          \
        }                                                                      \
        void TestBodyWrapped();                                                \
        static ::testing::TestInfo* const test_info_ GTEST_ATTRIBUTE_UNUSED_;  \
    };                                                                         \
                                                                               \
    ::testing::TestInfo* const GTEST_TEST_CLASS_NAME_(                         \
      test_suite_name, test_name)::test_info_                                  \
      = ::testing::internal::MakeAndRegisterTestInfo(                          \
        #test_suite_name,                                                      \
        #test_name,                                                            \
        nullptr,                                                               \
        nullptr,                                                               \
        ::testing::internal::CodeLocation(__FILE__, __LINE__),                 \
        (parent_id),                                                           \
        ::testing::internal::SuiteApiResolver<                                 \
          parent_class>::GetSetUpCaseOrSuite(__FILE__, __LINE__),              \
        ::testing::internal::SuiteApiResolver<                                 \
          parent_class>::GetTearDownCaseOrSuite(__FILE__, __LINE__),           \
        new ::testing::internal::TestFactoryImpl<GTEST_TEST_CLASS_NAME_(       \
          test_suite_name, test_name)>);                                       \
    void GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::TestBodyWrapped()

#define TEST_ASYNC(test_suite_name, test_name)                                 \
    GTEST_TEST_ASYNC_(                                                         \
      test_suite_name,                                                         \
      test_name,                                                               \
      ::testing::Test,                                                         \
      ::testing::internal::GetTestTypeId())

#define TEST_F_ASYNC(test_fixture, test_name)                                  \
    GTEST_TEST_ASYNC_(                                                         \
      test_fixture,                                                            \
      test_name,                                                               \
      test_fixture,                                                            \
      ::testing::internal::GetTypeId<test_fixture>())

#define TEST_P_ASYNC(test_suite_name, test_name)                               \
    class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                   \
      : public seastar_test_mixin                                              \
      , public test_suite_name {                                               \
    public:                                                                    \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() {}                \
        void TestBody() override {                                             \
            run_async([this] { TestBodyWrapped(); });                          \
        }                                                                      \
        void TestBodyWrapped();                                                \
                                                                               \
    private:                                                                   \
        static int AddToRegistry() {                                           \
            ::testing::UnitTest::GetInstance()                                 \
              ->parameterized_test_registry()                                  \
              .GetTestSuitePatternHolder<test_suite_name>(                     \
                GTEST_STRINGIFY_(test_suite_name),                             \
                ::testing::internal::CodeLocation(__FILE__, __LINE__))         \
              ->AddTestPattern(                                                \
                GTEST_STRINGIFY_(test_suite_name),                             \
                GTEST_STRINGIFY_(test_name),                                   \
                new ::testing::internal::TestMetaFactory<                      \
                  GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)>(),       \
                ::testing::internal::CodeLocation(__FILE__, __LINE__));        \
            return 0;                                                          \
        }                                                                      \
        static int gtest_registering_dummy_ GTEST_ATTRIBUTE_UNUSED_;           \
        GTEST_DISALLOW_COPY_AND_ASSIGN_(                                       \
          GTEST_TEST_CLASS_NAME_(test_suite_name, test_name));                 \
    };                                                                         \
    int GTEST_TEST_CLASS_NAME_(                                                \
      test_suite_name, test_name)::gtest_registering_dummy_                    \
      = GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::AddToRegistry();   \
    void GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::TestBodyWrapped()
