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

#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <gtest/gtest.h>

class seastar_test : public ::testing::Test {
public:
    virtual seastar::future<> SetUpAsync() {
        return seastar::make_ready_future<>();
    }
    virtual seastar::future<> TearDownAsync() {
        return seastar::make_ready_future<>();
    }

private:
    void SetUp() override { SetUpAsync().get(); }
    void TearDown() override { TearDownAsync().get(); }
};

// NOLINTBEGIN(cppcoreguidelines-macro-usage,bugprone-macro-parentheses)
#define GTEST_TEST_SEASTAR_(                                                   \
  test_suite_name, test_name, parent_class, parent_id)                         \
    static_assert(                                                             \
      sizeof(GTEST_STRINGIFY_(test_suite_name)) > 1,                           \
      "test_suite_name must not be empty");                                    \
    static_assert(                                                             \
      sizeof(GTEST_STRINGIFY_(test_name)) > 1, "test_name must not be empty"); \
    static_assert(                                                             \
      std::is_base_of_v<seastar_test, parent_class>, "wrong base");            \
    class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                   \
      : public parent_class {                                                  \
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
        void TestBody() override { TestBodyWrapped().get(); }                  \
        seastar::future<> TestBodyWrapped();                                   \
        static ::testing::TestInfo* const test_info_ [[maybe_unused]];         \
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
    seastar::future<> GTEST_TEST_CLASS_NAME_(                                  \
      test_suite_name, test_name)::TestBodyWrapped()

#define TEST_CORO(test_suite_name, test_name)                                  \
    GTEST_TEST_SEASTAR_(                                                       \
      test_suite_name,                                                         \
      test_name,                                                               \
      seastar_test,                                                            \
      ::testing::internal::GetTestTypeId())

#define TEST_F_CORO(test_fixture, test_name)                                   \
    GTEST_TEST_SEASTAR_(                                                       \
      test_fixture,                                                            \
      test_name,                                                               \
      test_fixture,                                                            \
      ::testing::internal::GetTypeId<test_fixture>())

#define TEST_P_SEASTAR_(test_suite_name, test_name)                            \
    static_assert(                                                             \
      std::is_base_of_v<seastar_test, test_suite_name>, "wrong base");         \
    class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                   \
      : public test_suite_name {                                               \
    public:                                                                    \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() {}                \
        void TestBody() override { TestBodyWrapped().get(); }                  \
        seastar::future<> TestBodyWrapped();                                   \
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
        static int gtest_registering_dummy_ [[maybe_unused]];                  \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                     \
        (GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) const&) = delete;  \
        GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) &                   \
        operator=(GTEST_TEST_CLASS_NAME_(test_suite_name, test_name) const&)   \
          = delete;                                                            \
    };                                                                         \
    int GTEST_TEST_CLASS_NAME_(                                                \
      test_suite_name, test_name)::gtest_registering_dummy_                    \
      = GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::AddToRegistry();   \
    seastar::future<> GTEST_TEST_CLASS_NAME_(                                  \
      test_suite_name, test_name)::TestBodyWrapped()

#define TEST_P_CORO(test_suite_name, test_name)                                \
    TEST_P_SEASTAR_(test_suite_name, test_name)

/*
 * Support for coroutine safe assertions
 */
#define GTEST_FATAL_FAILURE_CORO_(message)                                     \
    co_return GTEST_MESSAGE_(message, ::testing::TestPartResult::kFatalFailure)
#define GTEST_SKIP_CORO_(message)                                              \
    co_return GTEST_MESSAGE_(message, ::testing::TestPartResult::kSkip)
#define ASSERT_PRED_FORMAT2_CORO(pred_format, v1, v2)                          \
    GTEST_PRED_FORMAT2_(pred_format, v1, v2, GTEST_FATAL_FAILURE_CORO_)
#define GTEST_ASSERT_EQ_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::EqHelper::Compare, val1, val2)
#define GTEST_ASSERT_GT_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::CmpHelperGT, val1, val2)
#define GTEST_ASSERT_GE_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::CmpHelperGE, val1, val2)
#define GTEST_ASSERT_LT_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::CmpHelperLT, val1, val2)
#define GTEST_ASSERT_LE_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::CmpHelperLE, val1, val2)
#define GTEST_ASSERT_NE_CORO(val1, val2)                                       \
    ASSERT_PRED_FORMAT2_CORO(::testing::internal::CmpHelperNE, val1, val2)
#define GTEST_TEST_THROW_CORO(statement, expected_exception)                   \
    GTEST_TEST_THROW_(statement, expected_exception, GTEST_FATAL_FAILURE_CORO_)
#define GTEST_TEST_NO_THROW_CORO(statement)                                    \
    GTEST_TEST_NO_THROW_(statement, GTEST_FATAL_FAILURE_CORO_)
#define ASSERT_NO_FATAL_FAILURE_CORO(statement)                                \
    GTEST_TEST_NO_FATAL_FAILURE_(statement, GTEST_FATAL_FAILURE_CORO_)
/*
 * Coroutine safe assertions
 */
#define ASSERT_TRUE_CORO(condition)                                            \
    GTEST_TEST_BOOLEAN_(                                                       \
      condition, #condition, false, true, GTEST_FATAL_FAILURE_CORO_)
#define ASSERT_FALSE_CORO(condition)                                           \
    GTEST_TEST_BOOLEAN_(                                                       \
      !(condition), #condition, true, false, GTEST_FATAL_FAILURE_CORO_)

#define ASSERT_EQ_CORO(val1, val2) GTEST_ASSERT_EQ_CORO(val1, val2)
#define ASSERT_GT_CORO(val1, val2) GTEST_ASSERT_GT_CORO(val1, val2)
#define ASSERT_GE_CORO(val1, val2) GTEST_ASSERT_GE_CORO(val1, val2)
#define ASSERT_LT_CORO(val1, val2) GTEST_ASSERT_LT_CORO(val1, val2)
#define ASSERT_LE_CORO(val1, val2) GTEST_ASSERT_LE_CORO(val1, val2)
#define ASSERT_NE_CORO(val1, val2) GTEST_ASSERT_NE_CORO(val1, val2)

#define ASSERT_RESULT_EQ_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_EQ_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);
#define ASSERT_RESULT_GT_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_GT_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);
#define ASSERT_RESULT_GE_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_GE_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);
#define ASSERT_RESULT_LT_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_LT_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);
#define ASSERT_RESULT_LE_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_LE_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);
#define ASSERT_RESULT_NE_CORO(result_val, exp_naked_value)                     \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        auto result_val_copy = (result_val);                                   \
        ASSERT_TRUE_CORO(result_val_copy);                                     \
        GTEST_ASSERT_NE_CORO(result_val_copy.assume_value(), exp_naked_value); \
    } while (0);

#define ASSERT_THROW_CORO(statement, expected_exception)                       \
    GTEST_TEST_THROW_CORO(statement, expected_exception)

#define ASSERT_NO_THROW_CORO(statement) GTEST_TEST_NO_THROW_CORO(statement)

#define GTEST_SKIP_CORO() GTEST_SKIP_CORO_("")

// NOLINTEND(cppcoreguidelines-macro-usage,bugprone-macro-parentheses)
