#include "gtest/gtest.h"
#include "test_utils/gtest_utils.h"

#include <gtest/gtest.h>

/// Helper function to print and error message and exit with code 1.
/// We can't use gtest assertions because the way test is set up is meant to
/// fail and we hide that failure.
void fail_test(const std::string_view msg) {
    std::cout << testing::internal::GetCapturedStdout() << std::endl;
    fmt::print("FAILURE: {}\n", msg);
    exit(1);
}

void helper_asserts_false() {
    ASSERT_FALSE(true) << "Expected failure. Aye aye!";
}

TEST(AssertInHelper, AssertInHelperThrows) {
    try {
        helper_asserts_false();
        fail_test("Expected exception to be thrown above and this line to not "
                  "be executed.");
    } catch (const testing::AssertionException& e) {
        if (strstr(e.what(), "Expected failure. Aye aye!") == nullptr) {
            fail_test(
              "Expected message to contain 'Expected failure. Aye aye!'");
        }
        throw;
    }
}

/// In this test we want to check that the listener intercepts asserts and
/// throws exception resulting in not just test failure but test termination.
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    auto& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new rp_test_listener());

    // Hide stdout to avoid confusing users.
    testing::internal::CaptureStdout();

    int result = RUN_ALL_TESTS();
    if (result == 0) {
        fail_test("Expected the test to fail but it passed.");
    }

    return 0;
}
