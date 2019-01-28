#include <utility>

#include <gtest/gtest.h>

#include "tagged_ptr.h"

static const char *kDummyPayload = "Let's See about this tagged_ptr";

TEST(tagged_ptr, basic) {
  rp::tagged_ptr<const char> tptr(kDummyPayload, 42);
  // equate the ptrs
  ASSERT_EQ(kDummyPayload, tptr.get_ptr());

  // equate the tag
  ASSERT_EQ(42, tptr.get_tag());
}

TEST(tagged_ptr, increment) {
  rp::tagged_ptr<const char> tptr(kDummyPayload, 42);
  ASSERT_EQ(kDummyPayload, tptr.get_ptr());
  ASSERT_EQ(42, tptr.get_tag());
  for (auto i = 0; i < 10; ++i) {
    // set the increment the tag
    tptr.set_tag(tptr.get_tag() + 1);
  }
  ASSERT_EQ(kDummyPayload, tptr.get_ptr());
  ASSERT_EQ(52, tptr.get_tag());
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
