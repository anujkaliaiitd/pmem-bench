#include <gtest/gtest.h>
#include "pmem_linear_probing.h"

TEST(InsertTest, Test) {
  HashMap<size_t, size_t, 2> hashmap("/dev/dax0.0");
  bool success = hashmap.insert(1, 1);
  ASSERT_TRUE(success);

  success = hashmap.insert(1, 1);
  ASSERT_TRUE(success);

  success = hashmap.insert(2, 2);
  ASSERT_TRUE(success);

  success = hashmap.insert(3, 3);
  ASSERT_FALSE(success);

  size_t val = 0;
  success = hashmap.get(1, val);
  ASSERT_EQ(val, 1);
  ASSERT_TRUE(success);

  success = hashmap.get(2, val);
  ASSERT_EQ(val, 2);
  ASSERT_TRUE(success);

  success = hashmap.get(3, val);
  ASSERT_EQ(val, 2);
  ASSERT_FALSE(success);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
