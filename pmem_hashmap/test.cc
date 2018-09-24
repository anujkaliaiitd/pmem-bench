#include <assert.h>
#include <gtest/gtest.h>
#include "mica_pmem.h"

TEST(InsertTest, Test) {
  mica::HashMap<size_t, size_t> hashmap("/dev/dax0.0", 1.0);
  bool success = hashmap.set(1, 1);
  assert(success);

  success = hashmap.set(1, 1);
  assert(success);

  success = hashmap.set(2, 2);
  assert(success);

  success = hashmap.set(3, 3);
  assert(success);

  size_t val = 0;
  success = hashmap.get(1, val);
  assert(val == 1);
  assert(success);

  success = hashmap.get(2, val);
  assert(val == 2);
  assert(success);

  success = hashmap.get(4, val);
  assert(val == 2);
  assert(!success);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
