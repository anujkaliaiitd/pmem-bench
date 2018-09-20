#include <gtest/gtest.h>
#include "pmem_linear_probing.h"

TEST(Simple, One) { HashMap<size_t, size_t, 2ull> hashmap("/dev/dax0.0"); }

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
