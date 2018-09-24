#include <assert.h>
#include <gtest/gtest.h>
#include <map>
#include "mica_pmem.h"

TEST(Basic, Simple) {
  size_t num_keys = 32;
  mica::HashMap<size_t, size_t> hashmap("/dev/dax0.0", num_keys, 1.0);

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

TEST(Basic, Overload) {
  size_t num_keys = 32;
  mica::HashMap<size_t, size_t> hashmap("/dev/dax0.0", num_keys, 1.0);

  std::map<size_t, bool> insert_success_map;
  size_t num_success = 0;

  for (size_t i = 1; i <= num_keys; i++) {
    bool success = hashmap.set(i, i);
    insert_success_map[i] = success;

    if (success) num_success++;
  }

  printf("Loaded fraction = %.2f\n", num_success * 1.0 / num_keys);

  for (size_t i = 1; i <= num_keys; i++) {
    size_t v;
    bool success = hashmap.get(i, v);
    assert(success == insert_success_map[i]);
    if (success) assert(v == i);
  }
}

TEST(Basic, Large) {
  mica::HashMap<size_t, size_t> hashmap("/dev/dax0.0", GB(1), 0.2);

  size_t num_keys = 32;
  std::map<size_t, bool> insert_success_map;
  size_t num_success = 0;

  for (size_t i = 1; i <= num_keys; i++) {
    bool success = hashmap.set(i, i);
    insert_success_map[i] = success;

    if (success) num_success++;
  }

  printf("Loaded fraction = %.2f\n", num_success * 1.0 / num_keys);

  for (size_t i = 1; i <= num_keys; i++) {
    size_t v;
    bool success = hashmap.get(i, v);
    assert(success == insert_success_map[i]);
    if (success) assert(v == i);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
