#include <assert.h>
#include <gtest/gtest.h>
#include <map>
#include "phopscotch.h"

static constexpr size_t kDefaultFileOffset = 1024;
static constexpr const char *kPmemFile = "/mnt/pmem12/raft_log";

TEST(Basic, Simple) {
  size_t num_keys = 32;
  phopscotch::HashMap<size_t, size_t> hashmap(kPmemFile, kDefaultFileOffset,
                                              num_keys);

  size_t key, value;

  key = 1;
  value = 1;
  bool success = hashmap.set_nodrain(&key, &value);
  assert(success);

  key = 2;
  value = 2;
  success = hashmap.set_nodrain(&key, &value);
  assert(success);

  success = hashmap.set_nodrain(&key, &value);
  assert(success);

  key = 3;
  value = 3;
  success = hashmap.set_nodrain(&key, &value);
  assert(success);

  key = 1;
  value = 0;
  success = hashmap.get(&key, &value);
  assert(value == 1);
  assert(success);

  key = 2;
  value = 0;
  success = hashmap.get(&key, &value);
  assert(value == 2);
  assert(success);

  key = 4;
  value = 0;
  success = hashmap.get(&key, &value);
  assert(value == 0);
  assert(!success);
}

TEST(Basic, Overload) {
  size_t num_keys = 16384;
  phopscotch::HashMap<size_t, size_t> hashmap(kPmemFile, kDefaultFileOffset,
                                              num_keys);

  size_t max_key_inserted = 0;
  for (size_t i = 1; i <= num_keys; i++) {
    bool success = hashmap.set_nodrain(&i, &i);
    if (!success) break;

    max_key_inserted = i;
  }

  printf("Loaded fraction = %.2f\n", max_key_inserted * 1.0 / num_keys);

  for (size_t i = 1; i <= num_keys; i++) {
    size_t v;
    bool success = hashmap.get(&i, &v);
    assert(success == (i <= max_key_inserted));
    if (success) assert(v == i);
  }
}

TEST(Basic, Large) {
  phopscotch::HashMap<size_t, size_t> hashmap(kPmemFile, kDefaultFileOffset,
                                              (1ull << 30));

  size_t num_keys = 32;
  std::map<size_t, bool> insert_success_map;
  size_t num_success = 0;

  for (size_t i = 1; i <= num_keys; i++) {
    bool success = hashmap.set_nodrain(&i, &i);
    insert_success_map[i] = success;

    if (success) num_success++;
  }

  printf("Loaded fraction = %.2f\n", num_success * 1.0 / num_keys);

  for (size_t i = 1; i <= num_keys; i++) {
    size_t v;
    bool success = hashmap.get(&i, &v);
    assert(success == insert_success_map[i]);
    if (success) assert(v == i);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
