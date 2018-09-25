#include <map>
#include <pcg/pcg_random.hpp>
#include "mica_pmem.h"

// Other pmem benchmarks use the first terabyte on intel-1. Beyond 1 TB, we
// save the hash map so that we don't have to populate it repeatedly.
static constexpr size_t kDefaultFileOffset = GB(1024);
static constexpr size_t kTableKeyCapacity = MB(4);

// MICA's ``small'' workload: 16-byte keys and 64-byte values
class Key {
 public:
  size_t key_frag[2];
  bool operator==(const Key &rhs) { return memcmp(this, &rhs, sizeof(Key)); }
  bool operator!=(const Key &rhs) { return !memcmp(this, &rhs, sizeof(Key)); }
  Key() { memset(key_frag, 0, sizeof(Key)); }
};

class Value {
 public:
  size_t val_frag[8];
  Value() { memset(val_frag, 0, sizeof(Value)); }
};

/// Given a random number \p rand, return a random number
static inline uint64_t fastrange64(uint64_t rand, uint64_t n) {
  return static_cast<uint64_t>(
      static_cast<__uint128_t>(rand) * static_cast<__uint128_t>(n) >> 64);
}

typedef mica::HashMap<Key, Value> HashMap;

void batch_gets(HashMap *hashmap, size_t max_key, size_t batch_size) {
  printf("GET experiment, batch size %zu\n", batch_size);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  constexpr size_t kNumIters = MB(1);

  struct timespec start;
  bool is_set_arr[mica::kMaxBatchSize];
  Key key_arr[mica::kMaxBatchSize];
  Value val_arr[mica::kMaxBatchSize];
  bool success_arr[mica::kMaxBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);

  size_t num_success = 0;
  for (size_t i = 1; i <= kNumIters; i += batch_size) {
    for (size_t j = 0; j < batch_size; j++) {
      is_set_arr[j] = false;
      key_arr[j].key_frag[0] = fastrange64(pcg(), max_key);
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            batch_size);

    for (size_t j = 0; j < batch_size; j++) num_success += success_arr[j];
  }

  double seconds = sec_since(start);
  printf("Batched GET perf (%zu per batch ) = %.2f M/s. Success rate = %.4f\n",
         batch_size, kNumIters / (seconds * 1000000),
         num_success * 1.0 / kNumIters);
}

size_t populate(HashMap *hashmap) {
  printf("Populating hashmap. Expected time = %.1f seconds\n",
         kTableKeyCapacity / (4.0 * 1000000));  // 4 M/s inserts

  bool is_set_arr[mica::kMaxBatchSize];
  Key key_arr[mica::kMaxBatchSize];
  Value val_arr[mica::kMaxBatchSize];
  bool success_arr[mica::kMaxBatchSize];

  size_t num_success = 0;
  for (size_t i = 1; i <= kTableKeyCapacity; i += mica::kMaxBatchSize) {
    for (size_t j = 0; j < mica::kMaxBatchSize; j++) {
      is_set_arr[j] = true;
      key_arr[j].key_frag[0] = i + j;
      val_arr[j].val_frag[0] = i + j;
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            mica::kMaxBatchSize);

    for (size_t j = 0; j < mica::kMaxBatchSize; j++) {
      num_success += success_arr[j];
      if (!success_arr[j]) {
        printf("populate: Failed at key %zu of %zu. Fill fraction = %.2f\n", i,
               kTableKeyCapacity, i * 1.0 / hashmap->get_key_capacity());
        return num_success;
      }
    }
  }

  printf("populate: Added all keys. Fill fraction = %.2f\n",
         kTableKeyCapacity * 1.0 / hashmap->get_key_capacity());
  return kTableKeyCapacity;  // All keys were added
}

void batch_sets(HashMap *hashmap, size_t max_key, size_t batch_size) {
  printf("SET experiment, batch size %zu\n", batch_size);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  constexpr size_t kNumIters = MB(1);

  struct timespec start;
  bool is_set_arr[mica::kMaxBatchSize];
  Key key_arr[mica::kMaxBatchSize];
  Value val_arr[mica::kMaxBatchSize];
  bool success_arr[mica::kMaxBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);

  size_t num_success = 0;
  for (size_t i = 1; i <= kNumIters; i += batch_size) {
    for (size_t j = 0; j < batch_size; j++) {
      is_set_arr[j] = true;
      key_arr[j].key_frag[0] = fastrange64(pcg(), max_key);
      val_arr[j].val_frag[0] = key_arr[j].key_frag[0];
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            batch_size);

    for (size_t j = 0; j < batch_size; j++) num_success += success_arr[j];
  }

  double seconds = sec_since(start);
  printf("Batched SET perf (%zu per batch ) = %.2f M/s. Success rate = %.4f\n",
         batch_size, kNumIters / (seconds * 1000000),
         num_success * 1.0 / kNumIters);
}

int main() {
  auto *hashmap =
      new HashMap("/dev/dax0.0", kDefaultFileOffset, kTableKeyCapacity, 0.2);
  size_t max_key = populate(hashmap);

  batch_sets(hashmap, max_key, 16);
  batch_sets(hashmap, max_key, 8);
  batch_sets(hashmap, max_key, 1);

  batch_gets(hashmap, max_key, 1);
  batch_gets(hashmap, max_key, 8);
  batch_gets(hashmap, max_key, 16);

  delete hashmap;
}
