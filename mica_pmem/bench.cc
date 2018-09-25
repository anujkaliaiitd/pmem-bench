#include <map>
#include "mica_pmem.h"

static constexpr size_t kDefaultFileOffset = 0;
mica::HashMap<size_t, size_t> *hashmap;

static constexpr size_t kNumTotalKeys = GB(1);
static constexpr size_t kNumProbeKeys = kNumTotalKeys * 0.8;

void batch_gets(size_t batch_size) {
  printf("GET experiment, batch size %zu\n", batch_size);

  struct timespec start;
  bool is_set_arr[mica::kMaxBatchSize];
  size_t key_arr[mica::kMaxBatchSize];
  size_t val_arr[mica::kMaxBatchSize];
  bool success_arr[mica::kMaxBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);

  size_t num_success = 0;
  for (size_t i = 1; i <= kNumProbeKeys; i += batch_size) {
    for (size_t j = 0; j < batch_size; j++) {
      is_set_arr[j] = false;
      key_arr[j] = i + j;
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            batch_size);

    for (size_t j = 0; j < batch_size; j++) {
      num_success += success_arr[j];
    }
  }

  double seconds = sec_since(start);
  printf("Batched GET perf (%zu per batch ) = %.2f M/s. Success rate = %.4f\n",
         batch_size, kNumProbeKeys / (seconds * 1000000),
         num_success * 1.0 / kNumProbeKeys);
}

void batch_sets(size_t batch_size) {
  printf("SET experiment, batch size %zu\n", batch_size);

  struct timespec start;
  bool is_set_arr[mica::kMaxBatchSize];
  size_t key_arr[mica::kMaxBatchSize];
  size_t val_arr[mica::kMaxBatchSize];
  bool success_arr[mica::kMaxBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);

  size_t num_success = 0;
  for (size_t i = 1; i <= kNumProbeKeys; i += batch_size) {
    for (size_t j = 0; j < batch_size; j++) {
      is_set_arr[j] = true;
      key_arr[j] = i + j;
      val_arr[j] = i + j;
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            batch_size);

    for (size_t j = 0; j < batch_size; j++) {
      num_success += success_arr[j];
    }
  }

  double seconds = sec_since(start);
  printf("Batched SET perf (%zu per batch ) = %.2f M/s. Success rate = %.4f\n",
         batch_size, kNumProbeKeys / (seconds * 1000000),
         num_success * 1.0 / kNumProbeKeys);
}

int main() {
  hashmap = new mica::HashMap<size_t, size_t>("/dev/dax0.0", kDefaultFileOffset,
                                              kNumTotalKeys, 0.2);

  batch_sets(16);
  batch_sets(8);
  batch_sets(1);

  batch_gets(1);
  batch_gets(8);
  batch_gets(16);

  delete hashmap;
}
