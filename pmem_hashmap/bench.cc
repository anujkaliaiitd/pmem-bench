#include <map>
#include "mica_pmem.h"

int main() {
  mica::HashMap<size_t, size_t> hashmap("/dev/dax0.0", GB(1), 0.2);

  size_t num_keys = MB(8);

  // SET
  printf("SET experiment\n");
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);
  size_t num_success = 0;
  for (size_t i = 1; i <= num_keys; i++) {
    num_success += (hashmap.set(i, i) == true);
  }
  double seconds = sec_since(start);

  printf("SET perf = %.2f M/s. Success percent = %.4f\n",
         num_keys / (seconds * 1000000), num_success * 1.0 / num_keys);

  // GET
  printf("GET experiment\n");
  clock_gettime(CLOCK_REALTIME, &start);
  num_success = 0;
  for (size_t i = 1; i <= num_keys; i++) {
    size_t v;
    num_success += hashmap.get(i, v);
  }
  seconds = sec_since(start);
  printf("GET perf = %.2f M/s. Success percent = %.4f\n",
         num_keys / (seconds * 1000000), num_success * 1.0 / num_keys);

  // Batched GET
  printf("Batched GET experiment\n");
  static constexpr size_t kBatchSize = 10;
  size_t key_arr[kBatchSize];
  size_t val_arr[kBatchSize];
  bool success_arr[kBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);
  num_success = 0;
  for (size_t i = 1; i <= num_keys; i += kBatchSize) {
    for (size_t j = 0; j < kBatchSize; j++) {
      key_arr[j] = i + j;
    }

    hashmap.get(key_arr, val_arr, success_arr, kBatchSize);
    for (size_t j = 0; j < kBatchSize; j++) {
      num_success += success_arr[j];
    }
  }

  seconds = sec_since(start);
  printf("Batched GET perf = %.2f M/s. Success percent = %.4f\n",
         num_keys / (seconds * 1000000), num_success * 1.0 / num_keys);
}
