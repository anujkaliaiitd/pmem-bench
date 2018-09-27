#include "../common.h"
#include "main.h"

void bench_rand_write_tput(uint8_t *pbuf, size_t thread_id, size_t copy_sz,
                           size_t num_threads) {
  static constexpr size_t kBatchSize = 8;
  static constexpr size_t kNumIters = MB(4);

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;

  auto *copy_arr = new uint8_t[copy_sz];
  for (size_t i = 0; i < copy_sz; i++) copy_arr[i] = pcg();

  for (size_t iter = 0; iter < 5; iter++) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      size_t offset[kBatchSize];
      for (size_t j = 0; j < kBatchSize; j++) {
        offset[j] = base_addr + (pcg() % bytes_per_thread);
        offset[j] = roundup<64>(offset[j]);
        if (offset[j] + copy_sz >= kPmemFileSize) {
          j--;
          continue;
        }
        pmem_memcpy_nodrain(&pbuf[offset[j]], copy_arr, copy_sz);
      }
      pmem_drain();
    }

    double tot_sec = sec_since(start);
    double rate = kNumIters / tot_sec;
    printf("Thread %zu of %zu, size %zu: random write tput = %.2f M/sec\n",
           thread_id, num_threads, copy_sz, rate / 1000000);
  }
}
