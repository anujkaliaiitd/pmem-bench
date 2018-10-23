#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "../common.h"
#include "log.h"

static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr size_t kNumIters = 1000000;

int main() {
  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

  assert(pbuf != nullptr);
  assert(mapped_len >= Counter::get_reqd_space());

  Counter ctr(pbuf, true /* create a new counter */);

  for (size_t msr = 0; msr < 5; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) ctr.increment_naive();

    double bench_seconds = sec_since(bench_start);
    printf("Naive: %.2f M increments/s\n",
           kNumIters / (bench_seconds * 1000000));
  }

  for (size_t msr = 0; msr < 5; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) ctr.increment_rotate_no_full_cl();

    double bench_seconds = sec_since(bench_start);
    printf("Rotating, no full-cacheline writes: %.2f M increments/s\n",
           kNumIters / (bench_seconds * 1000000));
  }

  for (size_t msr = 0; msr < 5; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) ctr.increment_rotate_full_cl();

    double bench_seconds = sec_since(bench_start);
    printf("Rotating, full-cacheline writes: %.2f M increments/s\n",
           kNumIters / (bench_seconds * 1000000));
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
