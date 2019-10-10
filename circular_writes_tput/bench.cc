#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include "../common.h"
#include "../utils/timer.h"
#include "config.h"

// Config parameters:
// kNumCounters: Number of counters emulating one counter
// kStrideSize: Distance between counters

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr size_t kNumIters = 1000000;
static constexpr bool kUsePmem = true;
static constexpr bool kUseNtStore = true;

int main() {
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");
  static_assert(kStrideSize >= sizeof(size_t), "");
  static_assert(kStrideSize % sizeof(size_t) == 0, "");

  uint8_t *pbuf;
  size_t mapped_len;

  if (kUsePmem) {
    printf("Using persistent memory buffer\n");
    int is_pmem;
    pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr);
    rt_assert(mapped_len >= kNumCounters * kStrideSize);
  } else {
    printf("Using DRAM buffer\n");
    pbuf = reinterpret_cast<uint8_t *>(malloc(kNumCounters * kStrideSize));
  }

  size_t count = 0;
  for (size_t msr = 0; msr < 300; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) {
      count++;
      size_t counter_idx = i % kNumCounters;
      size_t buffer_offset = counter_idx * kStrideSize;

      if (kUseNtStore) {
        pmem_memcpy_persist(&pbuf[buffer_offset], &count, sizeof(size_t));
      } else {
        *reinterpret_cast<size_t *>(&pbuf[buffer_offset]) = count;
        pmem_clwb(&pbuf[buffer_offset]);
        sfence();
      }
    }

    printf("num_counters %zu, stride size %zu: %.2f M/s.\n", kNumCounters,
           kStrideSize, kNumIters / (sec_since(bench_start) * 1000000));
  }

  if (kUsePmem) pmem_unmap(pbuf, mapped_len);
}
