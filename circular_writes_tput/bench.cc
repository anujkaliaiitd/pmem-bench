#include <assert.h>
#include <gflags/gflags.h>
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
// FLAGS_num_counters: Number of counters emulating one counter
// FLAGS_stride_size: Distance between counters

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr size_t kNumIters = 1000000;
static constexpr bool kUsePmem = true;
static constexpr bool kUseNtStore = true;

DEFINE_uint64(num_counters, 16, "Number of counters to rotate on");
DEFINE_uint64(stride_size, 256, "Stride size");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");
  rt_assert(FLAGS_stride_size >= sizeof(size_t), "");
  rt_assert(FLAGS_stride_size % sizeof(size_t) == 0, "");

  uint8_t *pbuf;
  size_t mapped_len;

  if (kUsePmem) {
    printf("Using persistent memory buffer\n");
    int is_pmem;
    pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr);
    rt_assert(mapped_len >= FLAGS_num_counters * FLAGS_stride_size);
  } else {
    printf("Using DRAM buffer\n");
    pbuf = reinterpret_cast<uint8_t *>(
        malloc(FLAGS_num_counters * FLAGS_stride_size));
  }

  size_t counter_val = 1;
  size_t counter_idx = 0;
  for (size_t msr = 0; msr < 5; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) {
      size_t buffer_offset = counter_idx * FLAGS_stride_size;

      if (kUseNtStore) {
        pmem_memcpy_persist(&pbuf[buffer_offset], &counter_val, sizeof(size_t));
      } else {
        *reinterpret_cast<size_t *>(&pbuf[buffer_offset]) = counter_val;
        pmem_clwb(&pbuf[buffer_offset]);
        sfence();
      }

      counter_idx++;
      if (counter_idx == FLAGS_num_counters) counter_idx = 0;
      counter_val++;
    }

    printf("num_counters %zu, stride size %zu: %.2f M/s.\n", FLAGS_num_counters,
           FLAGS_stride_size, kNumIters / (sec_since(bench_start) * 1000000));
  }

  if (kUsePmem) pmem_unmap(pbuf, mapped_len);
}
