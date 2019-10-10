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
// kWriteSize: Size of the data that we wish to store persistenly
// kBufferSize: The data is written to the start of a buffer of size kBufferSize
// kNumBuffers: We have kNumBuffers buffers of size kBufferSize
static_assert(kBufferSize >= kWriteSize, "");

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr size_t kNumIters = 1000000;
static constexpr size_t kTimerBatchSize = 1;
static constexpr bool kUsePmem = false;

int main() {
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");

  uint8_t *pbuf;
  size_t mapped_len;

  if (kUsePmem) {
    printf("Using persistent memory buffer\n");
    int is_pmem;
    pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr);
    rt_assert(mapped_len >= kBufferSize * kNumBuffers);
  } else {
    printf("Using DRAM buffer\n");
    pbuf = reinterpret_cast<uint8_t *>(malloc(kBufferSize * kNumBuffers));
  }

  size_t data[kBufferSize / sizeof(size_t)] = {0};
  for (size_t msr = 0; msr < 50; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      data[0]++;
      const size_t buffer_idx = i % kNumBuffers;
      pmem_memcpy_persist(&pbuf[buffer_idx * kBufferSize], data, kWriteSize);
    }

    double bench_seconds = sec_since(bench_start);
    printf("write size %zu, buffer size %zu, num_buffers %zu: %.2f M/s.\n",
           kWriteSize, kBufferSize, kNumBuffers,
           kNumIters / (bench_seconds * 1000000));
  }

  if (kUsePmem) pmem_unmap(pbuf, mapped_len);
}
