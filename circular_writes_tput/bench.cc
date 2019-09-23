#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "config.h"

// Config parameters:
// kWriteSize: Size of the data that we wish to store persistenly
// kBufferSize: The data is written to the start of a buffer of size kBufferSize
// kNumBuffers: We have kNumBuffers buffers of size kBufferSize
static_assert(kBufferSize >= kWriteSize, "");

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr size_t kNumIters = 10000000;

int main() {
  if (getuid() != 0) {
    printf("You need to be root to run this benchmark\n");
    exit(-1);
  }

  size_t data[kBufferSize / sizeof(size_t)] = {0};

  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

  assert(pbuf != nullptr);
  assert(mapped_len >= kBufferSize * kNumBuffers);

  for (size_t msr = 0; msr < 5; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      data[0]++;
      const size_t buffer_idx = i % kNumBuffers;
      pmem_memcpy_persist(&pbuf[buffer_idx * kBufferSize], data, kWriteSize);
    }

    struct timespec bench_end;
    clock_gettime(CLOCK_REALTIME, &bench_end);
    double bench_seconds =
        (bench_end.tv_sec - bench_start.tv_sec) +
        (bench_end.tv_nsec - bench_start.tv_nsec) / 1000000000.0;

    printf("write size %zu, buffer size %zu, num_buffers %zu: %.2f M/s\n",
           kWriteSize, kBufferSize, kNumBuffers,
           kNumIters / (bench_seconds * 1000000));
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
