#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main() {
  static constexpr size_t kNumIters = 1000000;
  static constexpr size_t kChunkSize = 256;
  static constexpr size_t kWriteSize = 64;
  static constexpr size_t kNumChunks = 8;
  const uint8_t data[kChunkSize] = {0};

  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/raft_log", 0, 0, 0666, &mapped_len, &is_pmem));

  assert(pbuf != nullptr);
  assert(mapped_len >= kChunkSize * kNumChunks);

  while (true) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      const size_t chunk_idx = i % kNumChunks;
      pmem_memcpy_persist(&pbuf[chunk_idx * kChunkSize], data, kWriteSize);
    }

    struct timespec bench_end;
    clock_gettime(CLOCK_REALTIME, &bench_end);
    double bench_seconds =
        (bench_end.tv_sec - bench_start.tv_sec) +
        (bench_end.tv_nsec - bench_start.tv_nsec) / 1000000000.0;

    printf("Throughput of writes with %zu chunks = %.2f M/s\n", kNumChunks,
           kNumIters / (bench_seconds * 1000000));
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
