#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/// Return seconds elapsed since timestamp \p t0
static double sec_since(const struct timespec &t0) {
  struct timespec t1;
  clock_gettime(CLOCK_REALTIME, &t1);
  return (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1000000000.0;
}

int main() {
  static constexpr size_t kNumIters = 1000000;
  static constexpr size_t kChunkSize = 64;
  static constexpr size_t kNumChunks = 32;
  const uint8_t data[kChunkSize] = {0};

  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/raft_log", 0, 0, 0666, &mapped_len, &is_pmem));

  if (pbuf == nullptr) {
    fprintf(stderr, "/mnt/pmem12/raft_log does not exist\n");
    exit(-1);
  }

  if (mapped_len < kChunkSize * kNumChunks) {
    fprintf(stderr, "File too small\n");
    exit(-1);
  }

  while (true) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      size_t chunk_idx = i % kNumChunks;
      pmem_memcpy_persist(&pbuf[chunk_idx * kChunkSize], data, kChunkSize);
    }

    double bench_seconds = sec_since(bench_start);
    printf("Througput of writes circular buffer chunks = %.2f M/s\n",
           kNumIters / (bench_seconds * 1000000));
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
