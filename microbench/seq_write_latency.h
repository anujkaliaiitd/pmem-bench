#include "../common.h"
#include "main.h"

void bench_seq_write_latency(uint8_t *pbuf) {
  static constexpr size_t kNumIters = 1000000;
  static constexpr size_t kMinWriteSz = 64;
  static constexpr size_t kMaxWriteSz = KB(64);

  size_t file_offset = 0;
  uint8_t *data = reinterpret_cast<uint8_t *>(memalign(4096, kMaxWriteSz));

  for (size_t msr = 0; msr < 10; msr++) {
    printf("size avg_ns\n");

    for (size_t size = kMinWriteSz; size <= kMaxWriteSz; size *= 2) {
      struct timespec start;
      clock_gettime(CLOCK_REALTIME, &start);

      for (size_t i = 0; i < kNumIters; i++) {
        pmem_memmove_persist(&pbuf[file_offset], data, size);

        file_offset += size;
        if (file_offset + size >= kPmemFileSize) file_offset = 0;
      }

      double ns = ns_since(start);
      printf("%zu %.1f\n", size, ns / kNumIters);
    }
  }
}
