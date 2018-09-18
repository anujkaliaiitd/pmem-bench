#include <assert.h>
#include <libpmem.h>
#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <algorithm>
#include <vector>
#include "../../common.h"

static constexpr size_t kWriteSize = 512;
static constexpr size_t kNumIters = 1000000;

int main() {
  uint8_t *data = reinterpret_cast<uint8_t *>(memalign(4096, kWriteSize));

  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/dev/dax0.0", 0, 0, 0666, &mapped_len, &is_pmem));
  assert(pbuf != nullptr);
  assert(mapped_len >= kWriteSize * kNumIters);

  size_t file_offset = 0;
  std::vector<size_t> latency_vec;
  latency_vec.reserve(kNumIters);

  for (size_t msr = 0; msr < 10; msr++) {
    // Initialize measurement
    latency_vec.clear();
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      size_t start_tsc = rdtsc();
      mfence();
      pmem_memmove_persist(&pbuf[file_offset], data, kWriteSize);
      mfence();

      latency_vec.push_back(rdtsc() - start_tsc);

      file_offset += kWriteSize;
      if (file_offset + kWriteSize >= mapped_len) file_offset = 0;
    }

    double bench_seconds = sec_since(bench_start);
    printf("Throughput of writes = %.2f M ops/s, %.2f GB/s\n",
           kNumIters / (bench_seconds * 1000000),
           kNumIters * kWriteSize / (bench_seconds * 1000000000));

    std::sort(latency_vec.begin(), latency_vec.end());
    printf("Latency (cycles): median %zu, 99%% %zu, 99.9%% %zu\n",
           latency_vec.at(kNumIters * .5), latency_vec.at(kNumIters * .99),
           latency_vec.at(kNumIters * .999));
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
