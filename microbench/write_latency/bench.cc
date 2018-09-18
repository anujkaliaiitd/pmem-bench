#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../../common.h"

static constexpr size_t kWriteSize = 512;
static constexpr size_t kNumIters = 1000000;

static constexpr size_t kMinAEPLatCycles = 1;
static constexpr size_t kMaxAEPLatCycles = MB(1024);
static constexpr size_t kAEPLatPrecision = 2;

int main() {
  const uint8_t data[kWriteSize] = {0};

  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/dev/dax0.0", 0, 0, 0666, &mapped_len, &is_pmem));

  assert(pbuf != nullptr);
  assert(mapped_len >= kWriteSize * kNumIters);

  const double freq_ghz = measure_rdtsc_freq();
  HdrHistogram hist(kMinAEPLatCycles, kMaxAEPLatCycles, kAEPLatPrecision);
  size_t offset = 0;

  for (size_t msr = 0; msr < 10; msr++) {
    hist.reset();
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      size_t start_tsc = rdtscp();
      mfence();

      pmem_memcpy_persist(&pbuf[offset], data, kWriteSize);

      mfence();
      hist.record_value(rdtscp() - start_tsc);

      offset += kWriteSize;
      if (offset + kWriteSize >= mapped_len) offset = 0;
    }

    const double bench_seconds = sec_since(bench_start);
    printf("Throughput of writes = %.2f M ops/s, %.2f GB/s\n",
           kNumIters / (bench_seconds * 1000000),
           kNumIters * kWriteSize / (bench_seconds * 1000000000));
    printf("Latency (ns): 50 %.1f, 99 %.1f, 99.9 %.1f\n",
           hist.percentile(50) / freq_ghz, hist.percentile(99) / freq_ghz,
           hist.percentile(99.9) / freq_ghz);
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
