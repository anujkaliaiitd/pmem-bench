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

  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(kNumIters / kTimerBatchSize);
  double rtdsc_freq = measure_rdtsc_freq();

  for (size_t msr = 0; msr < 50; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      // lfence();
      // sfence();
      // mfence();
      uint64_t timer_start = rdtscp();

      data[0]++;
      const size_t buffer_idx = i % kNumBuffers;
      pmem_memcpy_persist(&pbuf[buffer_idx * kBufferSize], data, kWriteSize);

      // lfence();
      // sfence();
      // mfence();
      lat_vec.push_back(rdtscp() - timer_start);
    }

    struct timespec bench_end;
    clock_gettime(CLOCK_REALTIME, &bench_end);
    double bench_seconds =
        (bench_end.tv_sec - bench_start.tv_sec) +
        (bench_end.tv_nsec - bench_start.tv_nsec) / 1000000000.0;

    if (lat_vec.size() != kNumIters) {
      printf("lat vec size %zu, num iters %zu\n", lat_vec.size(), kNumIters);
    }

    std::sort(lat_vec.begin(), lat_vec.end());
    for (auto &t : lat_vec) t = t / rtdsc_freq;  // Convert to nanoseconds
    size_t mean_ns =
        std::accumulate(lat_vec.begin(), lat_vec.end(), 0.0) / lat_vec.size();

    printf("clock_gettime seconds = %.2f, rdtsc accumulated seconds = %.2f\n",
           bench_seconds,
           std::accumulate(lat_vec.begin(), lat_vec.end(), 0.0) / rtdsc_freq);

    printf(
        "write size %zu, buffer size %zu, num_buffers %zu: %.2f M/s. "
        "mean %zu, median %zu,[5th %zu, 95th %zu, 99th %zu, 99.9th %zu, "
        "99.99th %zu, 99.999th %zu], max %zu, freq = %.2f\n",
        kWriteSize, kBufferSize, kNumBuffers,
        kNumIters / (bench_seconds * 1000000), mean_ns,
        lat_vec.at(lat_vec.size() * 0.5), lat_vec.at(lat_vec.size() * 0.05),
        lat_vec.at(lat_vec.size() * 0.95), lat_vec.at(lat_vec.size() * 0.99),
        lat_vec.at(lat_vec.size() * 0.999), lat_vec.at(lat_vec.size() * 0.9999),
        lat_vec.at(lat_vec.size() * 0.99999), lat_vec.back(), rtdsc_freq);

    lat_vec.clear();
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
