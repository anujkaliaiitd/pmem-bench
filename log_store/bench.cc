#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "../common.h"
#include "log.h"

static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr size_t kNumMeasurements = 2;
static constexpr size_t kNumIters = 1000000;

// Amount of data appended to the log in on iteration
static constexpr size_t kMaxLogDataSize = 4096;

void counter_only_bench(uint8_t *pbuf) {
  Counter ctr(pbuf, true /* create a new counter */);

  for (size_t msr = 0; msr < kNumMeasurements; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) ctr.increment_naive(1);

    double bench_seconds = sec_since(bench_start);
    printf("Naive counter: %.2f M increments/s\n",
           kNumIters / (bench_seconds * 1000000));
  }

  for (size_t msr = 0; msr < kNumMeasurements; msr++) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kNumIters; i++) ctr.increment_rotate(1);

    double bench_seconds = sec_since(bench_start);
    printf("Rotating counter: %.2f M increments/s\n",
           kNumIters / (bench_seconds * 1000000));
  }
}

void log_bench(uint8_t *pbuf) {
  uint8_t source[kMaxLogDataSize] = {0};

  printf("write_bytes naive_GBps rotating_GBps\n");

  // Sweep over write sizes
  for (size_t write_sz = 64; write_sz <= kMaxLogDataSize; write_sz *= 2) {
    double naive_GBps, rotating_GBps;

    {
      // Naive log
      Log log(pbuf);
      struct timespec bench_start;
      clock_gettime(CLOCK_REALTIME, &bench_start);

      for (size_t i = 0; i < kNumIters; i++) {
        // Modify the source
        for (size_t j = 0; j < write_sz / 64; j += 64) source[j]++;
        log.append_naive(source, write_sz);
      }

      double bench_seconds = sec_since(bench_start);
      naive_GBps = kNumIters * write_sz / (bench_seconds * GB(1));
    }

    {
      // Rotating log
      Log log(pbuf);
      struct timespec bench_start;
      clock_gettime(CLOCK_REALTIME, &bench_start);

      for (size_t i = 0; i < kNumIters; i++) {
        // Modify the source
        for (size_t j = 0; j < write_sz / 64; j += 64) source[j]++;
        log.append_rotating(source, write_sz);
      }

      double bench_seconds = sec_since(bench_start);
      rotating_GBps = kNumIters * write_sz / (bench_seconds * GB(1));
    }

    printf("%zu %.2f %.2f\n", write_sz, naive_GBps, rotating_GBps);
  }
}

int main() {
  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

  assert(pbuf != nullptr);
  assert(mapped_len >= Counter::get_reqd_space());

  counter_only_bench(pbuf);
  for (size_t msr = 0; msr < kNumMeasurements; msr++) log_bench(pbuf);

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
