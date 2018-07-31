#include <errno.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../common.h"

static constexpr size_t kFileSizeGB = 64;  // The expected file size
static constexpr size_t kNumIters = 1000000;

static constexpr size_t kFileSizeBytes = kFileSizeGB * GB(1);

/// Latency of random reads
void bench_rand_read_lat(uint8_t *pbuf) {
  FastRand rand;
  struct timespec start, end;
  size_t sum = 0;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kNumIters; i++) {
    // Choose a random byte
    size_t rand_byte = ((sum % 8) + static_cast<size_t>(rand.next_u32())) * 64;

    sum += pbuf[rand_byte % kFileSizeBytes];
  }

  clock_gettime(CLOCK_REALTIME, &end);
  double tot_ns = (end.tv_sec - start.tv_sec) * 1000000000.0 +
                  (end.tv_nsec - start.tv_nsec);

  printf("Latency of random reads = %.2f ns. Sum = %zu\n", tot_ns / kNumIters,
         sum);
}

/// Latency of persisting to the same byte in a file. Useful for timestamps etc.
void bench_write_lat_byte(uint8_t *pbuf) {
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kNumIters; i++) {
    pbuf[0] = 'A';
    pmem_persist(pbuf, 1);
  }

  clock_gettime(CLOCK_REALTIME, &end);
  double tot_ns = (end.tv_sec - start.tv_sec) * 1000000000.0 +
                  (end.tv_nsec - start.tv_nsec);

  printf("Latency of persistent writes to same byte = %.2f ns\n",
         tot_ns / kNumIters);
}

/// Bandwidth of large writes
void bench_write_sequential(uint8_t *pbuf) {
  void *dram_src_buf = malloc(kFileSizeBytes);
  rt_assert(dram_src_buf != nullptr);

  for (size_t copy_GB = 1; copy_GB <= 8; copy_GB *= 2) {
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(pbuf, dram_src_buf, copy_GB * GB(1));
    clock_gettime(CLOCK_REALTIME, &end);

    double tot_sec = (end.tv_sec - start.tv_sec) +
                     (end.tv_nsec - start.tv_nsec) / 1000000000.0;

    printf("Bandwidth of persistent writes (%zu GB) = %.2f GB/s\n", copy_GB,
           copy_GB / tot_sec);
  }

  free(dram_src_buf);
}

int main() {
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/src.txt", 0 /* length */, 0 /* flags */, 0666,
                    &mapped_len, &is_pmem));

  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len == kFileSizeGB * GB(1),
            "Incorrect file size " + std::to_string(mapped_len));
  rt_assert(is_pmem == 1, "File is not pmem");

  printf("Warming up for around 1 second.\n");
  nano_sleep(1000000000, 3.0);  // Assume TSC frequency = 3 GHz

  bench_rand_read_lat(pbuf);
  bench_write_lat_byte(pbuf);
  bench_write_sequential(pbuf);

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
