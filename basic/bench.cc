#include <errno.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <thread>
#include "../common.h"

DEFINE_uint64(num_threads, 0, "Number of threads");

static constexpr size_t kFileSizeGB = 1024;  // The expected file size
static constexpr size_t kNumIters = 1000000;

static constexpr size_t kFileSizeBytes = kFileSizeGB * GB(1);

double tsc_freq = 0.0;

size_t get_dependent_rand_addr(size_t sum, FastRand &rand) {
  size_t rand_addr = ((sum % 8) + static_cast<size_t>(rand.next_u32())) * 64;
  return rand_addr % kFileSizeBytes;
}

size_t get_independent_rand_addr(FastRand &rand) {
  size_t rand_addr = static_cast<size_t>(rand.next_u32()) * 64;
  return rand_addr % kFileSizeBytes;
}

/// Latency of random reads
void bench_rand_read_lat(uint8_t *pbuf, size_t thread_id) {
  FastRand rand;
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters; i++) {
      // Choose a random dependent byte
      size_t rand_addr = get_dependent_rand_addr(sum, rand);
      sum += pbuf[rand_addr];
    }

    double tot_ns = ns_since(start);
    printf("Thread %zu: Random read latency = %.2f ns. Sum = %zu\n", thread_id,
           tot_ns / kNumIters, sum);
  }
}

/// Tput of random reads
void bench_rand_read_tput(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kBatchSize = 10;
  FastRand rand;
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      // Choose a random dependent byte
      size_t addrs[kBatchSize];
      for (size_t i = 0; i < kBatchSize; i++) {
        addrs[i] = get_independent_rand_addr(rand);
      }

      for (size_t i = 0; i < kBatchSize; i++) sum += pbuf[addrs[i]];
    }

    double tot_sec = sec_since(start);
    printf("Thread %zu: random read tput = %.1f M/sec. Sum = %zu\n", thread_id,
           kNumIters / (tot_sec * 1000000), sum);
  }
}

/// Latency of random persistent writes
void bench_rand_write_lat(uint8_t *pbuf) {
  FastRand rand;
  struct timespec start;
  size_t ticks_sum = 0;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kNumIters; i++) {
    size_t ticks_st = rdtsc();

    size_t rand_addr = get_independent_rand_addr(rand);
    pmem_memset_persist(&pbuf[rand_addr], i, 64);

    ticks_sum += (rdtscp() - ticks_st);
  }

  double tot_ns = ns_since(start);
  printf(
      "Inverse throughput of persistent rand writes = %.2f ns. "
      "Bandwidth = %.2f GB. Average ticks = %zu\n",
      tot_ns / kNumIters, (kNumIters * 64) / tot_ns, ticks_sum / kNumIters);
}

/// Latency of persisting to the same byte in a file. Useful for timestamps etc.
void bench_same_byte_write_lat(uint8_t *pbuf) {
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kNumIters; i++) {
    pbuf[0] = 'A';
    pmem_persist(pbuf, 1);
  }

  double tot_ns = ns_since(start);
  printf("Latency of persistent writes to same byte = %.2f ns\n",
         tot_ns / kNumIters);
}

/// Bandwidth of large writes
void bench_write_sequential(uint8_t *pbuf) {
  void *dram_src_buf = malloc(kFileSizeBytes);
  rt_assert(dram_src_buf != nullptr);
  memset(dram_src_buf, 0, kFileSizeBytes);  // Map-in the buffer

  for (size_t copy_GB = 1; copy_GB <= 8; copy_GB *= 2) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(pbuf, dram_src_buf, copy_GB * GB(1));

    double tot_sec = sec_since(start);
    printf("Bandwidth of persistent writes (%zu GB) = %.2f GB/s\n", copy_GB,
           copy_GB / tot_sec);
  }

  for (size_t copy_GB = 1; copy_GB <= 8; copy_GB *= 2) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(pbuf, dram_src_buf, copy_GB * GB(1));

    double tot_sec = sec_since(start);
    printf("Bandwidth of persistent writes (%zu GB) = %.2f GB/s\n", copy_GB,
           copy_GB / tot_sec);
  }

  free(dram_src_buf);
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  tsc_freq = measure_rdtsc_freq();
  printf("RDTSC frequency = %.2f GHz\n", tsc_freq);

  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/src.txt", 0 /* length */, 0 /* flags */, 0666,
                    &mapped_len, &is_pmem));

  /*
  printf("Writing to the whole file...\n");
  pmem_memset_persist(pbuf, 3185, mapped_len);  // Map-in the file
  printf("Done writing.\n");
  */

  //  nano_sleep(1000000000, 3.0);  // Assume TSC frequency = 3 GHz

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len == kFileSizeGB * GB(1),
            "Incorrect file size " + std::to_string(mapped_len));
  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(is_pmem == 1, "File is not pmem");

  printf("Warming up for around 1 second.\n");

  std::vector<std::thread> threads(FLAGS_num_threads);
  for (size_t i = 0; i < FLAGS_num_threads; i++) {
    threads[i] = std::thread(bench_rand_read_lat, pbuf, i);
    // threads[i] = std::thread(bench_rand_read_tput, pbuf, i);

    /*
    bench_rand_read_tput(pbuf);
    bench_rand_write_lat(pbuf);
    bench_same_byte_write_lat(pbuf);
    bench_write_sequential(pbuf);
    */
  }

  for (auto &thread : threads) thread.join();

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
