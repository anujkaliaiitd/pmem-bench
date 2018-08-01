#include <errno.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pcg/pcg_random.hpp>
#include <thread>
#include "../common.h"

DEFINE_uint64(num_threads, 0, "Number of threads");

static constexpr size_t kFileSizeGB = 1024;  // The expected file size
static constexpr size_t kFileSizeBytes = kFileSizeGB * GB(1);
double tsc_freq = 0.0;
static size_t align64(size_t x) { return x - x % 64; }

/// Latency of random reads
void bench_rand_read_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(1);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters; i++) {
      // Choose a random dependent byte
      size_t rand_addr = (pcg() + sum % 8) % kFileSizeBytes;
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
  static constexpr size_t kNumIters = MB(4);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      size_t offset[kBatchSize];
      for (size_t j = 0; j < kBatchSize; j++) {
        offset[j] = pcg() % kFileSizeBytes;
      }
      for (size_t j = 0; j < kBatchSize; j++) sum += pbuf[offset[j]];
    }

    double tot_sec = sec_since(start);
    printf("Thread %zu: random read tput = %.2f M/sec. Sum = %zu\n", thread_id,
           kNumIters / (tot_sec * 1000000), sum);
  }
}

/// Throughput of random batched persistent writes
void bench_rand_write_tput(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kBatchSize = 10;
  static constexpr size_t kNumIters = MB(4);

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kFileSizeBytes / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      size_t offset[kBatchSize];
      for (size_t j = 0; j < kBatchSize; j++) {
        offset[j] = base_addr + (pcg() % bytes_per_thread);
        offset[j] = align64(offset[j]);
        pmem_memset_nodrain(&pbuf[offset[j]], i + j, 64);
      }

      pmem_drain();
    }

    double tot_sec = sec_since(start);
    double cacheline_rate = kNumIters / tot_sec;
    printf("Thread %zu: random write tput = %.2f M/sec, %.2f GB/s\n", thread_id,
           cacheline_rate / 1000000, (cacheline_rate * 64) / 1000000000);
  }
}

/// Latency of random persistent writes
void bench_rand_write_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(2);
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kFileSizeBytes / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  while (true) {
    size_t ticks_sum = 0;
    for (size_t i = 0; i < kNumIters; i++) {
      size_t ticks_st = rdtsc();
      pmem_memset_persist(&pbuf[base_addr + (pcg() % bytes_per_thread)], i, 64);
      ticks_sum += (rdtscp() - ticks_st);
    }

    printf("Thread %zu: Latency of persistent rand writes = %.2f ns.\n",
           thread_id, ticks_sum / (kNumIters * tsc_freq));
  }
}

/// Latency of persisting to the same byte in a file. Useful for timestamps etc.
void bench_same_byte_write_lat(uint8_t *pbuf) {
  static constexpr size_t kNumIters = MB(1);

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
    // threads[i] = std::thread(bench_rand_read_lat, pbuf, i);
    // threads[i] = std::thread(bench_rand_read_tput, pbuf, i);
    // threads[i] = std::thread(bench_rand_write_lat, pbuf, i);
    threads[i] = std::thread(bench_rand_write_tput, pbuf, i);

    /*
    bench_rand_read_tput(pbuf);
    bench_same_byte_write_lat(pbuf);
    bench_write_sequential(pbuf);
    */
  }

  for (auto &thread : threads) thread.join();

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
