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

static constexpr size_t kPmemFileSizeGB = 256;  // The expected file size
static constexpr size_t kPmemFileSize = kPmemFileSizeGB * GB(1);
static constexpr bool kMeasureLatency = false;
double freq_ghz = 0.0;
static size_t align64(size_t x) { return x - x % 64; }

static constexpr int kHdrPrecision = 2;          // Precision for hdr histograms
static constexpr int kMinPmemLatCycles = 1;      // Min pmem latency in cycles
static constexpr int kMaxPmemLatCycles = MB(1);  // Max pmem latency in cycles

/// Get a random offset in the file with at least \p space after it
size_t get_random_offset_with_space(pcg64_fast &pcg, size_t space) {
  size_t iters = 0;
  while (true) {
    size_t rand_offset = pcg() % kPmemFileSize;
    if (kPmemFileSize - rand_offset > space) return rand_offset;
    iters++;
    if (iters > 2) printf("Random offset took over 2 iters\n");
  }
}

void bench_seq_read_tput(uint8_t *pbuf, size_t thread_id) {
  _unused(pbuf);
  _unused(thread_id);
}

/// Random read latency
void bench_rand_read_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(1);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters; i++) {
      // Choose a random dependent byte
      size_t rand_addr = (pcg() + sum % 8) % kPmemFileSize;
      sum += pbuf[rand_addr];
    }

    double tot_ns = ns_since(start);
    printf("Thread %zu: Random read latency = %.2f ns. Sum = %zu\n", thread_id,
           tot_ns / kNumIters, sum);
  }
}

/// Random read throughput
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
        offset[j] = pcg() % kPmemFileSize;
      }
      for (size_t j = 0; j < kBatchSize; j++) sum += pbuf[offset[j]];
    }

    double tot_sec = sec_since(start);
    printf("Thread %zu: random read tput = %.2f M/sec. Sum = %zu\n", thread_id,
           kNumIters / (tot_sec * 1000000), sum);
  }
}

/// Random write throughput
void bench_rand_write_tput(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kBatchSize = 10;
  static constexpr size_t kNumIters = MB(4);

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
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

/// Random write latency
void bench_rand_write_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(2);
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  while (true) {
    size_t ticks_sum = 0;
    for (size_t i = 0; i < kNumIters; i++) {
      size_t ticks_st = rdtsc();
      pmem_memset_persist(&pbuf[base_addr + (pcg() % bytes_per_thread)], i, 64);
      ticks_sum += (rdtscp() - ticks_st);
    }

    printf("Thread %zu: Latency of persistent rand writes = %.2f ns.\n",
           thread_id, ticks_sum / (kNumIters * freq_ghz));
  }
}

/// Sequential writes
void bench_seq_write(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kCopySize = MB(256);
  void *dram_src_buf = malloc(kCopySize);
  memset(dram_src_buf, 0, kCopySize);

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;
  size_t cur_base = base_addr;

  while (true) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(&pbuf[cur_base], dram_src_buf, kCopySize);

    cur_base += kCopySize;
    if (cur_base + kCopySize >= base_addr + bytes_per_thread) {
      cur_base = base_addr;
    }

    double tot_sec = sec_since(start);
    printf("Thread %zu: Bandwidth of persistent writes (%.3f GB) = %.2f GB/s\n",
           thread_id, kCopySize * 1.0 / GB(1), kCopySize / (tot_sec * GB(1)));
  }

  free(dram_src_buf);
}

// Write to the whole file to "map it in", whatever that means
void map_in_file_whole(uint8_t *pbuf, size_t mapped_len) {
  printf("Writing to the whole file for map-in...\n");
  const size_t chunk_sz = GB(16);
  rt_assert(mapped_len % chunk_sz == 0, "Invalid chunk size for map-in");

  for (size_t i = 0; i < mapped_len; i += chunk_sz) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memset_persist(&pbuf[i], 3185, chunk_sz);  // nodrain performs similar
    printf("Fraction complete = %.2f. Took %.3f sec for %zu GB.\n",
           (i + 1) * 1.0 / mapped_len, sec_since(start), chunk_sz / GB(1));
  }

  printf("Done writing.\n");
}

// Write to a byte in each page of the file, to map the pages in
void map_in_file_by_page(uint8_t *pbuf, size_t mapped_len) {
  printf("Mapping-in file pages.\n");
  const size_t chunk_sz = GB(16);
  rt_assert(mapped_len % chunk_sz == 0, "Invalid chunk size for map-in");

  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < mapped_len; i += KB(4)) {
    pmem_memset_nodrain(&pbuf[i], 3185, 1);
    if (i % GB(32) == 0 && i > 0) {
      printf("Fraction complete = %.2f. Took %.3f sec for %zu GB.\n",
             (i + 1) * 1.0 / mapped_len, sec_since(start), chunk_sz / GB(1));
      clock_gettime(CLOCK_REALTIME, &start);
    }
  }

  printf("Done mapping-in.\n");
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  freq_ghz = measure_rdtsc_freq();
  printf("RDTSC frequency = %.2f GHz\n", freq_ghz);

  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/raft_log", 0 /* length */, 0 /* flags */, 0666,
                    &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len == kPmemFileSizeGB * GB(1),
            "Incorrect file size " + std::to_string(mapped_len));
  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(is_pmem == 1, "File is not pmem");

  // map_in_file_by_page(pbuf, mapped_len);
  // map_in_file_whole(pbuf, mapped_len);

  //  nano_sleep(1000000000, 3.0);  // Assume TSC frequency = 3 GHz

  std::vector<std::thread> threads(FLAGS_num_threads);
  for (size_t i = 0; i < FLAGS_num_threads; i++) {
    // threads[i] = std::thread(bench_rand_read_lat, pbuf, i);
    // threads[i] = std::thread(bench_rand_read_tput, pbuf, i);
    // threads[i] = std::thread(bench_rand_write_lat, pbuf, i);
    // threads[i] = std::thread(bench_rand_write_tput, pbuf, i);
    threads[i] = std::thread(bench_seq_write, pbuf, i);
  }

  for (auto &thread : threads) thread.join();

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
