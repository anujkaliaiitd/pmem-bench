#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static constexpr size_t kNumIters = 1000000;
static constexpr size_t kFileSizeGB = 1024;
static constexpr size_t kFileSizeBytes = (1ull << 30) * kFileSizeGB;
static constexpr const char *kPmemFile = "/mnt/pmem12/raft_log";

inline uint32_t fastrand(uint64_t &seed) {
  seed = seed * 1103515245 + 12345;
  return static_cast<uint32_t>(seed >> 32);
}

/// Return nanoseconds elapsed since timestamp \p t0
static double ns_since(const struct timespec &t0) {
  struct timespec t1;
  clock_gettime(CLOCK_REALTIME, &t1);
  return (t1.tv_sec - t0.tv_sec) * 1000000000.0 + (t1.tv_nsec - t0.tv_nsec);
}

// Used for shuffle-based pointer chain measurement
struct cacheline_t {
  cacheline_t *ptr;
  size_t pad[7];
};
static_assert(sizeof(cacheline_t) == 64, "");

int main() {
  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kPmemFile, 0, 0, 0666, &mapped_len, &is_pmem));
  assert(pbuf != nullptr);
  assert(mapped_len >= kFileSizeBytes);
  assert(is_pmem == 1);

  uint64_t seed = 0xdeadbeef;
  size_t sum = 0;

  for (size_t msr = 0; msr < 5; msr++) {
    // Initialize measurement
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      size_t rand_sample = fastrand(seed);
      rand_sample = (rand_sample << 5);  // Ignore LSBs to get larger numbers

      size_t file_offset = (sum + rand_sample) % kFileSizeBytes;
      sum += pbuf[file_offset];  // Make the next read dependent
    }

    double bench_ns = ns_since(bench_start);
    printf("Average read latency = %.1f ns, sum = %zu\n", bench_ns / kNumIters,
           sum);
  }

  // Shuffle-based measurement here
  /*
   *
  auto *cl_arr = reinterpret_cast<cacheline_t *>(pbuf);
  const size_t num_cl = kFileSizeBytes / sizeof(cacheline_t);
  for (size_t i = 0; i < num_cl; i++) {
    cl_arr[i].ptr = &cl_arr[i];
  }

  for (size_t i = num_cl - 1; i >= 1; i--) {
    if (i % (128 * 1024) == 0) {
      printf("shuffle progress (left) = %.2f\n", i * 1.0 / num_cl);
    }
    size_t j = fastrand(seed) % i;
    cacheline_t *temp = cl_arr[j].ptr;
    cl_arr[j].ptr = cl_arr[i].ptr;
    cl_arr[i].ptr = temp;
  }
  cacheline_t *ptr = cl_arr[0].ptr;

  for (size_t msr = 0; msr < 10; msr++) {
    // Initialize measurement
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    // Real work
    for (size_t i = 0; i < kNumIters; i++) {
      ptr = reinterpret_cast<cacheline_t *>((*ptr).ptr);
    }

    double bench_ns = ns_since(bench_start);
    printf("Average read latency = %.1f ns, ptr = %p\n", bench_ns / kNumIters,
           ptr);
  }
  */

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
