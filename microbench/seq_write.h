#include "../common.h"
#include "main.h"

/// Sequential writes
void bench_seq_write(uint8_t *pbuf, size_t thread_id, size_t copy_sz,
                     double *avg_tput_GBps) {
  // We perform multiple measurements. In each measurement, a thread writes
  // kCopyPerThreadPerMsr bytes in copy_sz chunks.
  static constexpr size_t kNumMsr = 1;
  static constexpr size_t kCopyPerThreadPerMsr = GB(1);
  rt_assert(kCopyPerThreadPerMsr % copy_sz == 0, "Unaligned copy size");

  void *dram_src_buf = memalign(4096, copy_sz);
  memset(dram_src_buf, 0, copy_sz);

  // Each thread write to non-overlapping addresses
  const size_t excl_bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
  rt_assert(excl_bytes_per_thread >= kCopyPerThreadPerMsr);
  const size_t base_offset = thread_id * excl_bytes_per_thread;

  // We begin copies from a random aligned offset in the file. This prevents
  // multiple calls from writing to the same file region. std::random_device
  // produces a non-deterministic seed.
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  size_t offset = base_offset + (pcg() % excl_bytes_per_thread);
  offset = roundup<256>(offset);

  double tput_sum_GBps = 0;  // Used to compute average througput at the end

  for (size_t msr = 0; msr < kNumMsr; msr++) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kCopyPerThreadPerMsr / copy_sz; i++) {
      pmem_memmove_persist(&pbuf[offset], dram_src_buf, copy_sz);
      offset += copy_sz;
      if (offset + copy_sz >= base_offset + excl_bytes_per_thread) {
        offset = base_offset;
      }
    }

    double tot_sec = sec_since(start);
    double tput_GBps = kCopyPerThreadPerMsr / (tot_sec * 1000000000);
    printf("Thread %zu: copy_sz %zu, %.2f GB/s\n", thread_id, copy_sz,
           tput_GBps);
    tput_sum_GBps += tput_GBps;
  }

  *avg_tput_GBps = tput_sum_GBps / kNumMsr;
  free(dram_src_buf);
}
