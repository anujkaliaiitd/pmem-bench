
/**
 * @file bench.cc
 *
 * @brief Benchmark for IOAT DMA based on DPDK instead of SPDK. The benchmark
 * task is to paste small, cached source buffers sequentially into the large
 * destination buffer.
 *
 * Flexibility: use IOAT or memcpy for the copy. Use volatile or persistent
 * buffer for the large destination buffer.
 */

#include <errno.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <rte_rawdev.h>
#include <rte_ioat_rawdev.h>
#include <rte_memcpy.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <pcg/pcg_random.hpp>
#include <random>
#include "../common.h"
#include "huge_alloc.h"
#include "virt2phy.h"

static constexpr const char *kPmemFile = "/dev/dax0.0";
static constexpr size_t kNumaNode = 0;
static constexpr size_t kDevID = 0;
static constexpr size_t kIoatRingSize = 512;

static constexpr size_t kDstBufferSize = GB(4);
static constexpr bool kCheckCopyResults = true;

DEFINE_uint64(num_prints, 3, "Number of measurements printed before exit");
DEFINE_uint64(size, KB(128), "Size of each copy");
DEFINE_uint64(window_size, 8, "Number of outstanding transfers");
DEFINE_uint64(use_ioat, 1, "Use IOAT DMA engines, else memcpy");
DEFINE_uint64(use_pmem, 1, "Use persistent memory for destination buffer");

// Initialize and start device 0
void setup_ioat_device() {
  struct rte_rawdev_info info;
  info.dev_private = NULL;

  rt_assert(rte_rawdev_info_get(kDevID, &info) == 0);
  rt_assert(std::string(info.driver_name).find("ioat") != std::string::npos);

  struct rte_ioat_rawdev_config p;
  memset(&info, 0, sizeof(info));
  info.dev_private = &p;

  rte_rawdev_info_get(kDevID, &info);
  rt_assert(p.ring_size == 0, "Initial ring size is non-zero");

  p.ring_size = kIoatRingSize;
  rt_assert(rte_rawdev_configure(kDevID, &info) == 0,
            "rte_rawdev_configure failed");

  rte_rawdev_info_get(kDevID, &info);
  rt_assert(p.ring_size == kIoatRingSize, "Wrong ring size");

  rt_assert(rte_rawdev_start(kDevID) == 0, "Rawdev start failed");

  printf("Started device %zu\n", kDevID);
}

void poll_one() {
  while (true) {
    uintptr_t _src, _dst;
    int ret = rte_ioat_completed_copies(kDevID, 1u, &_src, &_dst);
    rt_assert(ret >= 0, "rte_ioat_completed_copies error");

    if (ret > 0) break;
  }
}

int main(int argc, char **argv) {
  if (getuid() != 0) {
    // Mapping devdax files needs root perms for now
    printf("You need to be root to run this benchmark\n");
    exit(-1);
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto hugepage_caching_v2p = new HugepageCachingVirt2Phy();
  double freq_ghz = measure_rdtsc_freq();

  rt_assert(FLAGS_size <= KB(128),
            "Copy size must be small to reduce the likelihood of "
            "straddling 2 hugepages");

  rt_assert(kDstBufferSize / FLAGS_size > 2 * FLAGS_window_size,
            "Copy size too large, pipelined copies might overlap");

  // Init DPDK
  const char *rte_argv[] = {"-c", "1",  "-n",  "4", "--log-level",
                            "5",  "-m", "128", NULL};

  int rte_argc = sizeof(rte_argv) / sizeof(rte_argv[0]) - 1;
  int ret = rte_eal_init(rte_argc, const_cast<char **>(rte_argv));
  rt_assert(ret >= 0, "rte_eal_init failed");

  if (FLAGS_use_ioat == 1) {
    size_t count = rte_rawdev_count();
    printf("Fount %zu rawdev devices\n", count);
    rt_assert(count >= 1, "No rawdev devices available");

    setup_ioat_device();
  }

  // Create source and destination buffers
  auto huge_alloc = new hugealloc::HugeAlloc(MB(512), kNumaNode);
  std::vector<hugealloc::Buffer> src_bufs(FLAGS_window_size);
  for (size_t i = 0; i < FLAGS_window_size; i++) {
    src_bufs[i] = huge_alloc->alloc(FLAGS_size);
    rt_assert(src_bufs[i].buf != nullptr);

    memset(src_bufs[i].buf, i + 1, FLAGS_size);  // Page-in
  }

  printf("Allocating %zu GB destination buffer...", kDstBufferSize / GB(1));
  uint8_t *dst_buf = nullptr;

  if (FLAGS_use_pmem == 1) {
    // Map pmem buffer
    size_t mapped_len;
    int is_pmem;
    dst_buf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kPmemFile, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(dst_buf != nullptr);
    rt_assert(mapped_len >= kDstBufferSize);
    rt_assert(is_pmem == 1);

  } else {
    hugealloc::Buffer _dst_buf = huge_alloc->alloc_raw(kDstBufferSize);
    rt_assert(_dst_buf.buf != nullptr);
    rt_assert(reinterpret_cast<size_t>(_dst_buf.buf) % MB(2) == 0);
    dst_buf = _dst_buf.buf;
  }

  for (size_t i = 0; i < kDstBufferSize; i += MB(2)) dst_buf[i] = i;  // Page-in
  printf("done!\n");

  // Start test
  printf("Flags: size %zu, window size %zu, use_ioat %zu, use_pmem %zu\n",
         FLAGS_size, FLAGS_window_size, FLAGS_use_ioat, FLAGS_use_pmem);

  size_t num_printed = 0;  // Number of times we printed stats
  size_t num_completed_copies = 0;

  size_t src_bufs_i = 0;      // Index among the source buffers for the next job
  size_t dst_buf_offset = 0;  // Offset in the destination buffer

  size_t ioat_outstanding_jobs = 0;
  size_t timer_start = rdtsc();
  FastRand fast_rand;

  while (true) {
    if (FLAGS_use_ioat == 1) {
      if (dst_buf_offset / MB(2) != (dst_buf_offset + FLAGS_size) / MB(2)) {
        // The copy operating will straddle two hugepages
        dst_buf_offset += FLAGS_size;
        continue;  // Go back
      }

      uint8_t *dst_buf_ptr = &dst_buf[dst_buf_offset];
      uint64_t dst_phys_addr = hugepage_caching_v2p->translate(dst_buf_ptr);

      uint8_t *src_buf_ptr = src_bufs[src_bufs_i].buf;
      uint64_t src_phys_addr = hugepage_caching_v2p->translate(src_buf_ptr);

      // Pass zeroes as callback args, we don't need them for now
      int ret = rte_ioat_enqueue_copy(
          kDevID, src_phys_addr, dst_phys_addr, FLAGS_size,
          reinterpret_cast<uintptr_t>(src_buf_ptr),
          reinterpret_cast<uintptr_t>(dst_buf_ptr), 0);

      rt_assert(ret == 1, "Error with rte_ioat_enqueue_copy");
      rte_ioat_do_copies(kDevID);

      ioat_outstanding_jobs++;
      rt_assert(ioat_outstanding_jobs <= kIoatRingSize);

      if (ioat_outstanding_jobs == FLAGS_window_size) {
        // Poll for a completed copy
        while (true) {
          uintptr_t _src = 0, _dst = 0;
          int ret = rte_ioat_completed_copies(kDevID, 1u, &_src, &_dst);
          rt_assert(ret >= 0, "rte_ioat_completed_copies error");

          if (ret == 1 && kCheckCopyResults) {
            // Check at a random offset
            size_t offset = fast_rand.next_u32() % FLAGS_size;
            uint8_t src_val = reinterpret_cast<uint8_t *>(_src)[offset];
            uint8_t dst_val = reinterpret_cast<uint8_t *>(_dst)[offset];
            if (unlikely(src_val != dst_val)) {
              fprintf(stderr, "Mismatch\n");
            }
          }

          num_completed_copies += static_cast<size_t>(ret);
          ioat_outstanding_jobs -= static_cast<size_t>(ret);
          if (ret > 0) break;
        }
      }
    } else {  // Use memcpy
      if (FLAGS_use_pmem == 0) {
        rte_memcpy(&dst_buf[dst_buf_offset], src_bufs[src_bufs_i].buf,
                   FLAGS_size);
      } else {
        pmem_memcpy_persist(&dst_buf[dst_buf_offset], src_bufs[src_bufs_i].buf,
                            FLAGS_size);
      }
      num_completed_copies++;
    }

    // If we're here, we did/enqueued a copy. Bump src and dst buffers.
    src_bufs_i++;
    if (src_bufs_i == FLAGS_window_size) src_bufs_i = 0;

    dst_buf_offset += FLAGS_size;
    if (dst_buf_offset + FLAGS_size >= kDstBufferSize) {
      dst_buf_offset = 0;

      double ns_total = to_nsec(rdtsc() - timer_start, freq_ghz);
      printf("%.2f GB/s\n", num_completed_copies * FLAGS_size / ns_total);

      num_completed_copies = 0;
      num_printed++;
      timer_start = rdtsc();
    }

    if (num_printed == FLAGS_num_prints) break;
  }

  // With IOAT, wait for outstanding copies before deleting hugepages
  printf("Waiting for outstanding copies to finish\n");
  while (FLAGS_use_ioat == 1 && ioat_outstanding_jobs > 0) {
    uintptr_t _src, _dst;
    int ret = rte_ioat_completed_copies(kDevID, 1u, &_src, &_dst);
    rt_assert(ret >= 0, "rte_ioat_completed_copies error");
    ioat_outstanding_jobs -= static_cast<size_t>(ret);
  }

  delete huge_alloc;
}
