#include <errno.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <rte_rawdev.h>
#include <rte_ioat_rawdev.h>
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

static constexpr const char *kPmemFile = "/dev/dax0.0";
static constexpr size_t kFileSize = GB(128);
static constexpr size_t kNumaNode = 0;
static constexpr size_t kDevID = 0;
static constexpr size_t kIoatRingSize = 512;

DEFINE_uint64(size, MB(1), "Size of each copy");
DEFINE_uint64(window_size, 8, "Number of outstanding transfers");
DEFINE_uint64(use_ioat, 1, "Use IOAT DMA engines, else memcpy");
DEFINE_uint64(flush_cachelines, 1, "Flush src and dst cachelines before copy");

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
  rt_assert(kFileSize > FLAGS_size * 2, "FLAGS_size too large for file size");

  SlowRand slow_rand;
  double freq_ghz = measure_rdtsc_freq();
  hugealloc::Virt2Phy v2p;
  hugealloc::HugeAlloc huge_alloc(MB(128), kNumaNode);

  // Init DPDK
  const char *rte_argv[] = {"-c", "1",  "-n",  "4", "--log-level",
                            "5",  "-m", "128", NULL};

  int rte_argc = sizeof(rte_argv) / sizeof(rte_argv[0]) - 1;
  int ret = rte_eal_init(rte_argc, const_cast<char **>(rte_argv));
  rt_assert(ret >= 0, "rte_eal_init failed");
  setup_ioat_device();

  // Map pmem buffer
  size_t mapped_len;
  int is_pmem;
  uint8_t *pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kPmemFile, 0, 0, 0666, &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr);
  rt_assert(mapped_len >= kFileSize);
  rt_assert(is_pmem == 1);

  printf("Zeroing pbuf\n");
  memset(pbuf, 0, kFileSize);
  printf("Finished zeroing pbuf\n");

  uint64_t pbuf_phy = v2p.translate(pbuf);
  rt_assert(pbuf_phy != 0, "Failed to translate pmem buffer");

  // Check that pbuf hugepages are all contiguous
  for (size_t i = MB(2); i < kFileSize; i += MB(2)) {
    if (v2p.translate(&pbuf[i]) != pbuf_phy + i) {
      printf("Pmem buffer non-contiguous at offset %zu\n", i);
      exit(-1);
    }
  }

  // Allocate source buffer
  hugealloc::Buffer src = huge_alloc.alloc(FLAGS_size);
  for (size_t i = 0; i < FLAGS_size; i++) src.buf[i] = rand();

  size_t outstanding_copies = 0;

  for (size_t iters = 0; iters < 100; iters++) {
    printf("Starting work. Iter = %zu\n", iters);
    size_t timer_start = rdtsc();
    for (size_t i = 0; i < kFileSize; i += FLAGS_size) {
      ret = rte_ioat_enqueue_copy(kDevID, src.phys_addr, pbuf_phy + i,
                                  FLAGS_size, 0, 0, 0);
      rt_assert(ret == 1, "Error with rte_ioat_enqueue_copy");

      rte_ioat_do_copies(kDevID);
      outstanding_copies++;

      if (outstanding_copies == FLAGS_window_size) {
        poll_one();
        outstanding_copies--;
      }
    }

    // Wait for outstanding copies before deleting hugepages
    printf("Waiting for outstanding copies to finish\n");
    while (outstanding_copies > 0) {
      poll_one();
      outstanding_copies--;
    }

    double ns_total = to_nsec(rdtsc() - timer_start, freq_ghz);
    printf("%.2f GB/s\n", kFileSize / ns_total);

    // Pick a random offset in the NVM file where we pasted the source buffer
    size_t offset =
        (slow_rand.next_u64() % ((kFileSize / FLAGS_size) - 1)) * FLAGS_size;
    for (size_t i = 0; i < FLAGS_size; i++) {
      if (pbuf[offset + i] != src.buf[i]) printf("Mismatch at index %zu\n", i);
    }
  }

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
