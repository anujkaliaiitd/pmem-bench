// This can be used to write random contents to a pmem file so that later
// experiments don't benefit from any crazy value prediction of a zeroed file.

#include <libpmem.h>
#include <pcg/pcg_random.hpp>
#include "../common.h"

static constexpr const char *kPmemFile = "/mnt/pmem12/raft_log";
static constexpr size_t kPmemFileSizeGB = 1024;  // The expected file size
static constexpr size_t kPmemFileSize = kPmemFileSizeGB * GB(1);
static constexpr size_t kRandTemplateSz = GB(32);

int main(int, char **) {
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  pbuf = reinterpret_cast<uint8_t *>(pmem_map_file(
      kPmemFile, 0 /* length */, 0 /* flags */, 0666, &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len >= kPmemFileSize,
            "pmem file too small " + std::to_string(mapped_len));
  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(is_pmem == 1, "File is not pmem");

  printf("Generating random contents\n");
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  size_t *rand_buf = reinterpret_cast<size_t *>(malloc(kRandTemplateSz));
  for (size_t i = 0; i < kRandTemplateSz / sizeof(size_t); i++) {
    rand_buf[i] = pcg();
  }

  printf("Writing random contents to the whole file.\n");
  rt_assert(kPmemFileSize % kRandTemplateSz == 0);

  for (size_t i = 0; i < kPmemFileSize; i += kRandTemplateSz) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(&pbuf[i], rand_buf, kRandTemplateSz);
    printf("Fraction complete = %.2f. Took %.3f sec for %zu GB.\n",
           (i + 1) * 1.0 / kPmemFileSize, sec_since(start),
           kRandTemplateSz / GB(1));
  }

  printf("Done writing.\n");

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
