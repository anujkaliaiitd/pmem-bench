#include <errno.h>
#include <fcntl.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "../common.h"

static constexpr size_t kBufLen = 4096;

int main() {
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/src.txt", 0 /* length */, 0 /* flags */, 0666,
                    &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(is_pmem == 1, "File is not pmem");
  printf("mapped length = %zu\n", mapped_len);

  pbuf[0] = 'A';
  pmem_persist(pbuf, 1);

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
