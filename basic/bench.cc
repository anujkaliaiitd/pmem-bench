#include <errno.h>
#include <fcntl.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static constexpr size_t BUF_LEN = 4096;

int main(int argc, char *argv[]) {
  int srcfd;
  char buf[BUF_LEN];
  char *pmemaddr;
  size_t mapped_len;
  int is_pmem;
  ssize_t cc;

  if (argc != 3) {
    fprintf(stderr, "usage: %s src-file dst-file\n", argv[0]);
    exit(1);
  }

  /* open src-file */
  if ((srcfd = open(argv[1], O_RDONLY)) < 0) {
    perror(argv[1]);
    exit(1);
  }

  /* create a pmem file and memory map it */
  if ((pmemaddr = reinterpret_cast<char *>(
           pmem_map_file(argv[2], BUF_LEN, PMEM_FILE_CREATE | PMEM_FILE_EXCL,
                         0666, &mapped_len, &is_pmem))) == NULL) {
    perror("pmem_map_file");
    exit(1);
  }

  /* read up to BUF_LEN from srcfd */
  if ((cc = read(srcfd, buf, BUF_LEN)) < 0) {
    pmem_unmap(pmemaddr, mapped_len);
    perror("read");
    exit(1);
  }

  /* write it to the pmem */
  if (is_pmem) {
    pmem_memcpy_persist(pmemaddr, buf, static_cast<size_t>(cc));
  } else {
    memcpy(pmemaddr, buf, static_cast<size_t>(cc));
    pmem_msync(pmemaddr, static_cast<size_t>(cc));
  }

  close(srcfd);
  pmem_unmap(pmemaddr, mapped_len);

  exit(0);
}
