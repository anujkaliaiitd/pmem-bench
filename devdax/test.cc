#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main() {
  void *buf = 0;

  int fd = open("/dev/dax12.0", O_RDWR);
  if (fd < 0) {
    printf("open failed\n");
    exit(-1);
  }

  size_t size = 2ull * 1024 * 1024 * 1024;
  buf = mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE,
             MAP_SHARED, fd, 0);
  if (buf == MAP_FAILED) {
    perror("mmap failed");
    exit(-1);
  }

  printf("%s\n", buf);
  snprintf(reinterpret_cast<char *>(buf), 8, "akalia");

  munmap(buf, static_cast<size_t>(size));
  close(fd);
}
