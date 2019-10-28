#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define clwb(addr) \
  asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(addr)));

int main(int argc, char **argv) {
  if (argc <= 1) {
    printf("Usage ./bench [num_counters]\n");
    exit(0);
  }

  size_t num_counters = static_cast<size_t>(atoi(argv[1]));
  uint8_t *buf = reinterpret_cast<uint8_t *>(memalign(num_counters * 64, 4096));

  size_t data = 0;
  for (size_t i = 0; i < 10000000; i++) {
    size_t buf_offset = (i % num_counters) * 64;
    buf[buf_offset] = data++;
    clwb(&buf[buf_offset]);
    asm volatile("sfence" ::: "memory");
  }

  free(buf);
}
