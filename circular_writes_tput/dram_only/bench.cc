#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "config.h"

#define clwb(addr) \
  asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(addr)));

int main() {
  uint8_t *buf = new uint8_t[kNumCounters * 64];
  size_t data = 0;
  for (size_t i = 0; i < 10000000; i++) {
    size_t buf_offset = (i % kNumCounters) * 64;
    buf[buf_offset] = data++;
    clwb(&buf[buf_offset]);
  }

  free(buf);
}
