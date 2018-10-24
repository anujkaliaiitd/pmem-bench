#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

class Counter {
 public:
  static constexpr size_t kNumBuffers = 16;
  static constexpr size_t kBufferSize = 256;

  Counter(uint8_t *pbuf, bool create_new) : pbuf(pbuf) {
    if (create_new) {
      pmem_memset_persist(pbuf, 0, kNumBuffers * kBufferSize);
    } else {
      v_value = get_value();
    }
  }

  size_t get_value() const {
    size_t ret = 0;
    for (size_t i = 0; i < kNumBuffers; i++) {
      size_t *counter_i = reinterpret_cast<size_t *>(&pbuf[i * kBufferSize]);
      ret = std::max(ret, *counter_i);
    }
    return ret;
  }

  static size_t get_reqd_space() { return kNumBuffers * kBufferSize; }

  // Increment by always writing to the same location
  inline void increment_naive() {
    v_value++;
    pmem_memcpy_persist(&pbuf[0], &v_value, sizeof(v_value));
  }

  // Increment by writing to rotating locations, but don't do full-cacheline
  // writes
  inline void increment_rotate_no_full_cl() {
    v_value++;
    pmem_memcpy_persist(&pbuf[buffer_idx * kBufferSize], &v_value,
                        sizeof(v_value));
    buffer_idx = (buffer_idx + 1) % kNumBuffers;
  }

  // Increment by writing to rotating locations, and do full-cacheline writes
  inline void increment_rotate_full_cl() {
    v_value++;
    size_t cacheline[8];
    cacheline[0] = v_value;

    pmem_memcpy_persist(&pbuf[buffer_idx * kBufferSize], cacheline, 64);
    buffer_idx = (buffer_idx + 1) % kNumBuffers;
  }

  size_t v_value = 0;  // Volatile value

  size_t buffer_idx = 0;
  uint8_t *pbuf = nullptr;
};
