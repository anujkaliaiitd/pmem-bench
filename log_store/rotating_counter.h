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

  /**
   * @brief Construct a counter
   *
   * @param pbuf The start address of the counter on persistent memory
   *
   * @param create_new If true, the counter is reset to zero. If false, the
   * counter is initialized using the prior pmem contents.
   */
  Counter(uint8_t *pbuf, bool create_new) : ctr_base_addr(pbuf) {
    if (create_new) {
      pmem_memset_persist(pbuf, 0, kNumBuffers * kBufferSize);
    } else {
      size_t cur_max = 0;    // Maximum value among the counters
      size_t cur_max_i = 0;  // Index of the maximum value
      for (size_t i = 0; i < kNumBuffers; i++) {
        size_t *counter_i = reinterpret_cast<size_t *>(&pbuf[i * kBufferSize]);
        if (*counter_i > cur_max) {
          cur_max = *counter_i;
          cur_max_i = i;
        }
      }

      v_value = cur_max;
      buffer_idx = (cur_max_i + 1) % kNumBuffers;
    }
  }

  Counter() {}

  /// The amount of contiguous pmem needed for this counter
  static size_t get_reqd_space() { return kNumBuffers * kBufferSize; }

  // Increment by always writing to the same location
  inline void increment_naive(size_t increment) {
    v_value += increment;
    pmem_memcpy_persist(&ctr_base_addr[0], &v_value, sizeof(v_value));
  }

  // Increment by writing to rotating locations, but don't do full-cacheline
  // writes
  inline void increment_rotate(size_t increment) {
    v_value += increment;
    pmem_memcpy_persist(&ctr_base_addr[buffer_idx * kBufferSize], &v_value,
                        sizeof(v_value));
    buffer_idx = (buffer_idx + 1) % kNumBuffers;
  }

  size_t v_value = 0;  // Volatile value of the counter

  size_t buffer_idx = 0;
  uint8_t *ctr_base_addr = nullptr;  // Starting address of the counter on pmem
};
