/**
 * @file huge_alloc.h
 * @brief A header-only fast hugepage allocator with no dependencies
 * @author Anuj Kalia
 * @date 2018-09-25
 */

#pragma once

#include <assert.h>
#include <errno.h>
#include <malloc.h>
#include <numaif.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace hugealloc {

static constexpr size_t kHugepageSize = (2 * 1024 * 1024);  ///< Hugepage size

/// Information about an SHM region
struct shm_region_t {
  // Constructor args
  const int shm_key;   /// The key used to create the SHM region
  const uint8_t *buf;  /// The start address of the allocated SHM buffer
  const size_t size;   /// The size in bytes of the allocated SHM buffer

  shm_region_t(int shm_key, uint8_t *buf, size_t size)
      : shm_key(shm_key), buf(buf), size(size) {
    assert(size % kHugepageSize == 0);
  }
};

class SlowRand {
  std::random_device rand_dev;  // Non-pseudorandom seed for twister
  std::mt19937_64 mt;
  std::uniform_int_distribution<uint64_t> dist;

 public:
  SlowRand() : mt(rand_dev()), dist(0, UINT64_MAX) {}

  inline uint64_t next_u64() { return dist(mt); }
};

/// A class to hold a fixed-size buffer. The size of the buffer is read-only
/// after the Buffer is created.
class Buffer {
 public:
  Buffer(uint8_t *buf, size_t class_size) : buf(buf), class_size(class_size) {}

  Buffer() {}

  /// Since \p Buffer does not allocate its own \p buf, do nothing here.
  ~Buffer() {}

  /// Return a string representation of this Buffer (excluding lkey)
  std::string to_string() const {
    char ret[100];
    sprintf(ret, "[buf %p, class sz %zu]", buf, class_size);
    return std::string(ret);
  }

  /// The backing memory of this Buffer. The Buffer is invalid if this is null.
  uint8_t *buf;
  size_t class_size;  ///< The class size
};

/// Return the index of the most significant bit of x. The index of the 2^0
/// bit is 1. (x = 0 returns 0, x = 1 returns 1.)
static inline size_t msb_index(int x) {
  assert(x < INT32_MAX / 2);
  int index;
  asm("bsrl %1, %0" : "=r"(index) : "r"(x << 1));
  return static_cast<size_t>(index);
}

template <typename T>
static constexpr inline bool is_power_of_two(T x) {
  return x && ((x & T(x - 1)) == 0);
}
template <uint64_t power_of_two_number, typename T>
static constexpr inline T round_up(T x) {
  static_assert(is_power_of_two(power_of_two_number),
                "PowerOfTwoNumber must be a power of 2");
  return ((x) + T(power_of_two_number - 1)) & (~T(power_of_two_number - 1));
}

static inline void rt_assert(bool condition, std::string throw_str, char *s) {
  if (!condition) {
    throw std::runtime_error(throw_str + std::string(s));
  }
}

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition, std::string throw_str) {
  if (!condition) throw std::runtime_error(throw_str);
}

/// Check a condition at runtime. If the condition is false, throw exception.
/// This is faster than rt_assert(cond, str) as it avoids string construction.
static inline void rt_assert(bool condition) {
  if (!condition) throw std::runtime_error("Error");
}

/**
 * A hugepage allocator that uses per-class freelists. The minimum class size
 * is kMinClassSize, and class size increases by a factor of 2 until
 * kMaxClassSize.
 *
 * When a new SHM region is added to the allocator, it is split into Buffers of
 * size kMaxClassSize and added to that class. These Buffers are later split to
 * fill up smaller classes.
 *
 * The \p size field of allocated Buffers equals the requested size, i.e., it's
 * not rounded to the class size.
 *
 * The allocator uses randomly generated positive SHM keys, and deallocates the
 * SHM regions it creates when deleted.
 */
class HugeAlloc {
 public:
  static constexpr const char *alloc_fail_help_str =
      "This could be due to insufficient huge pages or SHM limits.";
  static const size_t kMinClassSize = 64;     /// Min allocation size
  static const size_t kMinClassBitShift = 6;  /// For division by kMinClassSize
  static_assert((kMinClassSize >> kMinClassBitShift) == 1, "");

  static const size_t kMaxClassSize = 8 * 1024 * 1024;  /// Max allocation size
  static const size_t kNumClasses = 18;  /// 64 B (2^6), ..., 8 MB (2^23)
  static_assert(kMaxClassSize == kMinClassSize << (kNumClasses - 1), "");

  /// Return the maximum size of a class
  static constexpr size_t class_max_size(size_t class_i) {
    return kMinClassSize * (1ull << class_i);
  }

  /**
   * @brief Construct the hugepage allocator
   * @throw runtime_error if construction fails
   */
  HugeAlloc(size_t initial_size, size_t numa_node) : numa_node(numa_node) {
    if (initial_size < kMaxClassSize) initial_size = kMaxClassSize;
    prev_allocation_size = initial_size;
  }

  HugeAlloc(size_t numa_node) : numa_node(numa_node) {
    prev_allocation_size = kMaxClassSize;
  }

  ~HugeAlloc() {
    for (shm_region_t &shm_region : shm_list) {
      delete_shm(shm_region.shm_key, shm_region.buf);
    }
  }

  /**
   * @brief Allocate memory using raw SHM operations, always bypassing the
   * allocator's freelists. Unlike \p alloc(), the size of the allocated memory
   * need not fit in the allocator's max class size.
   *
   * Allocated memory can be freed only when this allocator is destroyed, i.e.,
   * free_buf() cannot be used. Use alloc() if freeing is needed.
   *
   * @param size The minimum size of the allocated memory
   *
   * @return The allocated hugepage-backed Buffer. buffer.buf is nullptr if we
   * ran out of memory. buffer.class_size is set to SIZE_MAX to indicate that
   * allocator classes were not used.
   *
   * @throw runtime_error if hugepage reservation failure is catastrophic
   */
  Buffer alloc_raw(size_t size) {
    std::ostringstream xmsg;  // The exception message
    size = round_up<kHugepageSize>(size);
    int shm_key, shm_id;

    while (true) {
      // Choose a positive SHM key. Negative is fine but it looks scary in the
      // error message.
      shm_key = static_cast<int>(slow_rand.next_u64());
      shm_key = std::abs(shm_key);

      // Try to get an SHM region
      shm_id = shmget(shm_key, size, IPC_CREAT | IPC_EXCL | 0666 | SHM_HUGETLB);

      if (shm_id == -1) {
        switch (errno) {
          case EEXIST:
            continue;  // shm_key already exists. Try again.

          case EACCES:
            xmsg << "HugeAlloc: SHM allocation error. "
                 << "Insufficient permissions.";
            throw std::runtime_error(xmsg.str());

          case EINVAL:
            xmsg << "HugeAlloc: SHM allocation error: SHMMAX/SHMIN "
                 << "mismatch. size = " << std::to_string(size) << " ("
                 << std::to_string(size / (1024 * 1024)) << " MB).";
            throw std::runtime_error(xmsg.str());

          case ENOMEM:
            // Out of memory - this is OK
            fprintf(
                stderr,
                "HugeAlloc: Insufficient hugepages. Can't reserve %lu MB.\n",
                size / (1024 * 1024));
            return Buffer(nullptr, 0);

          default:
            xmsg << "HugeAlloc: Unexpected SHM malloc error "
                 << strerror(errno);
            throw std::runtime_error(xmsg.str());
        }
      } else {
        // shm_key worked. Break out of the while loop.
        break;
      }
    }

    uint8_t *shm_buf = static_cast<uint8_t *>(shmat(shm_id, nullptr, 0));
    rt_assert(shm_buf != nullptr,
              "HugeAlloc: shmat() failed. Key = " + std::to_string(shm_key));

    // Bind the buffer to the NUMA node
    const unsigned long nodemask =
        (1ul << static_cast<unsigned long>(numa_node));
    long ret = mbind(shm_buf, size, MPOL_BIND, &nodemask, 32, 0);
    rt_assert(ret == 0,
              "HugeAlloc: mbind() failed. Key " + std::to_string(shm_key));

    // Save the SHM region so we can free it later
    shm_list.push_back(shm_region_t(shm_key, shm_buf, size));
    stats.shm_reserved += size;

    // buffer.class_size is invalid because we didn't allocate from a class
    return Buffer(shm_buf, SIZE_MAX);
  }

  /**
   * @brief Allocate a Buffer using the allocator's freelists, i.e., the max
   * size that can be allocated is the max freelist class size.
   *
   * The actual allocation is done in \p alloc_from_class.
   *
   * @param size The minimum size of the allocated Buffer. \p size need not
   * equal a class size.
   *
   * @return The allocated buffer. The buffer is invalid if we ran out of
   * memory. The \p class_size of the allocated Buffer is equal to a
   * HugeAlloc class size.
   *
   * @throw runtime_error if \p size is too large for the allocator, or if
   * hugepage reservation failure is catastrophic
   */
  Buffer alloc(size_t size) {
    assert(size <= kMaxClassSize);

    size_t size_class = get_class(size);
    assert(size_class < kNumClasses);

    if (!freelist[size_class].empty()) {
      return alloc_from_class(size_class);
    } else {
      // There is no free Buffer in this class. Find the first larger class with
      // free Buffers.
      size_t next_class = size_class + 1;
      for (; next_class < kNumClasses; next_class++) {
        if (!freelist[next_class].empty()) break;
      }

      if (next_class == kNumClasses) {
        // There's no larger size class with free pages, we we need to allocate
        // more hugepages. This adds some Buffers to the largest class.
        prev_allocation_size *= 2;
        bool success = reserve_hugepages(prev_allocation_size);
        if (!success) {
          prev_allocation_size /= 2;  // Restore the previous allocation
          return Buffer(nullptr, 0);
        } else {
          next_class = kNumClasses - 1;
        }
      }

      // If we're here, \p next_class has free Buffers
      assert(next_class < kNumClasses);
      while (next_class != size_class) {
        split(next_class);
        next_class--;
      }

      assert(!freelist[size_class].empty());
      return alloc_from_class(size_class);
    }

    assert(false);
    exit(-1);  // We should never get here
    return Buffer(nullptr, 0);
  }

  /// Free a Buffer
  inline void free_buf(Buffer buffer) {
    assert(buffer.buf != nullptr);

    size_t size_class = get_class(buffer.class_size);
    assert(class_max_size(size_class) == buffer.class_size);

    freelist[size_class].push_back(buffer);
    stats.user_alloc_tot -= buffer.class_size;
  }

  inline size_t get_numa_node() { return numa_node; }

  /// Return the total amount of memory reserved as hugepages
  inline size_t get_stat_shm_reserved() const {
    assert(stats.shm_reserved % kHugepageSize == 0);
    return stats.shm_reserved;
  }

  /// Return the total amoung of memory allocated to the user
  inline size_t get_stat_user_alloc_tot() const {
    assert(stats.user_alloc_tot % kMinClassSize == 0);
    return stats.user_alloc_tot;
  }

  /// Print a summary of this allocator
  void print_stats();

 private:
  /**
   * @brief Get the class index for a Buffer size
   * @param size The size of the buffer, which may or may not be a class size
   */
  inline size_t get_class(size_t size) {
    assert(size >= 1 && size <= kMaxClassSize);
    // Use bit shift instead of division to make debug-mode code a faster
    return msb_index(static_cast<int>((size - 1) >> kMinClassBitShift));
  }

  /// Reference function for the optimized \p get_class function above
  inline size_t get_class_slow(size_t size) {
    assert(size >= 1 && size <= kMaxClassSize);

    size_t size_class = 0;             // The size class for \p size
    size_t class_lim = kMinClassSize;  // The max size for \p size_class
    while (size > class_lim) {
      size_class++;
      class_lim *= 2;
    }

    return size_class;
  }

  /// Split one Buffers from class \p size_class into two Buffers of the
  /// previous class, which must be an empty class.
  inline void split(size_t size_class) {
    assert(size_class >= 1);
    assert(!freelist[size_class].empty());
    assert(freelist[size_class - 1].empty());

    Buffer buffer = freelist[size_class].back();
    freelist[size_class].pop_back();
    assert(buffer.class_size == class_max_size(size_class));

    Buffer buffer_0 = Buffer(buffer.buf, buffer.class_size / 2);
    Buffer buffer_1 =
        Buffer(buffer.buf + buffer.class_size / 2, buffer.class_size / 2);

    freelist[size_class - 1].push_back(buffer_0);
    freelist[size_class - 1].push_back(buffer_1);
  }

  /**
   * @brief Allocate a Buffer from a non-empty class
   * @param size_class Index of the non-empty size class to allocate from
   */
  inline Buffer alloc_from_class(size_t size_class) {
    assert(size_class < kNumClasses);

    // Use the Buffers at the back to improve locality
    Buffer buffer = freelist[size_class].back();
    assert(buffer.class_size = class_max_size(size_class));
    freelist[size_class].pop_back();

    stats.user_alloc_tot += buffer.class_size;

    return buffer;
  }

  /**
   * @brief Try to reserve \p size (rounded to 2MB) bytes as huge pages by
   * adding hugepage-backed Buffers to freelists.
   *
   * @return True if the allocation succeeds. False if the allocation fails
   * because no more hugepages are available.
   *
   * @throw runtime_error if allocation is catastrophic (i.e., it fails
   * due to reasons other than out-of-memory).
   */
  bool reserve_hugepages(size_t size) {
    assert(size >= kMaxClassSize);  // We need at least one max-sized buffer
    Buffer buffer = alloc_raw(size);
    if (buffer.buf == nullptr) return false;

    // Add Buffers to the largest class
    size_t num_buffers = size / kMaxClassSize;
    assert(num_buffers >= 1);
    for (size_t i = 0; i < num_buffers; i++) {
      uint8_t *buf = buffer.buf + (i * kMaxClassSize);
      freelist[kNumClasses - 1].push_back(Buffer(buf, kMaxClassSize));
    }
    return true;
  }

  /// Delete the SHM region specified by \p shm_key and \p shm_buf
  void delete_shm(int shm_key, const uint8_t *shm_buf) {
    int shmid = shmget(shm_key, 0, 0);
    if (shmid == -1) {
      switch (errno) {
        case EACCES:
          fprintf(stderr,
                  "HugeAlloc: SHM free error for key %d. Permission denied.",
                  shm_key);
          break;
        case ENOENT:
          fprintf(stderr, "HugeAlloc: SHM free error: No such SHM key %d.\n",
                  shm_key);
          break;
        default:
          fprintf(stderr, "HugeAlloc: SHM free error: A wild SHM error: %s\n",
                  strerror(errno));
          break;
      }

      exit(-1);
    }

    int ret = shmctl(shmid, IPC_RMID, nullptr);  // Please don't fail
    if (ret != 0) {
      fprintf(stderr, "HugeAlloc: Error freeing SHM ID %d\n", shmid);
      exit(-1);
    }

    ret = shmdt(static_cast<void *>(const_cast<uint8_t *>(shm_buf)));
    if (ret != 0) {
      fprintf(stderr, "HugeAlloc: Error freeing SHM buf for key %d.\n",
              shm_key);
      exit(-1);
    }
  }

  std::vector<shm_region_t> shm_list;  /// SHM regions by increasing alloc size
  std::vector<Buffer> freelist[kNumClasses];  /// Per-class freelist

  SlowRand slow_rand;           /// RNG to generate SHM keys
  const size_t numa_node;       /// NUMA node on which all memory is allocated
  size_t prev_allocation_size;  /// Size of previous hugepage reservation

  // Stats
  struct {
    size_t shm_reserved = 0;    /// Total hugepage memory reserved by allocator
    size_t user_alloc_tot = 0;  /// Total memory allocated to user
  } stats;
};

}  // namespace hugealloc
