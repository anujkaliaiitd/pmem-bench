/**
 * @file huge_alloc.h
 * @brief A header-only fast hugepage allocator with no dependencies
 * @author Anuj Kalia
 * @date 2018-09-25
 */

#pragma once

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <numaif.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <unistd.h>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace hugealloc {

static constexpr size_t kHugepageSize = (2 * 1024 * 1024);  ///< Hugepage size

/// Enable returning physical addresses in Buffers
static constexpr bool kEnablePhysAddrs = true;

static constexpr uint64_t kInvalidPhysAddr = UINT64_MAX;

//
// Utility classes for HugeAlloc
//

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

/// High-quality but slow RNG
class SlowRand {
  std::random_device rand_dev;  // Non-pseudorandom seed for twister
  std::mt19937_64 mt;
  std::uniform_int_distribution<uint64_t> dist;

 public:
  SlowRand() : mt(rand_dev()), dist(0, UINT64_MAX) {}

  inline uint64_t next_u64() { return dist(mt); }
};

/// Virtual-to-physical address translation
class Virt2Phy {
  static constexpr size_t kPfnMasSize = 8;

 public:
  Virt2Phy() {
    fd = open("/proc/self/pagemap", O_RDONLY);
    if (fd < 0) {
      printf("%s(): cannot open /proc/self/pagemap\n", strerror(errno));
      exit(-1);
    }

    page_size = static_cast<size_t>(getpagesize());  // Standard page size
  }

  ~Virt2Phy() { close(fd); }

  // Get physical address of any mapped virtual address in the current process.
  uint64_t translate(const void *virtaddr) {
    auto virt_pfn = static_cast<unsigned long>(
        reinterpret_cast<uint64_t>(virtaddr) / page_size);
    size_t offset = sizeof(uint64_t) * virt_pfn;

    if (lseek(fd, static_cast<long>(offset), SEEK_SET) == -1) {
      printf("%s(): seek error in /proc/self/pagemap\n", strerror(errno));
      close(fd);
      return 0;
    }

    uint64_t page;
    int ret = read(fd, &page, kPfnMasSize);
    if (ret < 0) {
      fprintf(stderr, "cannot read /proc/self/pagemap: %s\n", strerror(errno));
      return 0;
    } else if (static_cast<size_t>(ret) != kPfnMasSize) {
      fprintf(stderr,
              "read %d bytes from /proc/self/pagemap but expected %zu:\n", ret,
              kPfnMasSize);
      return 0;
    }

    // The pfn (page frame number) are bits 0-54 (see pagemap.txt in linux
    // Documentation)
    if ((page & 0x7fffffffffffffULL) == 0) return 0;

    uint64_t physaddr = ((page & 0x7fffffffffffffULL) * page_size) +
                        (reinterpret_cast<uint64_t>(virtaddr) % page_size);

    return physaddr;
  }

 private:
  int fd;
  size_t page_size;
};

//
// Definitions for HugeAlloc
//

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

/// The hugepage allocator returns Buffers
class Buffer {
 public:
  Buffer() {}

  ~Buffer() {
    // The hugepage allocator frees up memory for its Buffers
  }

  Buffer(uint8_t *buf, size_t class_size, uint64_t phys_addr)
      : buf(buf), class_size(class_size), phys_addr(phys_addr) {}

  /// Return a string representation of this Buffer (excluding lkey)
  std::string to_string() const {
    char ret[100];
    sprintf(ret, "[buf %p, class sz %zu, phys_addr %p]", buf, class_size,
            reinterpret_cast<void *>(phys_addr));
    return std::string(ret);
  }

  /// The backing memory of this Buffer. The Buffer is invalid if this is null.
  uint8_t *buf;
  size_t class_size;  ///< The size of the hugealloc class used for this Buffer

  /// For Buffers <= kHugepageSize, phys_addr is the physical address of buf.
  /// For larger Buffers, phys_addr is invalid, because larger Buffers may not
  /// be contiguous in physical memory.
  uint64_t phys_addr;
};

/// Return the index of the most significant bit of x. The index of the 2^0
/// bit is 1. (x = 0 returns 0, x = 1 returns 1.)
static inline size_t msb_index(int x) {
  assert(x < INT32_MAX / 2);
  int index;
  asm("bsrl %1, %0" : "=r"(index) : "r"(x << 1));
  return static_cast<size_t>(index);
}

/**
 * A hugepage allocator that uses per-class freelists. The minimum class size
 * is kMinClassSize, and class size increases by a factor of 2 until
 * kMaxClassSize.
 *
 * Large Buffers split into smaller Buffers when needed. Small Buffers never
 * merge into larger Buffers.
 *
 * When a new SHM region is added to the allocator, it is split into Buffers of
 * size kMaxClassSize and added to that class. These Buffers are later split to
 * fill up smaller classes.
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

  // We fill-in physical addresses only when splitting larger Buffers into
  // hugepage-sized buffers
  static_assert(kMaxClassSize >= 2 * kHugepageSize, "");

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
    reserve_hugepages(prev_allocation_size);
  }

  HugeAlloc(size_t numa_node) : numa_node(numa_node) {
    prev_allocation_size = kMaxClassSize;
    reserve_hugepages(prev_allocation_size);
  }

  ~HugeAlloc() {
    for (shm_region_t &shm_region : shm_list) {
      int ret =
          shmdt(static_cast<void *>(const_cast<uint8_t *>(shm_region.buf)));
      if (ret != 0) {
        fprintf(stderr, "HugeAlloc: Error freeing SHM buf for key %d.\n",
                shm_region.shm_key);
        exit(-1);
      }
    }
  }

  /**
   * @brief Reserve size bytes as huge pages by adding hugepage-backed Buffers
   * to freelists.
   *
   * @return True if the allocation succeeds. False if the allocation fails
   * because no more hugepages are available.
   */
  bool reserve_hugepages(size_t size) {
    Buffer buffer = alloc_raw(size);
    if (buffer.buf == nullptr) return false;

    // Add Buffers to the largest class
    size_t num_buffers = size / kMaxClassSize;
    assert(num_buffers >= 1);
    for (size_t i = 0; i < num_buffers; i++) {
      uint8_t *buf = buffer.buf + (i * kMaxClassSize);

      // These Buffers are larger than 2 MB, so we don't have a physical address
      freelist[kNumClasses - 1].push_back(
          Buffer(buf, kMaxClassSize, kInvalidPhysAddr));
    }
    return true;
  }

  /**
   * @brief Allocate memory using raw SHM operations, always bypassing the
   * allocator's freelists. Unlike alloc(), the size of the allocated memory
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
            return Buffer(nullptr, 0, kInvalidPhysAddr);

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

    rt_assert(reinterpret_cast<size_t>(shm_buf) % kHugepageSize == 0,
              "SHM buffer isn't aligned to hugepage size");

    // Mark the SHM region for deletion when this process exits
    shmctl(shm_id, IPC_RMID, nullptr);

    // Bind the buffer to the NUMA node
    const unsigned long nodemask =
        (1ul << static_cast<unsigned long>(numa_node));
    long ret = mbind(shm_buf, size, MPOL_BIND, &nodemask, 32, 0);
    rt_assert(ret == 0,
              "HugeAlloc: mbind() failed. Key " + std::to_string(shm_key));

    // Page-in all hugepages so they get assigned physical addresses.
    // This is slow!
    for (size_t i = 0; i < size; i += kHugepageSize) shm_buf[i] = 0;

    // Save the SHM region so we can free it later
    shm_list.push_back(shm_region_t(shm_key, shm_buf, size));
    stats.shm_reserved += size;

    // buffer.class_size is invalid because we didn't allocate from a class
    if (size <= kHugepageSize) {
      return Buffer(shm_buf, SIZE_MAX, v2p.translate(shm_buf));
    } else {
      return Buffer(shm_buf, SIZE_MAX, kInvalidPhysAddr);
    }
  }

  /**
   * @brief Allocate a Buffer using the allocator's freelists, i.e., the max
   * size that can be allocated is the max freelist class size.
   *
   * @param size The minimum size of the allocated Buffer. size need not
   * equal a class size.
   *
   * @return The allocated buffer. The buffer is invalid if we ran out of
   * memory. The class_size of the allocated Buffer is equal to a
   * HugeAlloc class size.
   *
   * @throw runtime_error if size is too large for the allocator, or if
   * hugepage reservation failure is catastrophic
   */
  Buffer alloc(size_t size) {
    assert(size <= kMaxClassSize);
    const size_t size_class = get_class(size);

    if (freelist[size_class].empty()) {
      // There is no free Buffer in this class. Find the first larger class with
      // free Buffers.
      size_t next_class = size_class + 1;
      for (; next_class < kNumClasses; next_class++) {
        if (!freelist[next_class].empty()) break;
      }

      if (next_class == kNumClasses) {
        // There's no larger size class with free pages, we need to allocate
        // more hugepages. This adds some Buffers to the largest class.
        prev_allocation_size *= 2;
        bool success = reserve_hugepages(prev_allocation_size);

        if (!success) {
          prev_allocation_size /= 2;  // Restore the previous allocation
          return Buffer(nullptr, 0, kInvalidPhysAddr);
        }

        next_class = kNumClasses - 1;
      }

      while (next_class != size_class) {
        split(next_class);
        next_class--;
      }
    }

    assert(!freelist[size_class].empty());

    Buffer buffer = freelist[size_class].back();
    freelist[size_class].pop_back();
    stats.user_alloc_tot += buffer.class_size;
    return buffer;
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

  /// Reference function for the optimized get_class function above
  inline size_t get_class_slow(size_t size) {
    assert(size >= 1 && size <= kMaxClassSize);

    size_t size_class = 0;             // The size class for size
    size_t class_lim = kMinClassSize;  // The max size for size_class
    while (size > class_lim) {
      size_class++;
      class_lim *= 2;
    }

    return size_class;
  }

  /// Split one Buffers from class size_class into two Buffers of the previous
  /// class. If physical addresses are enabled, we do virtual-to-physical
  /// address translation when splitting into 2 MB Buffers.
  inline void split(size_t size_class) {
    Buffer buffer = freelist[size_class].back();
    freelist[size_class].pop_back();

    const size_t split_class_size = buffer.class_size / 2;

    // Start with invalid physical addresses
    auto buffer_0 = Buffer(buffer.buf, split_class_size, kInvalidPhysAddr);
    auto buffer_1 = Buffer(buffer.buf + split_class_size, split_class_size,
                           kInvalidPhysAddr);

    if (kEnablePhysAddrs && split_class_size <= kHugepageSize) {
      if (split_class_size < kHugepageSize) {
        // Inherit physical addresses from parent Buffer
        buffer_0.phys_addr = buffer.phys_addr;
        buffer_1.phys_addr = buffer.phys_addr + split_class_size;
      } else {
        // Here, split_class_size == kHugepageSize
        buffer_0.phys_addr = v2p.translate(buffer_0.buf);
        buffer_1.phys_addr = v2p.translate(buffer_1.buf);
      }
    }

    freelist[size_class - 1].push_back(buffer_0);
    freelist[size_class - 1].push_back(buffer_1);
  }

  std::vector<shm_region_t> shm_list;  /// SHM regions by increasing alloc size
  std::vector<Buffer> freelist[kNumClasses];  /// Per-class freelist

  SlowRand slow_rand;           /// RNG to generate SHM keys
  const size_t numa_node;       /// NUMA node on which all memory is allocated
  size_t prev_allocation_size;  /// Size of previous hugepage reservation
  Virt2Phy v2p;                 /// The virtual-to-physical address translator

  // Stats
  struct {
    size_t shm_reserved = 0;    /// Total hugepage memory reserved by allocator
    size_t user_alloc_tot = 0;  /// Total memory allocated to user
  } stats;
};

}  // namespace hugealloc
