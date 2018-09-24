#pragma once

#include <assert.h>
#include <city.h>
#include <libpmem.h>
#include "../common.h"

namespace mica {

static constexpr size_t kSlotsPerBucket = 8;
static constexpr size_t kNumRegularBuckets = 8;
static constexpr double kExtraBucketsFraction = .2;
static constexpr size_t kMaxBatchSize = 16;  // = number of redo log entries
static constexpr bool kVerbose = true;

template <typename Key, typename Value>
class HashMap {
 public:
  static_assert(is_power_of_two(kNumRegularBuckets), "");
  enum class State : size_t { kEmpty = 0, kFull, kDelete };  // Slot state

  class Slot {
   public:
    Key key;
    Value value;

    Slot(Key key, Value value) : key(key), value(value) {}
    Slot() {}
  };

  struct Bucket {
    size_t next_extra_bucket_index;  // 1-base; 0 = no extra bucket
    Key key_arr[kSlotsPerBucket];
    Value val_arr[kSlotsPerBucket];
  };

  class RedoLogEntry {
   public:
    static constexpr size_t kInvalidOperationNumber = 0;

    size_t operation_number;  // Operation number of this entry. Zero is invalid
    Key key;
    Value value;
    size_t valid;

    // Align to 256 bytes
    char padding[256 - (sizeof(size_t) + sizeof(Key) + sizeof(Value) +
                        sizeof(size_t))];

    RedoLogEntry(size_t operation_number, Key key, Value value)
        : operation_number(operation_number),
          key(key),
          value(value),
          valid(0) {}
    RedoLogEntry() {}
  };
  static_assert(sizeof(RedoLogEntry) == 256, "");

  HashMap(std::string pmem_file)
      : num_extra_buckets(kNumRegularBuckets * kExtraBucketsFraction),
        num_total_buckets(kNumRegularBuckets + num_extra_buckets),
        invalid_key(get_invalid_key()) {
    int is_pmem;
    uint8_t* pbuf = reinterpret_cast<uint8_t*>(
        pmem_map_file(pmem_file.c_str(), 0 /* length */, 0 /* flags */, 0666,
                      &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr,
              "pmem_map_file() failed. " + std::string(strerror(errno)));
    rt_assert(mapped_len >= kMaxBatchSize * sizeof(RedoLogEntry) +
                                num_total_buckets * sizeof(Bucket),
              "pmem file too small " + std::to_string(mapped_len));
    rt_assert(is_pmem == 1, "File is not pmem");

    // Initialize redo log entries
    redo_log_entry_arr = reinterpret_cast<RedoLogEntry*>(pbuf);
    for (size_t i = 0; i < kMaxBatchSize; i++) {
      redo_log_entry_arr[i].operation_number =
          RedoLogEntry::kInvalidOperationNumber;
    }
    pmem_flush(&redo_log_entry_arr[0], kMaxBatchSize * sizeof(RedoLogEntry));

    // Initialize buckets
    size_t bucket_offset = roundup<256>(kMaxBatchSize * sizeof(RedoLogEntry));
    buckets_ = reinterpret_cast<Bucket*>(&pbuf[bucket_offset]);

    extra_buckets_ =
        reinterpret_cast<Bucket*>(reinterpret_cast<uint8_t*>(buckets_) +
                                  ((kNumRegularBuckets - 1) * sizeof(Bucket)));

    reset();
  }

  ~HashMap() {
    // redo_log_entry_arr = pbuf
    if (redo_log_entry_arr != nullptr)
      pmem_unmap(redo_log_entry_arr, mapped_len);
  }

  static size_t get_hash(const Key& k) {
    return CityHash64(reinterpret_cast<const char*>(&k), sizeof(Key));
  }

  static Key get_invalid_key() {
    Key ret;
    memset(&ret, 0, sizeof(ret));
    return ret;
  }

  void reset() {
    // Initialize both regular and extra buckets
    for (size_t bkt_i = 0; bkt_i < num_total_buckets; bkt_i++) {
      Bucket& bucket = buckets_[bkt_i];

      for (size_t i = 0; i < kSlotsPerBucket; i++) {
        // XXX: Persist
        bucket.key_arr[i] = invalid_key;
        bucket.next_extra_bucket_index = 0;
      }
    }

    // Initialize the free list of extra buckets
    for (size_t i = 0; i < num_extra_buckets; i++) {
      extra_bucket_free_list.push_back(i + 1);  // Extra-buckets are 1-based
    }
  }

  void prefetch_table(uint64_t key_hash) const {
    size_t bucket_index = key_hash % kNumRegularBuckets;
    const Bucket* bucket = &buckets_[bucket_index];

    __builtin_prefetch(bucket, 0, 0);
    __builtin_prefetch(reinterpret_cast<const char*>(bucket) + 64, 0, 0);
    __builtin_prefetch(reinterpret_cast<const char*>(bucket) + 128, 0, 0);
  }

  bool get(const Key& key, Value& out_value) const {
    get(get_hash(key), key, out_value);
  }

  bool get(uint64_t key_hash, const Key& key, Value& out_value) const {
    size_t bucket_index = key_hash % kNumRegularBuckets;
    Bucket* bucket = &buckets_[bucket_index];

    Bucket* located_bucket;
    size_t item_index = find_item_index(bucket, key, &located_bucket);
    if (item_index == kSlotsPerBucket) return false;

    memcpy(&out_value, &located_bucket->val_arr[item_index], sizeof(Value));
    return true;
  }

  bool set(const Key& key, const Value& value) {
    set(get_hash(key), key, value);
  }

  bool set(uint64_t key_hash, const Key& key, const Value& value) {
    // XXX Persist
    size_t bucket_index = key_hash % kNumRegularBuckets;
    Bucket* bucket = &buckets_[bucket_index];
    Bucket* located_bucket;
    size_t item_index = find_item_index(bucket, key, &located_bucket);

    if (item_index == kSlotsPerBucket) {
      item_index = get_empty(bucket, &located_bucket);
      if (item_index == kSlotsPerBucket) return false;
    }

    located_bucket->key_arr[item_index] = key;
    located_bucket->val_arr[item_index] = value;

    return true;
  }

  void print_buckets() const;
  void print_stats() const;
  void reset_stats(bool reset_count);

  bool alloc_extra_bucket(Bucket* bucket) {
    if (extra_bucket_free_list.empty()) return false;
    size_t extra_bucket_index = extra_bucket_free_list.back();
    extra_bucket_free_list.pop_back();

    bucket->next_extra_bucket_index = extra_bucket_index;
    return true;
  }

  void free_extra_bucket(Bucket* bucket);
  void fill_hole(Bucket* bucket, size_t unused_item_index);

  // Starting from \p bucket, fill in \p located_bucket such that located_bucket
  // contains an empty slot. The slot index is the return value.
  size_t get_empty(Bucket* bucket, Bucket** located_bucket) {
    Bucket* current_bucket = bucket;
    while (true) {
      for (size_t item_index = 0; item_index < kNumRegularBuckets;
           item_index++) {
        if (current_bucket->key_arr[item_index] == invalid_key) {
          *located_bucket = current_bucket;
          return item_index;
        }
      }
      if (current_bucket->next_extra_bucket_index == 0) break;
      current_bucket = &extra_buckets_[current_bucket->next_extra_bucket_index];
    }

    // no space; alloc new extra_bucket
    if (alloc_extra_bucket(current_bucket)) {
      *located_bucket =
          &extra_buckets_[current_bucket->next_extra_bucket_index];
      return 0;  // use the first slot (it should be empty)
    } else {
      // no free extra_bucket
      *located_bucket = nullptr;
      return kSlotsPerBucket;
    }
  }

  size_t find_item_index(Bucket* bucket, const Key& key,
                         Bucket** located_bucket) const {
    Bucket* current_bucket = bucket;

    while (true) {
      for (size_t i = 0; i < kSlotsPerBucket; i++) {
        if (current_bucket->key_arr[i] != key) continue;

        if (kVerbose) printf("find item index: %zu\n", i);
        *located_bucket = current_bucket;
        return i;
      }

      if (current_bucket->next_extra_bucket_index != 0) break;
      current_bucket = &extra_buckets_[current_bucket->next_extra_bucket_index];
    }

    if (kVerbose) printf("could not find item index\n");
    *located_bucket = nullptr;
    return kSlotsPerBucket;
  }

  void print_bucket(const Bucket* bucket) const;
  void print_bucket_occupancy();

  std::string name;  // Name of the table
  const size_t num_extra_buckets;
  const size_t num_total_buckets;  // Total of regular and extra buckets
  const Key invalid_key;
  Bucket* buckets_ = nullptr;

  // = (buckets + num_buckets); extra_buckets[0] is not used because index 0
  // indicates "no more extra buckets"
  Bucket* extra_buckets_ = nullptr;

  std::vector<size_t> extra_bucket_free_list;

  size_t mapped_len;
  RedoLogEntry* redo_log_entry_arr;
  size_t cur_operation_number = RedoLogEntry::kInvalidOperationNumber;
};

}  // namespace mica
