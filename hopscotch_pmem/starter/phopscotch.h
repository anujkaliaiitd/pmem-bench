#pragma once

#include <city.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

namespace phopscotch {

struct bucket_t {
  void *key;
  void *data;
  uint32_t hopinfo;
} __attribute__((aligned(8)));

// Bitmap size used for linear probing in hopscotch hashing
static constexpr size_t kBitmapSize = sizeof(bucket_t::hopinfo) * 8;

struct table_t {
  size_t exponent;
  size_t keylen;
  bucket_t *buckets;
  int _allocated;
};

struct table_t *init(struct table_t *ht, size_t keylen) {
  size_t exponent;
  bucket_t *buckets;

  /* Allocate buckets first */
  exponent = 10;
  buckets = new bucket_t[1 << exponent];
  if (nullptr == buckets) {
    return nullptr;
  }
  memset(buckets, 0, sizeof(struct bucket_t) * (1 << exponent));

  if (nullptr == ht) {
    ht = new table_t();
    if (nullptr == ht) {
      return nullptr;
    }
    ht->_allocated = 1;
  } else {
    ht->_allocated = 0;
  }
  ht->exponent = exponent;
  ht->buckets = buckets;
  ht->keylen = keylen;

  return ht;
}

void release(struct table_t *ht) {
  delete[] ht->buckets;
  if (ht->_allocated) delete ht;
}

void *lookup(struct table_t *ht, void *key) {
  uint32_t h;
  size_t idx;
  size_t i;
  size_t sz;

  sz = 1ull << ht->exponent;
  h = CityHash64(reinterpret_cast<const char *>(key), ht->keylen);
  idx = h & (sz - 1);

  for (i = 0; i < kBitmapSize; i++) {
    if (ht->buckets[idx].hopinfo & (1 << i)) {
      if (0 == memcmp(key, ht->buckets[idx + i].key, ht->keylen)) {
        return ht->buckets[idx + i].data;
      }
    }
  }

  return nullptr;
}

int update(struct table_t *ht, void *key, void *data) {
  uint32_t h;
  size_t idx;
  size_t i;
  size_t sz;
  size_t off;
  size_t j;

  sz = 1ull << ht->exponent;
  h = CityHash64(reinterpret_cast<const char *>(key), ht->keylen);
  idx = h & (sz - 1);

  // Linear probing to find an empty bucket
  for (i = idx; i < sz; i++) {
    if (nullptr == ht->buckets[i].key) {
      /* Found an available bucket */
      while (i - idx >= kBitmapSize) {
        for (j = 1; j < kBitmapSize; j++) {
          if (ht->buckets[i - j].hopinfo) {
            off =
                static_cast<size_t>(__builtin_ctz(ht->buckets[i - j].hopinfo));
            if (off >= j) continue;

            ht->buckets[i].key = ht->buckets[i - j + off].key;
            ht->buckets[i].data = ht->buckets[i - j + off].data;
            ht->buckets[i - j + off].key = nullptr;
            ht->buckets[i - j + off].data = nullptr;
            ht->buckets[i - j].hopinfo &= ~(1ull << off);
            ht->buckets[i - j].hopinfo |= (1ull << j);
            i = i - j + off;
            break;
          }
        }
        if (j >= kBitmapSize) {
          return -1;
        }
      }

      off = i - idx;
      ht->buckets[i].key = key;
      ht->buckets[i].data = data;
      ht->buckets[idx].hopinfo |= (1ull << off);

      return 0;
    }
  }

  return -1;
}

void *remove(struct table_t *ht, void *key) {
  uint32_t h;
  size_t idx;
  size_t i;
  size_t sz;
  void *data;

  sz = 1ull << ht->exponent;
  h = CityHash64(reinterpret_cast<const char *>(key), ht->keylen);
  idx = h & (sz - 1);

  for (i = 0; i < kBitmapSize; i++) {
    if (ht->buckets[idx].hopinfo & (1 << i)) {
      if (0 == memcmp(key, ht->buckets[idx + i].key, ht->keylen)) {
        data = ht->buckets[idx + i].data;
        ht->buckets[idx].hopinfo &= ~(1ull << i);
        ht->buckets[idx + i].key = nullptr;
        ht->buckets[idx + i].data = nullptr;
        return data;
      }
    }
  }

  return nullptr;
}
}  // namespace phopscotch
