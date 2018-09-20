/*
Copyright (c) 2017 Erik Rigtorp <erik@rigtorp.se>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#pragma once

#include <city.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>
#include "common.h"

template <typename Key, typename Value, size_t num_slots>
class HashMap {
  enum class State : size_t { kEmpty, kFull, kDelete };

  class Slot {
   public:
    State state;
    Key key;
    Value value;

    Slot(State state, Key key, Value value)
        : state(state), key(key), value(value) {}
    Slot() {}
  };

 public:
  HashMap(std::string pmem_file) {
    _unused(pmem_file);
    // Map pmem_file and initialize num_slots
  }

  static size_t get_hash(Key &k) {
    return CityHash64(reinterpret_cast<char *>(&k), sizeof(Key));
  }

  bool insert(const Key &key, const Key &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < num_slots; offset++) {
      size_t slot_idx = (hash + offset);
      if (slot_idx >= num_slots) slot_idx -= num_slots;

      if (slot_arr[slot_idx].state != State::kFull) {
        Slot to_insert(Slot::kFull, key, value);
        slot_arr[slot_idx] = to_insert;
      }
    }
  }

  bool get(const Key &key, const Value &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < num_slots; offset++) {
      size_t slot_idx = (hash + offset);
      if (slot_idx >= num_slots) slot_idx -= num_slots;

      auto &slot = slot_arr[slot_idx];
      if (slot.state == State::kFull &&
          memcmp(&slot.key, &key, sizeof(key)) == 0) {
        value = slot.value;
        return true;
      }

      if (slot.state == State::kEmpty) return false;
    }
  }

 private:
  Slot *slot_arr;
};
