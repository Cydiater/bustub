//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
#include <cassert>
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : head(nullptr), tail(nullptr), num_pages_(num_pages), num_size_(0) {
  for (int i = 0; i < static_cast<int>(num_pages); i++) {
    ref_table_.push_back(new LRUItem(i));
  }
}

LRUReplacer::~LRUReplacer() {
  for (int i = 0; i < static_cast<int>(num_pages_); i++) {
    delete ref_table_[i];
  }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (head == nullptr) {
    latch_.unlock();
    return false;
  }
  *frame_id = head->frame_id;
  auto next = head->next;
  head->next = nullptr;
  head->ok = false;
  if (next != nullptr) {
    next->prev = nullptr;
  }
  head = next;
  num_size_ -= 1;
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  assert(frame_id < static_cast<int>(num_pages_));
  if (ref_table_[frame_id]->ok) {
    ref_table_[frame_id]->ok = false;
    num_size_ -= 1;
    auto prev = ref_table_[frame_id]->prev;
    auto next = ref_table_[frame_id]->next;
    ref_table_[frame_id]->prev = ref_table_[frame_id]->next = nullptr;
    if (prev != nullptr) {
      prev->next = next;
    } else {
      head = next;
    }
    if (next != nullptr) {
      next->prev = prev;
    } else {
      tail = prev;
    }
  }
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  assert(frame_id < static_cast<int>(num_pages_));
  if (ref_table_[frame_id]->ok) {
    latch_.unlock();
    return;
  }
  if (num_size_ == num_pages_) {
    latch_.unlock();
    return;
  }
  if (head == nullptr) {
    head = tail = ref_table_[frame_id];
    ref_table_[frame_id]->ok = true;
    num_size_ = 1;
    latch_.unlock();
    return;
  }
  tail->next = ref_table_[frame_id];
  tail->next->prev = tail;
  tail = tail->next;
  ref_table_[frame_id]->ok = true;
  num_size_ += 1;
  latch_.unlock();
}

size_t LRUReplacer::Size() {
  latch_.lock();
  auto size = num_size_;
  latch_.unlock();
  return size;
}

}  // namespace bustub
