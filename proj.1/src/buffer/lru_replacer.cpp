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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : timer(0), num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (list_.empty()) {
    latch_.unlock();
    return false;
  }
  LRUItem v = list_.front();
  list_.pop_front();
  *frame_id = v.frame_id;
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  list_.remove_if([frame_id](LRUItem item) { return frame_id == item.frame_id; });
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (std::find_if(list_.begin(), list_.end(), [frame_id](LRUItem item) { return item.frame_id == frame_id; }) !=
      list_.end()) {
    latch_.unlock();
    return;
  }
  if (list_.size() == num_pages_) {
    latch_.unlock();
    return;
  }
  list_.emplace_back(frame_id, timer++);
  latch_.unlock();
}

size_t LRUReplacer::Size() { 
  latch_.lock();
  auto size = list_.size();
  latch_.unlock();
  return size; 
}

}  // namespace bustub
