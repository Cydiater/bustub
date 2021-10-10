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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : timer(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (list_.empty()) {
    return false;
  }
  LRUItem v = list_.front();
  list_.pop_front();
  *frame_id = v.frame_id;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  list_.remove_if([frame_id](LRUItem item) { return frame_id == item.frame_id; });
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (std::find_if(list_.begin(), list_.end(), [frame_id](LRUItem item) { return item.frame_id == frame_id; }) !=
      list_.end()) {
    return;
  }
  list_.emplace_back(frame_id, timer++);
}

size_t LRUReplacer::Size() { return list_.size(); }

}  // namespace bustub
