//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    auto *page = &pages_[frame_id];
    page->pin_count_ += 1;
    if (page->GetPageId() != page_id) {
      // LOG_DEBUG("page_id = %d but page->GetPageId() = %d", page_id, page->GetPageId());
    }
    replacer_->Pin(frame_id);
    // LOG_DEBUG("fetch #%d at $%d", page_id, frame_id);
    latch_.unlock();
    // assert(page->GetPageId() == page_id);
    return page;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    auto page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    // LOG_DEBUG("erase #%d -> $%d from page_table", page->GetPageId(), page_table_[page->GetPageId()]);
    page_table_.erase(page->GetPageId());
  } else {
    latch_.unlock();
    return nullptr;
  }
  auto page = &pages_[frame_id];
  // page->WLatch();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  // page->WUnlatch();
  page_table_[page_id] = frame_id;
  // LOG_DEBUG("fetch #%d at $%d", page_id, frame_id);
  latch_.unlock();
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    // LOG_DEBUG("#%d not found", page_id);
    // assert(false);
    return true;
  }
  auto frame_id = it->second;
  auto page = &pages_[frame_id];
  if (page->pin_count_ <= 0) {
    // assert(false);
    latch_.unlock();
    return false;
  }
  page->pin_count_ -= 1;
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  // assert(page->GetPageId() == page_id);
  // LOG_DEBUG("release #%d at $%d, pin_count = %d", page_id, frame_id, page->GetPinCount());
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  } else {
    // LOG_DEBUG("pin count of #%d = %d", page_id, page->pin_count_);
    // if (page_id > 0) {
    // assert(false);
    //}
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  auto frame_id = it->second;
  auto page = &pages_[frame_id];
  // page->WLatch();
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  // page->WUnlatch();
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() != 0) {
    if (!replacer_->Victim(&frame_id)) {
      latch_.unlock();
      // assert(false);
      return nullptr;
    }
    auto page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    // LOG_DEBUG("erase #%d -> $%d from page_table", page->GetPageId(), page_table_[page->GetPageId()]);
    page_table_.erase(page->GetPageId());
  } else {
    latch_.unlock();
    // assert(false);
    return nullptr;
  }
  auto page = &pages_[frame_id];
  // page->WLatch();
  *page_id = page->page_id_ = disk_manager_->AllocatePage();
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
  page_table_[page->page_id_] = frame_id;
  // LOG_DEBUG("new #%d from $%d", *page_id, frame_id);
  replacer_->Pin(frame_id);
  latch_.unlock();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    // assert(false);
    return true;
  }
  auto frame_id = it->second;
  auto page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    // assert(false);
    latch_.unlock();
    return false;
  }
  // LOG_DEBUG("delete #%d", page_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  disk_manager_->DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  latch_.lock();
  for (auto kv : page_table_) {
    auto frame_id = kv.second;
    auto page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
  latch_.unlock();
}

}  // namespace bustub
