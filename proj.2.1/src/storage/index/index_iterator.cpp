/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager,
                                  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *_page, int _offset)
    : offset(_offset), page(_page), buffer_pool_manager_(buffer_pool_manager) {}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page != nullptr) {
    reinterpret_cast<Page *>(page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const {
  if (page == nullptr) {
    return true;
  }
  if (offset < page->GetSize()) {
    return false;
  }
  if (page->GetNextPageId() != INVALID_PAGE_ID) {
    return false;
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(page != nullptr);
  return page->GetItem(offset);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  assert(page != nullptr);
  if (offset + 1 < page->GetSize()) {
    offset += 1;
  } else {
    offset = 0;
    auto page_id = page->GetNextPageId();
    Page *next = nullptr;
    if (page_id != INVALID_PAGE_ID) {
      next = buffer_pool_manager_->FetchPage(page_id);
      next->WLatch();
    }
    reinterpret_cast<Page *>(page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    if (next != nullptr) {
      page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(next);
    } else {
      page = nullptr;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
