//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *buffer_pool_manager, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *_page,
                int _offset);
  ~IndexIterator();

  bool isEnd() const;

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (isEnd() && itr.isEnd()) {
      return true;
    }
    if (isEnd()) {
      return false;
    }
    if (itr.isEnd()) {
      return false;
    }
    return page->GetPageId() == itr.page->GetPageId() && offset == itr.offset;
  }

  bool operator!=(const IndexIterator &itr) const { return !((*this) == itr); }

 private:
  int offset = 0;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page = nullptr;
  BufferPoolManager *buffer_pool_manager_ = nullptr;
};

}  // namespace bustub
