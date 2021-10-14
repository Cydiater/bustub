//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      size_(0) {
  assert(internal_max_size >= 2);
  assert(leaf_max_size >= 1);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return size_ == 0; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  std::cout << "try get " << key << std::endl;
  auto current_page_id = root_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    // out of mem
    if (page == nullptr) {
      throw new Exception(ExceptionType::OUT_OF_MEMORY, "GetValue");
    }
    auto bpt_page = reinterpret_cast<BPlusTreePage *>(page);
    if (!bpt_page->IsLeafPage()) {
      // got internal page, go deeper
      auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
      current_page_id = i_page->Lookup(key, comparator_);
      assert(current_page_id != INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
    } else {
      // got leaf page
      auto l_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
      RID rid;
      // have the excat key
      if (l_page->Lookup(key, &rid, comparator_)) {
        result->push_back(rid);
        buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
        return true;
      }
      buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
      return false;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  std::cout << "try insert " << key << std::endl;
  // case empty
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  auto ret = InsertIntoLeaf(key, value, transaction);
  // SanityCheck();
  return ret;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  // out of memory
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "StartNewTree");
  }
  // fill as root node, initially the root_page just need
  // to be a leaf page.
  auto root_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
  root_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = page_id;
  size_ = root_page->Insert(key, value, comparator_);
  // size must be just one
  assert(size_ == 1);
  // update root page id
  UpdateRootPageId(1);
  // unpin page
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto current_page_id = root_page_id_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *dest = nullptr;
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
      throw new Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoLeaf");
    }
    assert(page->GetPinCount() == 1);
    assert(page->GetPageId() == current_page_id);
    auto b_page = reinterpret_cast<BPlusTreePage *>(page);
    // got leaf page, next step
    if (b_page->IsLeafPage()) {
      dest = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
      break;
    }
    // got internal page, go deeper
    auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    current_page_id = i_page->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
    // it's not possible that we access invalid page
    assert(current_page_id != INVALID_PAGE_ID);
  }
  // check if needed to split
  if (dest->GetSize() == dest->GetMaxSize()) {
    auto new_node = Split(dest);
    auto dest_id = dest->GetPageId();
    auto new_id = new_node->GetPageId();
    assert(dest->GetSize() < dest->GetMaxSize());
    assert(new_node != nullptr);
    auto middle_key = new_node->KeyAt(0);
    // maintain NextPageId
    new_node->SetNextPageId(dest->GetNextPageId());
    dest->SetNextPageId(new_node->GetPageId());
    // overflow to parent, drop ownership of dest and new_node
    InsertIntoParent(dest, middle_key, new_node);
    // refetch dest and new_node
    dest = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(dest_id));
    new_node = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(new_id));
    if (comparator_(key, new_node->KeyAt(0)) >= 0) {
      buffer_pool_manager_->UnpinPage(dest->GetPageId(), false);
      dest = new_node;
    } else {
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), false);
    }
  }
  assert(dest->GetSize() < dest->GetMaxSize());
  auto old_size = dest->GetSize();
  auto new_size = dest->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(dest->GetPageId(), true);
  return new_size == old_size + 1;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "Split");
  }
  auto new_node = reinterpret_cast<N *>(page);
  if (node->IsLeafPage()) {
    new_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    new_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  }
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // check nullptr
  assert(new_node != nullptr);
  // root node, create new root
  if (old_node->IsRootPage()) {
    page_id_t page_id;
    // new internal page
    auto page = buffer_pool_manager_->NewPage(&page_id);
    if (page == nullptr) {
      throw new Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    // setup root
    parent->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    parent->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = page_id;
    // link to parent, release old_node and new_node
    old_node->SetParentPageId(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    new_node->SetParentPageId(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    UpdateRootPageId();
    // release parent
    auto ret = buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    assert(ret);
    return;
  }
  // fetch the parent
  auto parent_page_id = old_node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
  // check if ok to insert directly
  if (parent->GetSize() < parent->GetMaxSize()) {
    // update parent
    auto old_size = parent->GetSize();
    auto new_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    assert(new_size == old_size + 1);
    // release parent
    auto ret = buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    assert(ret);
    // update new child
    new_node->SetParentPageId(parent->GetPageId());
    // release old_node and new_node
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }
  assert(parent->GetSize() == parent->GetMaxSize());
  // release old_node, since the split will modify the
  // parent of old_node
  auto old_page_id = old_node->GetPageId();
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), false);
  // split parent
  auto new_parent = Split(parent);
  assert(parent != nullptr);
  // refetch the old_node
  page = buffer_pool_manager_->FetchPage(old_page_id);
  assert(page != nullptr);
  old_node = reinterpret_cast<BPlusTreePage *>(page);
  // select the parent
  auto old_parent = parent;
  if (old_parent->GetPageId() != old_node->GetParentPageId()) {
    old_parent = new_parent;
    assert(new_parent->GetPageId() == old_node->GetParentPageId());
  }
  // update the parent
  auto old_size = old_parent->GetSize();
  auto new_size = old_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  assert(old_size + 1 == new_size);
  // update new child
  new_node->SetParentPageId(old_parent->GetPageId());
  // release old_node and new_node
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  // overflow to parent
  auto parent_key = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_parent)->KeyAt(0);
  InsertIntoParent(parent, parent_key, new_parent);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FindSiblingOf(BPlusTreePage *node) {
  if (node->IsRootPage()) {
    return nullptr;
  }
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  assert(parent_page->GetPinCount() == 1);
  if (parent_page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "FindSiblingOf");
  }
  auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);
  auto index = i_page->ValueIndex(node->GetPageId());
  assert(index >= 0);
  // only one child
  if (index == 0) {
    buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
    return nullptr;
  }
  auto page_id = i_page->ValueAt(index - 1);
  buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  assert(page->GetPinCount() == 1);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "FindSiblingOf");
  }
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  return b_page;
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FindRightSiblingOf(BPlusTreePage *node) {
  if (node->IsRootPage()) {
    return nullptr;
  }
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "FindRightSiblingOf");
  }
  assert(parent_page->GetPinCount() == 1);
  auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);
  auto index = i_page->ValueIndex(node->GetPageId());
  assert(index != -1);
  if (index == i_page->GetSize() - 1) {
    buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
    return nullptr;
  }
  auto page_id = i_page->ValueAt(index + 1);
  buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "FindRightSiblingOf");
  }
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  assert(b_page->GetPageId() == page_id);
  return b_page;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto page = FindLeafPage(key, false);
  if (page == nullptr) {
    return;
  }
  assert(page->GetPinCount() == 1);
  auto l_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
  auto old_size = l_page->GetSize();
  auto new_size = l_page->RemoveAndDeleteRecord(key, comparator_);
  // remove not effect
  if (new_size == old_size) {
    buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
    return;
  }
  size_ -= 1;
  assert(new_size == old_size - 1);
  // do merge or coalesce
  if (l_page->GetSize() <= l_page->GetMinSize()) {
    // this func will transfer onwership
    CoalesceOrRedistribute(l_page);
    // SanityCheck();
    return;
  }
  buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);
  // SanityCheck();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  auto b_page = FindSiblingOf(node);
  auto left_sibling = true;
  // no left sibling, try right
  if (b_page == nullptr) {
    left_sibling = false;
    b_page = FindRightSiblingOf(node);
    // no right sibling
    if (b_page == nullptr) {
      // assume root
      assert(node->IsRootPage());
      auto ret = AdjustRoot(node);
      auto page_id = node->GetPageId();
      buffer_pool_manager_->UnpinPage(page_id, true);
      if (ret) {
        buffer_pool_manager_->DeletePage(page_id);
      }
      return ret;
    }
  }
  auto sibling = reinterpret_cast<N *>(b_page);
  assert(sibling->GetMaxSize() == node->GetMaxSize());
  // case redistribute
  if (node->IsLeafPage()) {
    // leaf node can hold most at MaxSize - 1
    if (sibling->GetSize() + node->GetSize() >= sibling->GetMaxSize()) {
      Redistribute(sibling, node, left_sibling ? -1 : 0);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
  } else {
    // internal node can hold most at MaxSize
    if (sibling->GetSize() + node->GetSize() > sibling->GetMaxSize()) {
      Redistribute(sibling, node, left_sibling ? -1 : 0);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
  }
  // fetch parent
  auto parent_id = node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute");
  }
  assert(page->GetPinCount() == 1);
  assert(page->GetPageId() == parent_id);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
  // do coalesce
  Coalesce(&sibling, &node, &parent_page, left_sibling ? -1 : 0, transaction);
  // move all, child should be deleted
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if (index == 0) {
    std::swap(node, neighbor_node);
  }
  // do coalesce
  auto node_page_id = (*node)->GetPageId();
  auto pos = (*parent)->ValueIndex(node_page_id);
  (*node)->MoveAllTo(*neighbor_node, (*parent)->KeyAt(pos), buffer_pool_manager_);
  (*parent)->Remove(pos);
  // release left node
  auto page_id = (*node)->GetPageId();
  buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->DeletePage(page_id);
  // check parent
  if ((*parent)->GetSize() <= (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // neighbor_node always sit left
  if (index == 0) {
    std::swap(node, neighbor_node);
  }
  // fetch parent
  auto parent_id = node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "Redistribute");
  }
  assert(page->GetPinCount() == 1);
  assert(page->GetPageId() == parent_id);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
  // fetch key
  auto pos = parent_page->ValueIndex(node->GetPageId());
  assert(index != -1);
  auto key = parent_page->KeyAt(pos);
  // redistribute
  if (index == 0) {
    if (node->GetSize() <= neighbor_node->GetSize()) {
      LOG_DEBUG("node->GetSize = %d neighbor_node->GetSize = %d", node->GetSize(), neighbor_node->GetSize());
    }
    assert(node->GetSize() > neighbor_node->GetSize());
    node->MoveFirstToEndOf(neighbor_node, key, buffer_pool_manager_);
    parent_page->SetKeyAt(pos, node->KeyAt(0));
  } else {
    assert(node->GetSize() < neighbor_node->GetSize());
    neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
    parent_page->SetKeyAt(pos, node->KeyAt(0));
  }
  // release parent
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    if (i_page->ValueAt(0) != INVALID_PAGE_ID) {
      root_page_id_ = i_page->ValueAt(0);
      auto page = buffer_pool_manager_->FetchPage(root_page_id_);
      i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
      i_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(i_page->GetPageId(), true);
    } else {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    }
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  auto current_page_id = root_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
      throw new Exception(ExceptionType::OUT_OF_MEMORY, "FindLeafPage");
    }
    assert(page->GetPageId() == current_page_id);
    assert(page->GetPinCount() == 1);
    auto b_page = reinterpret_cast<BPlusTreePage *>(page);
    if (!b_page->IsLeafPage()) {
      // got internal page, go deeper
      auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
      if (leftMost) {
        current_page_id = i_page->ValueAt(0);
      } else {
        current_page_id = i_page->Lookup(key, comparator_);
      }
      assert(current_page_id != INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(i_page->GetPageId(), false);
    } else {
      return page;
    }
  }
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    auto ret = Insert(index_key, rid, transaction);
    if (!ret) {
      LOG_DEBUG("insert failed");
    }
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    std::cout << "try remove " << key << std::endl;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->GetPageId() == INVALID_PAGE_ID) {
    return;
  }
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      if (internal->ValueAt(i) != INVALID_PAGE_ID) {
        ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::SanityCheck(BPlusTreePage *page) {
  auto sum = 0;
  if (page->IsLeafPage()) {
    sum += page->GetSize();
    auto l_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
    for (int i = 0; i < l_page->GetSize() - 1; i++) {
      // assert key increase
      assert(comparator_(l_page->KeyAt(i), l_page->KeyAt(i + 1)) == -1);
    }
  } else {
    auto i_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    if (!i_page->IsRootPage()) {
      assert(i_page->GetSize() > i_page->GetMinSize());
      assert(i_page->GetSize() <= i_page->GetMaxSize());
    }
    for (int i = 0; i < i_page->GetSize(); i++) {
      if (i + 1 < i_page->GetSize()) {
        // assert key increase
        if (comparator_(i_page->KeyAt(i), i_page->KeyAt(i + 1)) != -1) {
          std::cout << i_page->GetPageId() << ": " << i_page->KeyAt(i) << " !< " << i_page->KeyAt(i + 1) << std::endl;
        }
        assert(comparator_(i_page->KeyAt(i), i_page->KeyAt(i + 1)) == -1);
      }
      auto page_id = i_page->ValueAt(i);
      auto page = buffer_pool_manager_->FetchPage(page_id);
      assert(page != nullptr);
      assert(page->GetPinCount() == 1);
      assert(page->GetPageId() == page_id);
      auto b_page = reinterpret_cast<BPlusTreePage *>(page);
      assert(b_page->GetPageId() == page_id);
      sum += SanityCheck(b_page);
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  return sum;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SanityCheck() {
  // fetch root
  auto page_id = root_page_id_;
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "SanityCheck");
  }
  // check from refresh
  assert(page->GetPinCount() == 1);
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  // kick out the recursive check
  auto sum = SanityCheck(b_page);
  // check size
  if (sum != size_) {
    ToString(b_page, buffer_pool_manager_);
  }
  assert(sum == size_);
  buffer_pool_manager_->UnpinPage(page_id, false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
