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
      size_(0),
      mu(new std::mutex) {
  LOG_DEBUG("leaf_max_size = %d internal_max_size = %d", leaf_max_size_, internal_max_size_);
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree() { delete mu; }

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  mu->lock();
  auto size = size_;
  mu->unlock();
  return size == 0;
}

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
  // std::cout << "try get " << key << std::endl;
  // check empty
  if (IsEmpty()) {
    LOG_DEBUG("try get value from an empty tree");
    return false;
  }
  // fetch root_page_id
  mu->lock();
  auto current_page_id = root_page_id_;
  assert(current_page_id != INVALID_PAGE_ID);
  mu->unlock();
  // fetch root_page
  auto page = buffer_pool_manager_->FetchPage(current_page_id);
  page->RLatch();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  // iterate search
  while (true) {
    if (!b_page->IsLeafPage()) {
      // cast to internal page
      auto i_page = reinterpret_cast<InternalPage *>(b_page);
      // fetch child page_id
      auto child_id = i_page->Lookup(key, comparator_);
      assert(current_page_id != INVALID_PAGE_ID);
      // fetch child page
      page = buffer_pool_manager_->FetchPage(child_id);
      page->RLatch();
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(page);
      }
      // cast child page to b_page
      b_page = reinterpret_cast<BPlusTreePage *>(page);
      // cast parent page to page
      page = reinterpret_cast<Page *>(i_page);
      auto page_id = page->GetPageId();
      // clear from page from transaction
      if (transaction != nullptr) {
        assert(transaction->GetPageSet()->front()->GetPageId() == page_id);
        transaction->GetPageSet()->pop_front();
      }
      // release front page
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
    } else {
      // cast b_page to l_page
      auto l_page = reinterpret_cast<LeafPage *>(b_page);
      // cast b_page to page
      page = reinterpret_cast<Page *>(b_page);
      // prepare rid
      RID rid;
      auto ret = false;
      if (l_page->Lookup(key, &rid, comparator_)) {
        // found
        result->push_back(rid);
        ret = true;
      }
      // clear from transaction
      if (transaction != nullptr) {
        assert(transaction->GetPageSet()->front()->GetPageId() == l_page->GetPageId());
        transaction->GetPageSet()->pop_front();
      }
      // relase page
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
      return ret;
    }
  }
  // not reachable
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
  // std::cout << "insert " << key << std::endl;
  // case empty
  mu->lock();
  if (size_ == 0) {
    StartNewTree(key, value);
    mu->unlock();
    return true;
  }
  mu->unlock();

  auto ret = InsertIntoLeaf(key, value, transaction);
  // LOG_DEBUG("size = %d", size_);
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
  // LOG_DEBUG("key %lld: start new tree", key.ToString());
  // new a page and write latch
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  page->WLatch();
  // fill as root node
  auto root_page = reinterpret_cast<LeafPage *>(page);
  root_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  // update meta info
  root_page_id_ = page_id;
  size_ = root_page->Insert(key, value, comparator_);
  assert(size_ == 1);
  // release page
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true);
  // update root page id
  UpdateRootPageId(1);
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
  assert(transaction != nullptr);
  // fetch root page_id
  // LOG_DEBUG("key %lld: start", key.ToString());
  mu->lock();
  auto page_id = root_page_id_;
  // fetch root page
  auto page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  // LOG_DEBUG("key %lld: fetch leaf page", key.ToString());
  transaction->AddIntoPageSet(page);
  // cast to b_page
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  // fetch leaf page iteratively
  while (!b_page->IsLeafPage()) {
    // cast to internal page
    auto i_page = reinterpret_cast<InternalPage *>(b_page);
    // fetch child id
    page_id = i_page->Lookup(key, comparator_);
    assert(page_id != INVALID_PAGE_ID);
    // fetch child
    page = buffer_pool_manager_->FetchPage(page_id);
    // latch child and add to transaction
    page->WLatch();
    transaction->AddIntoPageSet(page);
    // cast to b_page
    b_page = reinterpret_cast<BPlusTreePage *>(page);
    // good to release parent? note that we don't need to check
    // this for the root page
    if (i_page->GetSize() < i_page->GetMaxSize() && !i_page->IsRootPage()) {
      // fetch page set
      auto pages = transaction->GetPageSet();
      auto i_page_id = i_page->GetPageId();
      // LOG_DEBUG("release pages before #%d", i_page_id);
      // remove front pages
      while (true) {
        // fetch front page
        assert(!pages->empty());
        auto front = pages->front();
        auto page_id = front->GetPageId();
        auto is_root = reinterpret_cast<BPlusTreePage *>(front)->IsRootPage();
        // i_page founed, jump out
        if (page_id == i_page_id) {
          break;
        }
        // release this page
        front->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
        pages->pop_front();
        if (is_root) {
          mu->unlock();
        }
      }
    }
  }
  // cast to l_page
  auto dest = reinterpret_cast<LeafPage *>(b_page);
  auto ret = false;
  // check if needed to split
  // LOG_DEBUG("key %lld: perform insert", key.ToString());
  if (dest->GetSize() == dest->GetMaxSize()) {
    // split dest to new_node, we have write latch of new_node
    auto new_node = Split(dest);
    transaction->GetPageSet()->push_back(reinterpret_cast<Page *>(new_node));
    assert(dest->GetSize() < dest->GetMaxSize());
    assert(new_node != nullptr);
    // perform insert to either dest or new_node
    if (comparator_(key, new_node->KeyAt(0)) >= 0) {
      auto old_size = new_node->GetSize();
      auto new_size = new_node->Insert(key, value, comparator_);
      ret = (new_size == old_size + 1);
    } else {
      auto old_size = dest->GetSize();
      auto new_size = dest->Insert(key, value, comparator_);
      ret = (new_size == old_size + 1);
    }
    // fetch middle key
    auto middle_key = new_node->KeyAt(0);
    // maintain NextPageId
    new_node->SetNextPageId(dest->GetNextPageId());
    dest->SetNextPageId(new_node->GetPageId());
    // overflow to parent
    InsertIntoParent(dest, middle_key, new_node, transaction);
  } else {
    // simple insert
    assert(dest->GetSize() < dest->GetMaxSize());
    auto old_size = dest->GetSize();
    auto new_size = dest->Insert(key, value, comparator_);
    ret = (new_size == old_size + 1);
  }
  // LOG_DEBUG("key %lld: release transcation", key.ToString());
  // release transaction
  auto pages = transaction->GetPageSet();
  while (!pages->empty()) {
    page = pages->back();
    page_id = page->GetPageId();
    auto is_root = reinterpret_cast<BPlusTreePage *>(page)->IsRootPage();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    pages->pop_back();
    if (is_root) {
      mu->unlock();
    }
  }
  // LOG_DEBUG("key %lld: update size", key.ToString());
  // update size
  if (ret) {
    mu->lock();
    size_ += 1;
    mu->unlock();
  }
  return ret;
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
  assert(page != nullptr);
  page->WLatch();
  auto new_node = reinterpret_cast<N *>(page);
  if (node->IsLeafPage()) {
    new_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    new_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  }
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &_key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // check nullptr
  assert(new_node != nullptr);
  assert(transaction != nullptr);
  // pre redistribute to balance
  KeyType key = _key;
  if (new_node->GetSize() < new_node->GetMinSize()) {
    if (new_node->IsLeafPage()) {
      LeafPage *leaf[2] = {reinterpret_cast<LeafPage *>(old_node), reinterpret_cast<LeafPage *>(new_node)};
      leaf[0]->MoveLastToFrontOf(leaf[1], _key, buffer_pool_manager_);
      key = leaf[1]->KeyAt(0);
    } else {
      InternalPage *leaf[2] = {reinterpret_cast<InternalPage *>(old_node), reinterpret_cast<InternalPage *>(new_node)};
      leaf[0]->MoveLastToFrontOf(leaf[1], _key, buffer_pool_manager_);
      key = leaf[1]->KeyAt(0);
    }
  } else if (old_node->GetSize() < old_node->GetMinSize()) {
    if (old_node->IsLeafPage()) {
      LeafPage *leaf[2] = {reinterpret_cast<LeafPage *>(old_node), reinterpret_cast<LeafPage *>(new_node)};
      leaf[1]->MoveFirstToEndOf(leaf[0], _key, buffer_pool_manager_);
      key = leaf[1]->KeyAt(0);
    } else {
      InternalPage *leaf[2] = {reinterpret_cast<InternalPage *>(old_node), reinterpret_cast<InternalPage *>(new_node)};
      leaf[1]->MoveFirstToEndOf(leaf[0], _key, buffer_pool_manager_);
      key = leaf[1]->KeyAt(0);
    }
  }
  if (old_node->IsRootPage()) {
    page_id_t page_id;
    // new internal page
    auto page = buffer_pool_manager_->NewPage(&page_id);
    page->WLatch();
    transaction->GetPageSet()->push_back(page);
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    // setup root
    parent->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    parent->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // link children to parent
    old_node->SetParentPageId(parent->GetPageId());
    new_node->SetParentPageId(parent->GetPageId());
    // update root page_id
    root_page_id_ = page_id;
    UpdateRootPageId();
    return;
  }
  // fetch parent page_id
  auto page_id = old_node->GetParentPageId();
  // fetch the parent, we have fetched wlatch of parent
  auto page = buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  auto parent = reinterpret_cast<InternalPage *>(page);
  // check if ok to insert directly
  if (parent->GetSize() < parent->GetMaxSize()) {
    // update parent
    auto old_size = parent->GetSize();
    auto new_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    assert(new_size == old_size + 1);
    // update new child
    new_node->SetParentPageId(parent->GetPageId());
    return;
  }
  assert(parent->GetSize() == parent->GetMaxSize());
  // split parent
  auto new_parent = Split(parent);
  transaction->AddIntoPageSet(reinterpret_cast<Page *>(new_parent));
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
  // overflow to parent
  auto parent_key = reinterpret_cast<InternalPage *>(new_parent)->KeyAt(0);
  InsertIntoParent(parent, parent_key, new_parent, transaction);
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
  // std::cout << "try remove " << key << std::endl;
  if (IsEmpty()) {
    return;
  }
  assert(transaction != nullptr);
  // fetch root page_id
  mu->lock();
  auto page_id = root_page_id_;
  // fetch page
  auto page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  mu->unlock();
  transaction->AddIntoPageSet(page);
  // cast to b_page
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  // fetch leaf page iteratively
  while (!b_page->IsLeafPage()) {
    // cast to internal
    auto i_page = reinterpret_cast<InternalPage *>(b_page);
    // fetch child id
    page_id = i_page->Lookup(key, comparator_);
    assert(page_id != INVALID_PAGE_ID);
    // fetch child
    page = buffer_pool_manager_->FetchPage(page_id);
    // latch child and add to transaction
    // LOG_DEBUG("latch #%d", page->GetPageId());
    page->WLatch();
    transaction->AddIntoPageSet(page);
    // cast to b_page
    b_page = reinterpret_cast<BPlusTreePage *>(page);
    if (i_page->GetSize() > i_page->GetMinSize() && !i_page->IsRootPage()) {
      auto pages = transaction->GetPageSet();
      auto i_page_id = i_page->GetPageId();
      while (true) {
        // fetch front page
        assert(!pages->empty());
        auto front = pages->front();
        auto page_id = front->GetPageId();
        if (page_id == i_page_id) {
          break;
        }
        // release this page
        // LOG_DEBUG("unlatch #%d", front->GetPageId());
        front->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
        pages->pop_front();
      }
    }
  }
  // cast to l_page
  auto l_page = reinterpret_cast<LeafPage *>(b_page);
  // do delete
  auto old_size = l_page->GetSize();
  auto new_size = l_page->RemoveAndDeleteRecord(key, comparator_);
  // delete not effect
  if (new_size == old_size) {
    LOG_DEBUG("remove not effect");
  } else {
    // underflow to parent
    if (l_page->GetSize() < l_page->GetMinSize()) {
      CoalesceOrRedistribute(l_page, transaction);
    }
  }
  // release transaction
  auto pages = transaction->GetPageSet();
  while (!pages->empty()) {
    page = pages->front();
    page_id = page->GetPageId();
    // LOG_DEBUG("unlatch %d", page->GetPageId());
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    pages->pop_front();
  }
  // delete pages
  auto deleted_page_ids = transaction->GetDeletedPageSet();
  for (auto page_id : *deleted_page_ids) {
    // LOG_DEBUG("delete %d", page_id);
    buffer_pool_manager_->DeletePage(page_id);
  }
  if (new_size == old_size - 1) {
    mu->lock();
    size_ -= 1;
    mu->unlock();
  }
  // SanityCheck();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // handle root
  if (node->IsRootPage()) {
    if (AdjustRoot(node)) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return;
  }
  // fetch parent page_id
  auto page_id = node->GetParentPageId();
  assert(page_id != INVALID_PAGE_ID);
  // fetch parent page
  auto page = buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  assert(page != nullptr);
  // cast to i_page
  auto i_page = reinterpret_cast<InternalPage *>(page);
  auto index = i_page->ValueIndex(node->GetPageId());
  // prepare siblings
  N *siblings[2] = {nullptr, nullptr};
  int rich_pos = -1;
  // fill siblings
  if (index - 1 >= 0) {
    page_id = i_page->ValueAt(index - 1);
    assert(page_id != INVALID_PAGE_ID);
    page = buffer_pool_manager_->FetchPage(page_id);
    buffer_pool_manager_->UnpinPage(page_id, true);
    // LOG_DEBUG("latch #%d", page->GetPageId());
    page->WLatch();
    transaction->AddIntoPageSet(page);
    siblings[0] = reinterpret_cast<N *>(page);
    siblings[1] = node;
    rich_pos = 0;
  } else if (index + 1 < i_page->GetSize()) {
    page_id = i_page->ValueAt(index + 1);
    assert(page_id != INVALID_PAGE_ID);
    siblings[0] = node;
    page = buffer_pool_manager_->FetchPage(page_id);
    buffer_pool_manager_->UnpinPage(page_id, true);
    // LOG_DEBUG("latch #%d", page->GetPageId());
    page->WLatch();
    transaction->AddIntoPageSet(page);
    siblings[1] = reinterpret_cast<N *>(page);
    rich_pos = 1;
  } else {
    assert(false);
  }
  // case coalesce
  if (siblings[0]->GetSize() + siblings[1]->GetSize() <= siblings[0]->GetMaxSize()) {
    Coalesce(i_page, siblings, transaction);
    return;
  }
  // case redistribute
  Redistribute(i_page, siblings, rich_pos);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(InternalPage *i_page, N *siblings[], Transaction *transaction) {
  // do coalesce
  auto index = i_page->ValueIndex(siblings[1]->GetPageId());
  auto key = i_page->KeyAt(index);
  siblings[1]->MoveAllTo(siblings[0], key, buffer_pool_manager_);
  i_page->Remove(index);
  // delete right sibling
  transaction->AddIntoDeletedPageSet(siblings[1]->GetPageId());
  // check parent
  if (i_page->GetSize() < i_page->GetMinSize()) {
    CoalesceOrRedistribute(i_page, transaction);
  }
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
void BPLUSTREE_TYPE::Redistribute(InternalPage *i_page, N *siblings[], int rich_pos) {
  // check
  assert(siblings[rich_pos]->GetSize() > siblings[rich_pos ^ 1]->GetSize());
  // fetch index of right sibling
  auto index = i_page->ValueIndex(siblings[1]->GetPageId());
  assert(index != -1);
  // fetch key of right sibling
  auto key = i_page->KeyAt(index);
  // redistribute
  if (rich_pos == 1) {
    siblings[1]->MoveFirstToEndOf(siblings[0], key, buffer_pool_manager_);
  } else {
    siblings[0]->MoveLastToFrontOf(siblings[1], key, buffer_pool_manager_);
  }
  i_page->SetKeyAt(index, siblings[1]->KeyAt(0));
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
    // update root_page_id
    UpdateRootPageId(0);
    return true;
  }
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    auto i_page = reinterpret_cast<InternalPage *>(old_root_node);
    auto page_id = i_page->ValueAt(0);
    if (page_id != INVALID_PAGE_ID) {
      // update root_page_id
      root_page_id_ = page_id;
      UpdateRootPageId(0);
      // set up new root
      auto page = buffer_pool_manager_->FetchPage(page_id);
      i_page = reinterpret_cast<InternalPage *>(page);
      i_page->SetParentPageId(INVALID_PAGE_ID);
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
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  auto dummy = KeyType();
  auto page = FindLeafPage(dummy, true);
  assert(page != nullptr);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(page), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key, false);
  assert(page != nullptr);
  auto leaf = reinterpret_cast<LeafPage *>(page);
  auto offset = leaf->KeyIndex(key, comparator_);
  assert(offset >= 0);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(page), offset);
}

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
  // std::cout << "try find leaf with key " << key << " leftMost " << leftMost << std::endl;
  // fetch root page_id
  mu->lock();
  auto page_id = root_page_id_;
  mu->unlock();
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  // fetch root page
  auto page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  // cast to b_page
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  while (!b_page->IsLeafPage()) {
    // cast to i_page
    auto i_page = reinterpret_cast<InternalPage *>(page);
    // fetch child page_id
    if (leftMost) {
      page_id = i_page->ValueAt(0);
    } else {
      page_id = i_page->Lookup(key, comparator_);
    }
    assert(page_id != INVALID_PAGE_ID);
    // fetch child page
    page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    page->WLatch();
    // release current page
    reinterpret_cast<Page *>(b_page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(b_page->GetPageId(), false);
    // cast to b_page
    b_page = reinterpret_cast<BPlusTreePage *>(page);
  }
  return reinterpret_cast<Page *>(b_page);
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
    if (key == -1) {
      break;
    }

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    auto ret = Insert(index_key, rid, transaction);
    if (!ret) {
      LOG_DEBUG("insert failed");
    }
    key = -1;
  }
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetValueFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    auto result = std::vector<RID>();
    auto ret = GetValue(index_key, &result, transaction);
    assert(result.size() == 1);
    if (!ret) {
      LOG_DEBUG("get failed");
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
      assert(i_page->GetSize() >= i_page->GetMinSize());
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
      // assert(page->GetPinCount() == 1);
      assert(page->GetPageId() == page_id);
      auto b_page = reinterpret_cast<BPlusTreePage *>(page);
      assert(b_page->GetPageId() == page_id);
      sum += SanityCheck(b_page);
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  // LOG_DEBUG("#%d sum = %d", page->GetPageId(), sum);
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
  // assert(page->GetPinCount() == 1);
  auto b_page = reinterpret_cast<BPlusTreePage *>(page);
  // kick out the recursive check
  auto sum = SanityCheck(b_page);
  // check size
  if (sum != size_) {
    LOG_DEBUG("sum = %d size = %d", sum, size_);
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
