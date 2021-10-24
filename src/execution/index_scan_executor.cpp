//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

#define BI BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_meta(nullptr), table_meta(nullptr) {}

void IndexScanExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    index_meta = catalog->GetIndex(plan_->GetIndexOid());
    auto bp_index = static_cast<BI*>(index_meta->index_.get());
    auto table_name = index_meta->table_name_;
    table_meta = catalog->GetTable(table_name);
    it = bp_index->GetBeginIterator();
    end = bp_index->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) { 
    auto pred = plan_->GetPredicate();
    while (it != end) {
        *rid = (*it).second;
        *tuple = Tuple(*rid);
        ++it;
        if (pred->Evaluate(tuple, &table_meta->schema_).GetAs<bool>()) {
            return true;
        }
    }
    return false;
}

}  // namespace bustub
