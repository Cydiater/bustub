//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), meta(nullptr) {}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  meta = exec_ctx_->GetCatalog()->GetTable(table_oid);
  it = meta->table_->Begin(exec_ctx_->GetTransaction());
  end = meta->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto pred = plan_->GetPredicate();
  while (it != end) {
    *tuple = *it;
    it++;
    if (pred->Evaluate(tuple, &meta->schema_).GetAs<bool>()) {
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
