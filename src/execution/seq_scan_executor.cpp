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
    auto tmp = *it;
    *rid = tmp.GetRid();
    it++;
    if (pred == nullptr || pred->Evaluate(&tmp, &meta->schema_).GetAs<bool>()) {
      auto columns = plan_->OutputSchema()->GetColumns();
      std::vector<bustub::Value> values(columns.size());
      for (int i = 0; i < static_cast<int>(columns.size()); i++) {
        values[i] = columns[i].GetExpr()->Evaluate(&tmp, &meta->schema_);
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
