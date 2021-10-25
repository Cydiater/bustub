//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  auto table_oid = plan_->TableOid();
  auto catalog = exec_ctx_->GetCatalog();
  table_meta = catalog->GetTable(table_oid);
  indexes = catalog->GetTableIndexes(table_meta->name_);
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next(Tuple *tuple, RID *rid) {
  if (child_executor_ != nullptr) {
    if (child_executor_->Next(tuple, rid)) {
      auto new_tuple = GenerateUpdatedTuple(*tuple);
      table_meta->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
      for (auto index : indexes) {
        auto old_key = tuple->KeyFromTuple(table_meta->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
        auto new_key = new_tuple.KeyFromTuple(table_meta->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
