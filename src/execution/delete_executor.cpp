//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_meta = catalog->GetTable(plan_->TableOid());
  indexes = catalog->GetTableIndexes(table_meta->name_);
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next(Tuple *tuple, RID *rid) {
  if (child_executor_ == nullptr) {
    return false;
  }
  if (child_executor_->Next(tuple, rid)) {
    table_meta->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (auto index : indexes) {
      auto key = tuple->KeyFromTuple(table_meta->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
