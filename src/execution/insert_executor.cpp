//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_meta = catalog->GetTable(table_oid);
  indexes = catalog->GetTableIndexes(table_meta->name_);
  offset = 0;
  if (!plan_->IsRawInsert()) {
    child_executor = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
    child_executor->Init();
  }
}

bool InsertExecutor::Next(Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    if (offset == static_cast<int>(values.size())) {
      return false;
    }
    auto tuple = Tuple(values[offset], &table_meta->schema_);
    auto ret = table_meta->table_->InsertTuple(tuple, rid, exec_ctx_->GetTransaction());
    if (!ret) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert failed");
    }
    for (auto index : indexes) {
      auto key = tuple.KeyFromTuple(table_meta->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    offset += 1;
    return true;
  }
  if (child_executor->Next(tuple, rid)) {
    table_meta->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    for (auto index : indexes) {
      auto key = tuple->KeyFromTuple(table_meta->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
