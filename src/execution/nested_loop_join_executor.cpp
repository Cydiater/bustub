//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor(std::move(left_executor)),
      right_executor(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  if (left_executor != nullptr) {
    left_executor->Init();
  }
  if (right_executor != nullptr) {
    right_executor->Init();
  }
  left_offset = 0;
  tuple_from_right = nullptr;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (left_executor == nullptr || right_executor == nullptr) {
    return false;
  }
  // fetch tuples from left, and
  // set up the first tuple from
  // right
  RID tmp_rid;
  if (tuple_from_right == nullptr) {
    Tuple *tmp_tuple = new Tuple();
    while (left_executor->Next(tmp_tuple, &tmp_rid)) {
      tuples_from_left.push_back(tmp_tuple);
      tmp_tuple = new Tuple();
    }
    left_offset = 0;
    tuple_from_right = new Tuple();
    LOG_DEBUG("fetch %lu tuples from left", tuples_from_left.size());
    if (!right_executor->Next(tuple_from_right, &tmp_rid)) {
      return false;
    }
  }
  if (left_offset == static_cast<int>(tuples_from_left.size())) {
    return false;
  }
  while (true) {
    for (int i = left_offset; i < static_cast<int>(tuples_from_left.size()); i++) {
      auto tuple_from_left = tuples_from_left[i];
      if (plan_->Predicate()
              ->EvaluateJoin(tuple_from_left, left_executor->GetOutputSchema(), tuple_from_right,
                             right_executor->GetOutputSchema())
              .GetAs<bool>()) {
        auto columns = plan_->OutputSchema()->GetColumns();
        auto values = std::vector<bustub::Value>();
        for (auto &column : columns) {
          values.push_back(column.GetExpr()->EvaluateJoin(tuple_from_left, left_executor->GetOutputSchema(),
                                                          tuple_from_right, right_executor->GetOutputSchema()));
        }
        left_offset = i + 1;
        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
    if (right_executor->Next(tuple_from_right, &tmp_rid)) {
        left_offset = 0;
    } else {
        return false;
    }
  }
}

}  // namespace bustub
