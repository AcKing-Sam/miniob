/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

using namespace std;
using namespace common;

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
      : group_by_exprs_(std::move(group_by_exprs)), ht_(expressions) {
        aggregate_expressions_ = expressions;
        value_expressions_.reserve(aggregate_expressions_.size());
        ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
          auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
          Expression *child_expr     = aggregate_expr->child().get();
          ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
          value_expressions_.emplace_back(child_expr);
        });
      };

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override {
    ASSERT(children_.size() == 1, "vectorized group by operator only support one child, but got %d", children_.size());
    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }

    while (OB_SUCC(rc = child.next(chunk_))) {
      // traverse each row of chunk_
      int col_id = 0;
      Chunk group_chunks, aggrs_chunks;
      for(auto& group_expr : group_by_exprs_) {
        // std::cout << "group pos: " << group_expr->pos() << std::endl;
        Column col;
        group_expr->get_column(chunk_, col);
        group_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
        group_chunks.column_ptr(col_id)->reference(col);
        col_id ++;
      }
      col_id = 0;
      for(auto aggrs_expr : value_expressions_) {
        Column col;
        aggrs_expr->get_column(chunk_, col);
        aggrs_chunks.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
        aggrs_chunks.column_ptr(col_id)->reference(col);
        col_id ++;
      }
      rc = ht_.add_chunk(group_chunks, aggrs_chunks);
      if (OB_FAIL(rc)) {
        LOG_INFO("failed to add chunks. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (rc == RC::RECORD_EOF) {
      rc = RC::SUCCESS;
    }
    emited_  = false;
    return rc;
  }

  RC next(Chunk &chunk) override {
    if(emited_) {
      return RC::RECORD_EOF;
    }
    
    int col_id = 0;
    for(auto& group_expr : group_by_exprs_) {
      Column col;
      group_expr->get_column(chunk_, col);
      chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
      col_id ++;
    }
    for(auto& aggrs_expr : value_expressions_) {
      Column col;
      aggrs_expr->get_column(chunk_, col);
      chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
      col_id ++;
    }
    
    StandardAggregateHashTable::Scanner sc(&ht_);
    sc.open_scan();
    while(OB_SUCC(sc.next(chunk))) {

    }
    emited_ = true;
    return RC::SUCCESS;
  }

  RC close() override {
    children_[0]->close();
    LOG_INFO("close vectorized group by operator");
    return RC::SUCCESS;
  }

private:
  std::vector<Expression *> aggregate_expressions_;  /// 聚合表达式
  std::vector<Expression *> value_expressions_;      /// 计算聚合时的表达式
  std::vector<std::unique_ptr<Expression>> group_by_exprs_;
  bool emited_ = false;
  Chunk chunk_;
  StandardAggregateHashTable ht_;
};