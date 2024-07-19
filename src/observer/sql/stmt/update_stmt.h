/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "common/rc.h"
#include "sql/stmt/stmt.h"

class Table;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  UpdateStmt(Table *table, Value *values, int value_amount);
  ~UpdateStmt() override;

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

  StmtType type() const override { return StmtType::UPDATE; }

public:
  Table *table() const { return table_; }
  Value *value() const { return value_; }
  int    value_amount() const { return value_amount_; }

  void set_table(Table* t) {table_ = t;}
  void set_value(const Value& val) {
    value_ = new Value(val);
  }
  void set_filter(FilterStmt* f) {filter_stmt_ = f;}

private:
  Table *table_        = nullptr;
  Value *value_       = nullptr;
  int    value_amount_ = 0;
  FilterStmt  *filter_stmt_ = nullptr;
};
