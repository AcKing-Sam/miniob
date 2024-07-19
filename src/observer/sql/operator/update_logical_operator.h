#pragma once

#include "sql/operator/logical_operator.h"

/**
 * @brief 逻辑算子，用于执行 update 语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, Value* val, std::string str);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }
  Value* value() const { return value_; }
  std::string attribute() const { return attribute_name_; }
  int value_num() const { return value_nums_;}

  // void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);
  // auto predicates() -> std::vector<std::unique_ptr<Expression>> & { return predicates_; }

private:
  Table *table_ = nullptr;
  Value *value_ = nullptr;
  int value_nums_ = 1;
  std::string attribute_name_;
  // std::vector<std::unique_ptr<Expression>> predicates_;
};