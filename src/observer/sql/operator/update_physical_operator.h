#pragma once

#include "sql/operator/physical_operator.h"

class Trx;
class UpdateStmt;

/**
 * @brief 物理算子，更新
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, Value* val, std::string str) : table_(table), value_(val), attribute_name_(str) {}

  virtual ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);

private:

  RC filter(RowTuple &tuple, bool &result);

  Table              *table_ = nullptr;
  Trx                *trx_   = nullptr;

  // value want to update
  Value* value_ = nullptr;
  // attribute want to update
  std::string attribute_name_;

  std::vector<Record> records_;
  std::vector<Record> new_records_;
  std::vector<std::unique_ptr<Expression>> predicates_;
};